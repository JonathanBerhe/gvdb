package io.gvdb.client;

import io.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * gRPC client interceptor that retries failed calls with exponential backoff.
 * <p>
 * Retryable: UNAVAILABLE, DEADLINE_EXCEEDED, ABORTED.
 * Non-retryable: RESOURCE_EXHAUSTED, INVALID_ARGUMENT, NOT_FOUND, PERMISSION_DENIED, UNAUTHENTICATED.
 * <p>
 * Backoff: 500ms base, 2x multiplier, +/-20% jitter.
 * Matches Python SDK retry behavior in importers.py.
 */
final class RetryInterceptor implements ClientInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(RetryInterceptor.class);

    private static final Set<Status.Code> RETRYABLE_CODES = Set.of(
            Status.Code.UNAVAILABLE,
            Status.Code.DEADLINE_EXCEEDED,
            Status.Code.ABORTED
    );

    private static final long BASE_BACKOFF_MS = 500;
    private static final double BACKOFF_MULTIPLIER = 2.0;
    private static final double JITTER_FACTOR = 0.2;

    private static final ScheduledExecutorService RETRY_SCHEDULER =
            Executors.newSingleThreadScheduledExecutor(r -> {
                var t = new Thread(r, "gvdb-retry-scheduler");
                t.setDaemon(true);
                return t;
            });

    private final int maxRetries;

    RetryInterceptor(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions,
            Channel next) {

        // For streaming calls, don't retry — just pass through
        if (method.getType() != MethodDescriptor.MethodType.UNARY) {
            return next.newCall(method, callOptions);
        }

        return new RetryingCall<>(method, callOptions, next);
    }

    private final class RetryingCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {

        private final MethodDescriptor<ReqT, RespT> method;
        private final CallOptions callOptions;
        private final Channel channel;

        private Listener<RespT> responseListener;
        private Metadata requestHeaders;
        private ReqT requestMessage;
        private final AtomicInteger attempt = new AtomicInteger(0);
        private volatile ClientCall<ReqT, RespT> delegate;

        RetryingCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel channel) {
            this.method = method;
            this.callOptions = callOptions;
            this.channel = channel;
        }

        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
            this.responseListener = responseListener;
            this.requestHeaders = headers;
            startAttempt();
        }

        @Override
        public void request(int numMessages) {
            if (delegate != null) delegate.request(numMessages);
        }

        @Override
        public void cancel(String message, Throwable cause) {
            if (delegate != null) delegate.cancel(message, cause);
        }

        @Override
        public void halfClose() {
            if (delegate != null) delegate.halfClose();
        }

        @Override
        public void sendMessage(ReqT message) {
            this.requestMessage = message;
            if (delegate != null) delegate.sendMessage(message);
        }

        private void startAttempt() {
            delegate = channel.newCall(method, callOptions);
            delegate.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(responseListener) {
                @Override
                public void onClose(Status status, Metadata trailers) {
                    if (status.isOk() || attempt.get() >= maxRetries || !RETRYABLE_CODES.contains(status.getCode())) {
                        super.onClose(status, trailers);
                    } else {
                        int current = attempt.incrementAndGet();
                        long backoff = computeBackoff(current);
                        LOG.warn("gRPC call {} failed with {}, retrying ({}/{}) in {}ms",
                                method.getFullMethodName(), status.getCode(), current, maxRetries, backoff);
                        RETRY_SCHEDULER.schedule(() -> {
                            try {
                                startAttempt();
                                if (requestMessage != null) {
                                    delegate.sendMessage(requestMessage);
                                }
                                delegate.halfClose();
                                delegate.request(1);
                            } catch (Exception e) {
                                responseListener.onClose(
                                        Status.INTERNAL.withDescription("Retry failed").withCause(e),
                                        new Metadata());
                            }
                        }, backoff, TimeUnit.MILLISECONDS);
                    }
                }
            }, requestHeaders);
        }

        private long computeBackoff(int attempt) {
            long baseMs = (long) (BASE_BACKOFF_MS * Math.pow(BACKOFF_MULTIPLIER, attempt - 1));
            double jitter = 1.0 + (ThreadLocalRandom.current().nextDouble() * 2 - 1) * JITTER_FACTOR;
            return (long) (baseMs * jitter);
        }
    }
}
