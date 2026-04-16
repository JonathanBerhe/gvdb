package io.gvdb.client;

import io.grpc.*;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.gvdb.proto.HealthCheckRequest;
import io.gvdb.proto.HealthCheckResponse;
import io.gvdb.proto.VectorDBServiceGrpc;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RetryInterceptorTest {

    @Test
    void retriesOnUnavailableThenSucceeds() throws Exception {
        var attempts = new AtomicInteger(0);
        var service = new VectorDBServiceGrpc.VectorDBServiceImplBase() {
            @Override
            public void healthCheck(HealthCheckRequest request, StreamObserver<HealthCheckResponse> observer) {
                if (attempts.incrementAndGet() <= 2) {
                    observer.onError(Status.UNAVAILABLE.withDescription("transient").asRuntimeException());
                } else {
                    observer.onNext(HealthCheckResponse.newBuilder()
                            .setStatus(HealthCheckResponse.Status.SERVING).build());
                    observer.onCompleted();
                }
            }
        };

        String serverName = InProcessServerBuilder.generateName();
        var server = InProcessServerBuilder.forName(serverName).directExecutor()
                .addService(service).build().start();
        try {
            var channel = InProcessChannelBuilder.forName(serverName).directExecutor()
                    .intercept(new RetryInterceptor(3)).build();
            try {
                var stub = VectorDBServiceGrpc.newBlockingStub(channel);
                var resp = stub.healthCheck(HealthCheckRequest.getDefaultInstance());

                assertEquals(HealthCheckResponse.Status.SERVING, resp.getStatus());
                assertEquals(3, attempts.get()); // 2 failures + 1 success
            } finally {
                channel.shutdownNow();
            }
        } finally {
            server.shutdownNow();
        }
    }

    @Test
    void doesNotRetryResourceExhausted() throws Exception {
        var attempts = new AtomicInteger(0);
        var service = new VectorDBServiceGrpc.VectorDBServiceImplBase() {
            @Override
            public void healthCheck(HealthCheckRequest request, StreamObserver<HealthCheckResponse> observer) {
                attempts.incrementAndGet();
                observer.onError(Status.RESOURCE_EXHAUSTED.withDescription("batch too large").asRuntimeException());
            }
        };

        String serverName = InProcessServerBuilder.generateName();
        var server = InProcessServerBuilder.forName(serverName).directExecutor()
                .addService(service).build().start();
        try {
            var channel = InProcessChannelBuilder.forName(serverName).directExecutor()
                    .intercept(new RetryInterceptor(3)).build();
            try {
                var stub = VectorDBServiceGrpc.newBlockingStub(channel);
                var ex = assertThrows(StatusRuntimeException.class,
                        () -> stub.healthCheck(HealthCheckRequest.getDefaultInstance()));

                assertEquals(Status.Code.RESOURCE_EXHAUSTED, ex.getStatus().getCode());
                assertEquals(1, attempts.get()); // No retries
            } finally {
                channel.shutdownNow();
            }
        } finally {
            server.shutdownNow();
        }
    }

    @Test
    void respectsMaxRetries() throws Exception {
        var attempts = new AtomicInteger(0);
        var service = new VectorDBServiceGrpc.VectorDBServiceImplBase() {
            @Override
            public void healthCheck(HealthCheckRequest request, StreamObserver<HealthCheckResponse> observer) {
                attempts.incrementAndGet();
                observer.onError(Status.UNAVAILABLE.withDescription("always failing").asRuntimeException());
            }
        };

        String serverName = InProcessServerBuilder.generateName();
        var server = InProcessServerBuilder.forName(serverName).directExecutor()
                .addService(service).build().start();
        try {
            var channel = InProcessChannelBuilder.forName(serverName).directExecutor()
                    .intercept(new RetryInterceptor(2)).build();
            try {
                var stub = VectorDBServiceGrpc.newBlockingStub(channel);
                assertThrows(StatusRuntimeException.class,
                        () -> stub.healthCheck(HealthCheckRequest.getDefaultInstance()));

                assertEquals(3, attempts.get()); // 1 initial + 2 retries
            } finally {
                channel.shutdownNow();
            }
        } finally {
            server.shutdownNow();
        }
    }

    @Test
    void successOnFirstTryNoRetry() throws Exception {
        var attempts = new AtomicInteger(0);
        var service = new VectorDBServiceGrpc.VectorDBServiceImplBase() {
            @Override
            public void healthCheck(HealthCheckRequest request, StreamObserver<HealthCheckResponse> observer) {
                attempts.incrementAndGet();
                observer.onNext(HealthCheckResponse.newBuilder()
                        .setStatus(HealthCheckResponse.Status.SERVING).build());
                observer.onCompleted();
            }
        };

        String serverName = InProcessServerBuilder.generateName();
        var server = InProcessServerBuilder.forName(serverName).directExecutor()
                .addService(service).build().start();
        try {
            var channel = InProcessChannelBuilder.forName(serverName).directExecutor()
                    .intercept(new RetryInterceptor(3)).build();
            try {
                var stub = VectorDBServiceGrpc.newBlockingStub(channel);
                stub.healthCheck(HealthCheckRequest.getDefaultInstance());
                assertEquals(1, attempts.get());
            } finally {
                channel.shutdownNow();
            }
        } finally {
            server.shutdownNow();
        }
    }
}
