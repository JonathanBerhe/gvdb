package io.gvdb.client;

import io.grpc.*;

/**
 * gRPC client interceptor that injects a Bearer token into the authorization header.
 * Matches the Python SDK pattern: {@code ("authorization", f"Bearer {api_key}")}.
 */
final class BearerAuthInterceptor implements ClientInterceptor {

    private static final Metadata.Key<String> AUTH_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private final String bearerToken;

    BearerAuthInterceptor(String apiKey) {
        this.bearerToken = "Bearer " + apiKey;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions,
            Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                headers.put(AUTH_KEY, bearerToken);
                super.start(responseListener, headers);
            }
        };
    }
}
