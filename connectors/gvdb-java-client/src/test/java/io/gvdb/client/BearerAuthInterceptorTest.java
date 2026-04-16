package io.gvdb.client;

import io.grpc.*;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.gvdb.proto.HealthCheckRequest;
import io.gvdb.proto.HealthCheckResponse;
import io.gvdb.proto.VectorDBServiceGrpc;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class BearerAuthInterceptorTest {

    @Test
    void authHeaderAttached() throws Exception {
        var capturedAuth = new AtomicReference<String>();

        // Interceptor on the server side to capture the auth header
        var serverInterceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, io.grpc.Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                var key = io.grpc.Metadata.Key.of("authorization", io.grpc.Metadata.ASCII_STRING_MARSHALLER);
                capturedAuth.set(headers.get(key));
                return next.startCall(call, headers);
            }
        };

        String serverName = InProcessServerBuilder.generateName();
        var server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(ServerInterceptors.intercept(new FakeHealthService(), serverInterceptor))
                .build()
                .start();

        try {
            var channel = InProcessChannelBuilder.forName(serverName)
                    .directExecutor()
                    .intercept(new BearerAuthInterceptor("my-secret-key"))
                    .build();

            try {
                var stub = VectorDBServiceGrpc.newBlockingStub(channel);
                stub.healthCheck(HealthCheckRequest.getDefaultInstance());

                assertEquals("Bearer my-secret-key", capturedAuth.get());
            } finally {
                channel.shutdownNow();
            }
        } finally {
            server.shutdownNow();
        }
    }

    private static class FakeHealthService extends VectorDBServiceGrpc.VectorDBServiceImplBase {
        @Override
        public void healthCheck(HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
            responseObserver.onNext(HealthCheckResponse.newBuilder()
                    .setStatus(HealthCheckResponse.Status.SERVING)
                    .build());
            responseObserver.onCompleted();
        }
    }
}
