package io.gvdb.client;

import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Round-robin pool of gRPC channels for write parallelism.
 */
final class GvdbChannelPool implements Closeable {

    private final List<ManagedChannel> channels;
    private final AtomicInteger index = new AtomicInteger(0);

    GvdbChannelPool(GvdbClientConfig config) {
        var interceptors = new ArrayList<ClientInterceptor>();
        if (config.apiKey() != null && !config.apiKey().isBlank()) {
            interceptors.add(new BearerAuthInterceptor(config.apiKey()));
        }
        interceptors.add(new RetryInterceptor(config.maxRetries()));

        this.channels = new ArrayList<>(config.channelCount());
        for (int i = 0; i < config.channelCount(); i++) {
            var channel = ManagedChannelBuilder.forTarget(config.target())
                    .usePlaintext()
                    .maxInboundMessageSize(config.maxMessageSizeBytes())
                    .keepAliveTime(config.keepAliveTime().toSeconds(), TimeUnit.SECONDS)
                    .keepAliveTimeout(10, TimeUnit.SECONDS)
                    .idleTimeout(config.idleTimeout().toMinutes(), TimeUnit.MINUTES)
                    .intercept(interceptors)
                    .build();
            channels.add(channel);
        }
    }

    ManagedChannel next() {
        int idx = Math.floorMod(index.getAndIncrement(), channels.size());
        return channels.get(idx);
    }

    @Override
    public void close() {
        for (var channel : channels) {
            channel.shutdown();
        }
        boolean interrupted = false;
        for (var channel : channels) {
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                interrupted = true;
                channel.shutdownNow();
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
