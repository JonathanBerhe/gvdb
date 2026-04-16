package io.gvdb.client;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for {@link GvdbClient}. Use {@link #builder(String)} to construct.
 */
public final class GvdbClientConfig {

    private final String target;
    private final String apiKey;
    private final int channelCount;
    private final int batchSize;
    private final int maxMessageSizeBytes;
    private final int maxRetries;
    private final Duration timeout;
    private final Duration keepAliveTime;
    private final Duration idleTimeout;

    private GvdbClientConfig(Builder b) {
        this.target = b.target;
        this.apiKey = b.apiKey;
        this.channelCount = b.channelCount;
        this.batchSize = b.batchSize;
        this.maxMessageSizeBytes = b.maxMessageSizeBytes;
        this.maxRetries = b.maxRetries;
        this.timeout = b.timeout;
        this.keepAliveTime = b.keepAliveTime;
        this.idleTimeout = b.idleTimeout;
    }

    public static Builder builder(String target) {
        return new Builder(target);
    }

    public String target() { return target; }
    public String apiKey() { return apiKey; }
    public int channelCount() { return channelCount; }
    public int batchSize() { return batchSize; }
    public int maxMessageSizeBytes() { return maxMessageSizeBytes; }
    public int maxRetries() { return maxRetries; }
    public Duration timeout() { return timeout; }
    public Duration keepAliveTime() { return keepAliveTime; }
    public Duration idleTimeout() { return idleTimeout; }

    public static final class Builder {
        private final String target;
        private String apiKey;
        private int channelCount = 4;
        private int batchSize = 10_000;
        private int maxMessageSizeBytes = 256 * 1024 * 1024; // 256 MB
        private int maxRetries = 3;
        private Duration timeout = Duration.ofSeconds(30);
        private Duration keepAliveTime = Duration.ofSeconds(60);
        private Duration idleTimeout = Duration.ofMinutes(30);

        private Builder(String target) {
            this.target = Objects.requireNonNull(target, "target must not be null");
        }

        public Builder apiKey(String apiKey) { this.apiKey = apiKey; return this; }
        public Builder channelCount(int channelCount) { this.channelCount = channelCount; return this; }
        public Builder batchSize(int batchSize) { this.batchSize = batchSize; return this; }
        public Builder maxMessageSizeBytes(int size) { this.maxMessageSizeBytes = size; return this; }
        public Builder maxRetries(int maxRetries) { this.maxRetries = maxRetries; return this; }
        public Builder timeout(Duration timeout) { this.timeout = timeout; return this; }
        public Builder keepAliveTime(Duration keepAliveTime) { this.keepAliveTime = keepAliveTime; return this; }
        public Builder idleTimeout(Duration idleTimeout) { this.idleTimeout = idleTimeout; return this; }

        public GvdbClientConfig build() {
            if (target.isBlank()) throw new IllegalArgumentException("target must not be blank");
            if (channelCount < 1) throw new IllegalArgumentException("channelCount must be >= 1");
            if (batchSize < 1) throw new IllegalArgumentException("batchSize must be >= 1");
            if (maxMessageSizeBytes < 1) throw new IllegalArgumentException("maxMessageSizeBytes must be >= 1");
            if (maxRetries < 0) throw new IllegalArgumentException("maxRetries must be >= 0");
            return new GvdbClientConfig(this);
        }
    }
}
