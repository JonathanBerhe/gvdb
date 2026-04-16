package io.gvdb.client;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class GvdbClientConfigTest {

    @Test
    void defaultValues() {
        var config = GvdbClientConfig.builder("localhost:50051").build();
        assertEquals("localhost:50051", config.target());
        assertNull(config.apiKey());
        assertEquals(4, config.channelCount());
        assertEquals(10_000, config.batchSize());
        assertEquals(256 * 1024 * 1024, config.maxMessageSizeBytes());
        assertEquals(3, config.maxRetries());
        assertEquals(Duration.ofSeconds(30), config.timeout());
    }

    @Test
    void customValues() {
        var config = GvdbClientConfig.builder("myhost:9090")
                .apiKey("my-key")
                .channelCount(8)
                .batchSize(5000)
                .maxRetries(5)
                .timeout(Duration.ofSeconds(60))
                .build();

        assertEquals("myhost:9090", config.target());
        assertEquals("my-key", config.apiKey());
        assertEquals(8, config.channelCount());
        assertEquals(5000, config.batchSize());
        assertEquals(5, config.maxRetries());
        assertEquals(Duration.ofSeconds(60), config.timeout());
    }

    @Test
    void nullTargetFails() {
        assertThrows(NullPointerException.class, () -> GvdbClientConfig.builder(null));
    }

    @Test
    void blankTargetFails() {
        assertThrows(IllegalArgumentException.class,
                () -> GvdbClientConfig.builder("  ").build());
    }

    @Test
    void invalidChannelCountFails() {
        assertThrows(IllegalArgumentException.class,
                () -> GvdbClientConfig.builder("localhost:50051").channelCount(0).build());
    }

    @Test
    void invalidBatchSizeFails() {
        assertThrows(IllegalArgumentException.class,
                () -> GvdbClientConfig.builder("localhost:50051").batchSize(-1).build());
    }

    @Test
    void zeroRetriesAllowed() {
        var config = GvdbClientConfig.builder("localhost:50051").maxRetries(0).build();
        assertEquals(0, config.maxRetries());
    }
}
