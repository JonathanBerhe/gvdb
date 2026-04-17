package io.gvdb.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("GvdbChannelPool")
class GvdbChannelPoolTest {

    private static GvdbClientConfig config(int channelCount) {
        return GvdbClientConfig.builder("localhost:1")
                .channelCount(channelCount)
                .build();
    }

    @Test
    @DisplayName("next() cycles through all channels in round-robin order")
    void roundRobin() {
        try (var pool = new GvdbChannelPool(config(3))) {
            var first = pool.next();
            var second = pool.next();
            var third = pool.next();
            var fourth = pool.next();

            assertNotSame(first, second);
            assertNotSame(second, third);
            assertNotSame(first, third);
            assertSame(first, fourth);
        }
    }

    @Test
    @DisplayName("next() returns the same channel repeatedly when the pool has one channel")
    void singleChannelPool() {
        try (var pool = new GvdbChannelPool(config(1))) {
            var a = pool.next();
            var b = pool.next();
            assertSame(a, b);
        }
    }

    @Test
    @DisplayName("next() is thread-safe under concurrent access")
    void concurrentAccess() throws Exception {
        int threads = 8;
        int callsPerThread = 200;
        try (var pool = new GvdbChannelPool(config(4))) {
            var executor = Executors.newFixedThreadPool(threads);
            Set<Object> distinct = java.util.Collections.synchronizedSet(new HashSet<>());
            try {
                for (int t = 0; t < threads; t++) {
                    executor.submit(() -> {
                        for (int i = 0; i < callsPerThread; i++) {
                            distinct.add(pool.next());
                        }
                    });
                }
            } finally {
                executor.shutdown();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            }
            assertEquals(4, distinct.size());
        }
    }

    @Test
    @DisplayName("close() is idempotent across repeated calls")
    void closeIsIdempotent() {
        var pool = new GvdbChannelPool(config(2));
        pool.close();
        assertDoesNotThrow(pool::close);
    }
}
