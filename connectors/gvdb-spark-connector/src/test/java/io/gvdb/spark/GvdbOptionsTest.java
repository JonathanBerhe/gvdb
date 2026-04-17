package io.gvdb.spark;

import io.gvdb.client.model.IndexType;
import io.gvdb.client.model.MetricType;
import io.gvdb.spark.write.WriteMode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("GvdbOptions")
class GvdbOptionsTest {

    @Test
    @DisplayName("applies defaults when only required options are set")
    void defaults() {
        var opts = new GvdbOptions(Map.of(
                "gvdb.target", "localhost:50051",
                "gvdb.collection", "test"
        ));

        assertEquals("localhost:50051", opts.target());
        assertEquals("test", opts.collection());
        assertTrue(opts.apiKey().isEmpty());
        assertEquals("id", opts.idColumn());
        assertEquals("vector", opts.vectorColumn());
        assertEquals(10000, opts.batchSize());
        assertEquals(WriteMode.UPSERT, opts.writeMode());
        assertEquals(3, opts.maxRetries());
        assertEquals(30, opts.timeoutSeconds());
        assertEquals(MetricType.COSINE, opts.metric());
        assertEquals(IndexType.AUTO, opts.indexType());
        assertTrue(opts.dimension().isEmpty());
        assertTrue(opts.autoCreate());
    }

    @Test
    @DisplayName("parses custom values including enum-typed fields")
    void customValues() {
        var opts = new GvdbOptions(Map.of(
                "gvdb.target", "myhost:9090",
                "gvdb.collection", "embeddings",
                "gvdb.api_key", "sk-123",
                "gvdb.id_column", "my_id",
                "gvdb.vector_column", "embedding",
                "gvdb.batch_size", "5000",
                "gvdb.write_mode", "stream_insert",
                "gvdb.metric", "l2",
                "gvdb.index_type", "hnsw",
                "gvdb.dimension", "768"
        ));

        assertEquals("myhost:9090", opts.target());
        assertEquals("sk-123", opts.apiKey().orElseThrow());
        assertEquals("my_id", opts.idColumn());
        assertEquals("embedding", opts.vectorColumn());
        assertEquals(5000, opts.batchSize());
        assertEquals(WriteMode.STREAM_INSERT, opts.writeMode());
        assertEquals(MetricType.L2, opts.metric());
        assertEquals(IndexType.HNSW, opts.indexType());
        assertEquals(768, opts.dimension().orElseThrow());
    }

    @Nested
    @DisplayName("required option validation")
    class Required {
        @Test
        @DisplayName("target() throws when missing")
        void missingTargetThrows() {
            var opts = new GvdbOptions(Map.of("gvdb.collection", "test"));
            assertThrows(IllegalArgumentException.class, opts::target);
        }

        @Test
        @DisplayName("collection() throws when missing")
        void missingCollectionThrows() {
            var opts = new GvdbOptions(Map.of("gvdb.target", "localhost:50051"));
            assertThrows(IllegalArgumentException.class, opts::collection);
        }
    }

    @Test
    @DisplayName("writeMode() rejects unknown values with a helpful message")
    void writeModeRejectsUnknown() {
        var opts = new GvdbOptions(Map.of(
                "gvdb.target", "localhost:50051",
                "gvdb.collection", "test",
                "gvdb.write_mode", "nope"
        ));
        var ex = assertThrows(IllegalArgumentException.class, opts::writeMode);
        assertTrue(ex.getMessage().contains("upsert"));
        assertTrue(ex.getMessage().contains("stream_insert"));
    }

    @Test
    @DisplayName("apiKey() is empty when value is blank")
    void apiKeyBlankIsEmpty() {
        var opts = new GvdbOptions(Map.of(
                "gvdb.target", "localhost:50051",
                "gvdb.collection", "test",
                "gvdb.api_key", "   "
        ));
        assertTrue(opts.apiKey().isEmpty());
    }

    @Test
    @DisplayName("toClientConfig() propagates target, apiKey, batchSize, and maxRetries")
    void toClientConfig() {
        var opts = new GvdbOptions(Map.of(
                "gvdb.target", "host:50051",
                "gvdb.collection", "test",
                "gvdb.api_key", "key",
                "gvdb.batch_size", "2000",
                "gvdb.max_retries", "5",
                "gvdb.timeout_seconds", "60"
        ));

        var config = opts.toClientConfig();
        assertEquals("host:50051", config.target());
        assertEquals("key", config.apiKey());
        assertEquals(2000, config.batchSize());
        assertEquals(5, config.maxRetries());
    }
}
