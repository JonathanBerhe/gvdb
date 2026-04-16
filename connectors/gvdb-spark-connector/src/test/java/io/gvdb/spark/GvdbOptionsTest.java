package io.gvdb.spark;

import io.gvdb.client.model.IndexType;
import io.gvdb.client.model.MetricType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class GvdbOptionsTest {

    @Test
    void defaults() {
        var opts = new GvdbOptions(Map.of(
                "gvdb.target", "localhost:50051",
                "gvdb.collection", "test"
        ));

        assertEquals("localhost:50051", opts.target());
        assertEquals("test", opts.collection());
        assertNull(opts.apiKey());
        assertEquals("id", opts.idColumn());
        assertEquals("vector", opts.vectorColumn());
        assertEquals(10000, opts.batchSize());
        assertEquals("upsert", opts.writeMode());
        assertEquals(3, opts.maxRetries());
        assertEquals(30, opts.timeoutSeconds());
        assertEquals(MetricType.COSINE, opts.metric());
        assertEquals(IndexType.AUTO, opts.indexType());
        assertEquals(-1, opts.dimension());
        assertTrue(opts.autoCreate());
    }

    @Test
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
        assertEquals("sk-123", opts.apiKey());
        assertEquals("my_id", opts.idColumn());
        assertEquals("embedding", opts.vectorColumn());
        assertEquals(5000, opts.batchSize());
        assertEquals("stream_insert", opts.writeMode());
        assertEquals(MetricType.L2, opts.metric());
        assertEquals(IndexType.HNSW, opts.indexType());
        assertEquals(768, opts.dimension());
    }

    @Test
    void missingTargetThrows() {
        var opts = new GvdbOptions(Map.of("gvdb.collection", "test"));
        assertThrows(IllegalArgumentException.class, opts::target);
    }

    @Test
    void missingCollectionThrows() {
        var opts = new GvdbOptions(Map.of("gvdb.target", "localhost:50051"));
        assertThrows(IllegalArgumentException.class, opts::collection);
    }

    @Test
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
