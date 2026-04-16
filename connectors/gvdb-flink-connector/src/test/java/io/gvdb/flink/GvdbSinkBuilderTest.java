package io.gvdb.flink;

import io.gvdb.client.model.GvdbVector;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class GvdbSinkBuilderTest {

    @Test
    void buildSucceeds() {
        var sink = GvdbSink.<String>builder()
                .setTarget("localhost:50051")
                .setCollection("test")
                .setRecordMapper(s -> new GvdbVector(s.hashCode(), new float[]{1.0f}))
                .build();

        assertNotNull(sink);
    }

    @Test
    void missingTargetThrows() {
        assertThrows(NullPointerException.class, () ->
                GvdbSink.<String>builder()
                        .setCollection("test")
                        .setRecordMapper(s -> new GvdbVector(1, new float[]{1.0f}))
                        .build());
    }

    @Test
    void missingCollectionThrows() {
        assertThrows(NullPointerException.class, () ->
                GvdbSink.<String>builder()
                        .setTarget("localhost:50051")
                        .setRecordMapper(s -> new GvdbVector(1, new float[]{1.0f}))
                        .build());
    }

    @Test
    void missingRecordMapperThrows() {
        assertThrows(NullPointerException.class, () ->
                GvdbSink.<String>builder()
                        .setTarget("localhost:50051")
                        .setCollection("test")
                        .build());
    }

    @Test
    void invalidBatchSizeThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                GvdbSink.<String>builder()
                        .setTarget("localhost:50051")
                        .setCollection("test")
                        .setBatchSize(0)
                        .setRecordMapper(s -> new GvdbVector(1, new float[]{1.0f}))
                        .build());
    }

    @Test
    void customConfig() {
        var builder = GvdbSink.<String>builder()
                .setTarget("myhost:9090")
                .setCollection("embeddings")
                .setApiKey("key-123")
                .setBatchSize(5000)
                .setMaxRetries(5)
                .setTimeoutSeconds(60)
                .setRecordMapper(s -> new GvdbVector(1, new float[]{1.0f}));

        assertEquals("myhost:9090", builder.target());
        assertEquals("embeddings", builder.collection());
        assertEquals("key-123", builder.apiKey());
        assertEquals(5000, builder.batchSize());
        assertEquals(5, builder.maxRetries());
        assertEquals(60, builder.timeoutSeconds());
    }

    @Test
    void sinkIsSerializable() throws Exception {
        var sink = GvdbSink.<String>builder()
                .setTarget("localhost:50051")
                .setCollection("test")
                .setRecordMapper(s -> new GvdbVector(s.hashCode(), new float[]{1.0f}))
                .build();

        var baos = new ByteArrayOutputStream();
        try (var oos = new ObjectOutputStream(baos)) {
            oos.writeObject(sink);
        }

        var bais = new ByteArrayInputStream(baos.toByteArray());
        try (var ois = new ObjectInputStream(bais)) {
            var restored = ois.readObject();
            assertInstanceOf(GvdbSink.class, restored);
        }
    }
}
