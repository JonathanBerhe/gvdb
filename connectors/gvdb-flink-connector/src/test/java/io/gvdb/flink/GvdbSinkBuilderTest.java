package io.gvdb.flink;

import io.gvdb.client.model.GvdbVector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("GvdbSinkBuilder")
class GvdbSinkBuilderTest {

    @Test
    @DisplayName("build() succeeds with minimum required fields")
    void buildSucceeds() {
        var sink = GvdbSink.<String>builder()
                .setTarget("localhost:50051")
                .setCollection("test")
                .setRecordMapper(s -> new GvdbVector(s.hashCode(), new float[]{1.0f}))
                .build();

        assertNotNull(sink);
    }

    @Nested
    @DisplayName("validation")
    class Validation {

        @Test
        @DisplayName("missing target throws NPE")
        void missingTargetThrows() {
            assertThrows(NullPointerException.class, () ->
                    GvdbSink.<String>builder()
                            .setCollection("test")
                            .setRecordMapper(s -> new GvdbVector(1, new float[]{1.0f}))
                            .build());
        }

        @Test
        @DisplayName("missing collection throws NPE")
        void missingCollectionThrows() {
            assertThrows(NullPointerException.class, () ->
                    GvdbSink.<String>builder()
                            .setTarget("localhost:50051")
                            .setRecordMapper(s -> new GvdbVector(1, new float[]{1.0f}))
                            .build());
        }

        @Test
        @DisplayName("missing record mapper throws NPE")
        void missingRecordMapperThrows() {
            assertThrows(NullPointerException.class, () ->
                    GvdbSink.<String>builder()
                            .setTarget("localhost:50051")
                            .setCollection("test")
                            .build());
        }

        @Test
        @DisplayName("batchSize < 1 throws IllegalArgument")
        void invalidBatchSizeThrows() {
            assertThrows(IllegalArgumentException.class, () ->
                    GvdbSink.<String>builder()
                            .setTarget("localhost:50051")
                            .setCollection("test")
                            .setBatchSize(0)
                            .setRecordMapper(s -> new GvdbVector(1, new float[]{1.0f}))
                            .build());
        }
    }

    @Test
    @DisplayName("retains all custom configuration values")
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
    @DisplayName("sink is Java-serializable for Flink task distribution")
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
