package io.gvdb.client;

import io.gvdb.client.model.GvdbVector;
import io.gvdb.client.model.ImportStatus;
import io.gvdb.client.model.MetricType;
import io.gvdb.proto.*;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ProtoConverterTest {

    @Test
    void vectorRoundTrip() {
        var original = new GvdbVector(42L, new float[]{1.0f, 2.0f, 3.0f},
                Map.of("label", "test", "score", 0.95, "count", 100L, "active", true));

        VectorWithId proto = ProtoConverter.toProto(original);
        GvdbVector restored = ProtoConverter.fromProto(proto);

        assertEquals(original.id(), restored.id());
        assertArrayEquals(original.values(), restored.values());
        assertEquals("test", restored.metadata().get("label"));
        assertEquals(0.95, (Double) restored.metadata().get("score"), 1e-9);
        assertEquals(100L, restored.metadata().get("count"));
        assertEquals(true, restored.metadata().get("active"));
    }

    @Test
    void vectorWithNoMetadata() {
        var original = new GvdbVector(1L, new float[]{0.5f});
        VectorWithId proto = ProtoConverter.toProto(original);
        GvdbVector restored = ProtoConverter.fromProto(proto);

        assertEquals(1L, restored.id());
        assertArrayEquals(new float[]{0.5f}, restored.values());
        assertTrue(restored.metadata().isEmpty());
    }

    @Test
    void vectorListRoundTrip() {
        var v1 = new GvdbVector(1L, new float[]{1.0f, 2.0f});
        var v2 = new GvdbVector(2L, new float[]{3.0f, 4.0f}, Map.of("key", "value"));

        List<VectorWithId> protos = ProtoConverter.toProtoList(List.of(v1, v2));
        assertEquals(2, protos.size());

        List<GvdbVector> restored = ProtoConverter.fromProtoList(protos);
        assertEquals(2, restored.size());
        assertEquals(1L, restored.get(0).id());
        assertEquals(2L, restored.get(1).id());
        assertEquals("value", restored.get(1).metadata().get("key"));
    }

    @Test
    void emptyMetadataRoundTrip() {
        Map<String, Object> metadata = Map.of();
        Metadata proto = ProtoConverter.toProtoMetadata(metadata);
        Map<String, Object> restored = ProtoConverter.fromProtoMetadata(proto);
        assertTrue(restored.isEmpty());
    }

    @Test
    void metadataIntegerPromotedToLong() {
        // Java Integer should be stored as int_value (Long) in proto
        var vector = new GvdbVector(1L, new float[]{1.0f}, Map.of("count", 42));
        VectorWithId proto = ProtoConverter.toProto(vector);
        GvdbVector restored = ProtoConverter.fromProto(proto);
        assertEquals(42L, restored.metadata().get("count"));
    }

    @Test
    void metadataFloatPromotedToDouble() {
        var vector = new GvdbVector(1L, new float[]{1.0f}, Map.of("score", 0.5f));
        VectorWithId proto = ProtoConverter.toProto(vector);
        GvdbVector restored = ProtoConverter.fromProto(proto);
        assertEquals(0.5, (Double) restored.metadata().get("score"), 1e-6);
    }

    @Test
    void collectionInfoFromProto() {
        var proto = io.gvdb.proto.CollectionInfo.newBuilder()
                .setCollectionName("test_col")
                .setCollectionId(1)
                .setDimension(128)
                .setMetricType("COSINE")
                .setVectorCount(50000)
                .build();

        var info = ProtoConverter.fromProto(proto);
        assertEquals("test_col", info.name());
        assertEquals(1, info.id());
        assertEquals(128, info.dimension());
        assertEquals(MetricType.COSINE, info.metricType());
        assertEquals(50000, info.vectorCount());
    }

    @Test
    void collectionInfoFromProtoLowercaseMetric() {
        var proto = io.gvdb.proto.CollectionInfo.newBuilder()
                .setCollectionName("c")
                .setMetricType("l2")
                .build();

        assertEquals(MetricType.L2, ProtoConverter.fromProto(proto).metricType());
    }

    @Test
    void collectionInfoFromProtoUnknownMetricThrows() {
        var proto = io.gvdb.proto.CollectionInfo.newBuilder()
                .setCollectionName("c")
                .setMetricType("HAMMING")
                .build();

        var ex = assertThrows(IllegalArgumentException.class, () -> ProtoConverter.fromProto(proto));
        assertTrue(ex.getMessage().contains("HAMMING"));
    }

    @Test
    void importStatusFromProto() {
        var proto = GetImportStatusResponse.newBuilder()
                .setImportId("import-123")
                .setState(ImportState.IMPORT_RUNNING)
                .setTotalVectors(100000)
                .setImportedVectors(50000)
                .setProgressPercent(50.0f)
                .setElapsedSeconds(12.5f)
                .setSegmentsCreated(3)
                .build();

        ImportStatus status = ProtoConverter.fromProto(proto);
        assertEquals("import-123", status.importId());
        assertEquals(ImportStatus.ImportState.RUNNING, status.state());
        assertEquals(100000, status.totalVectors());
        assertEquals(50000, status.importedVectors());
        assertEquals(50.0f, status.progressPercent(), 0.01f);
        assertEquals(3, status.segmentsCreated());
    }

    @Test
    void metricTypeMapping() {
        assertEquals(CreateCollectionRequest.MetricType.L2,
                ProtoConverter.toProtoMetric(io.gvdb.client.model.MetricType.L2));
        assertEquals(CreateCollectionRequest.MetricType.INNER_PRODUCT,
                ProtoConverter.toProtoMetric(io.gvdb.client.model.MetricType.INNER_PRODUCT));
        assertEquals(CreateCollectionRequest.MetricType.COSINE,
                ProtoConverter.toProtoMetric(io.gvdb.client.model.MetricType.COSINE));
    }

    @Test
    void indexTypeMapping() {
        assertEquals(CreateCollectionRequest.IndexType.AUTO,
                ProtoConverter.toProtoIndex(io.gvdb.client.model.IndexType.AUTO));
        assertEquals(CreateCollectionRequest.IndexType.HNSW,
                ProtoConverter.toProtoIndex(io.gvdb.client.model.IndexType.HNSW));
        assertEquals(CreateCollectionRequest.IndexType.IVF_TURBOQUANT,
                ProtoConverter.toProtoIndex(io.gvdb.client.model.IndexType.IVF_TURBOQUANT));
    }

    @Test
    void highDimensionVector() {
        float[] values = new float[1536];
        for (int i = 0; i < values.length; i++) values[i] = i * 0.001f;
        var vector = new GvdbVector(99L, values);

        VectorWithId proto = ProtoConverter.toProto(vector);
        GvdbVector restored = ProtoConverter.fromProto(proto);

        assertEquals(1536, restored.dimension());
        assertArrayEquals(values, restored.values());
    }
}
