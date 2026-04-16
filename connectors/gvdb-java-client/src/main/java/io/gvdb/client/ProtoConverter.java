package io.gvdb.client;

import io.gvdb.client.model.GvdbVector;
import io.gvdb.client.model.ImportStatus;
import io.gvdb.proto.GetImportStatusResponse;
import io.gvdb.proto.ImportState;
import io.gvdb.proto.Metadata;
import io.gvdb.proto.MetadataValue;
import io.gvdb.proto.Vector;
import io.gvdb.proto.VectorWithId;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Bidirectional conversion between domain model and protobuf types.
 * Package-private — protobuf types never leak to the public API.
 */
final class ProtoConverter {

    private ProtoConverter() {}

    // ---- Domain -> Proto ----

    static VectorWithId toProto(GvdbVector vector) {
        var builder = VectorWithId.newBuilder()
                .setId(vector.id())
                .setVector(Vector.newBuilder()
                        .addAllValues(toFloatList(vector.values()))
                        .setDimension(vector.values().length)
                        .build());

        if (vector.metadata() != null && !vector.metadata().isEmpty()) {
            builder.setMetadata(toProtoMetadata(vector.metadata()));
        }
        return builder.build();
    }

    static List<VectorWithId> toProtoList(List<GvdbVector> vectors) {
        var result = new ArrayList<VectorWithId>(vectors.size());
        for (var v : vectors) {
            result.add(toProto(v));
        }
        return result;
    }

    static Metadata toProtoMetadata(Map<String, Object> metadata) {
        var builder = Metadata.newBuilder();
        for (var entry : metadata.entrySet()) {
            var key = entry.getKey();
            var value = entry.getValue();
            if (value instanceof Long l) {
                builder.putFields(key, MetadataValue.newBuilder().setIntValue(l).build());
            } else if (value instanceof Integer i) {
                builder.putFields(key, MetadataValue.newBuilder().setIntValue(i.longValue()).build());
            } else if (value instanceof Double d) {
                builder.putFields(key, MetadataValue.newBuilder().setDoubleValue(d).build());
            } else if (value instanceof Float f) {
                builder.putFields(key, MetadataValue.newBuilder().setDoubleValue(f.doubleValue()).build());
            } else if (value instanceof String s) {
                builder.putFields(key, MetadataValue.newBuilder().setStringValue(s).build());
            } else if (value instanceof Boolean b) {
                builder.putFields(key, MetadataValue.newBuilder().setBoolValue(b).build());
            }
            // Skip unsupported types silently — matches Python SDK behavior
        }
        return builder.build();
    }

    static io.gvdb.proto.CreateCollectionRequest.MetricType toProtoMetric(
            io.gvdb.client.model.MetricType metric) {
        return switch (metric) {
            case L2 -> io.gvdb.proto.CreateCollectionRequest.MetricType.L2;
            case INNER_PRODUCT -> io.gvdb.proto.CreateCollectionRequest.MetricType.INNER_PRODUCT;
            case COSINE -> io.gvdb.proto.CreateCollectionRequest.MetricType.COSINE;
        };
    }

    static io.gvdb.proto.CreateCollectionRequest.IndexType toProtoIndex(
            io.gvdb.client.model.IndexType indexType) {
        return switch (indexType) {
            case FLAT -> io.gvdb.proto.CreateCollectionRequest.IndexType.FLAT;
            case HNSW -> io.gvdb.proto.CreateCollectionRequest.IndexType.HNSW;
            case IVF_FLAT -> io.gvdb.proto.CreateCollectionRequest.IndexType.IVF_FLAT;
            case IVF_PQ -> io.gvdb.proto.CreateCollectionRequest.IndexType.IVF_PQ;
            case IVF_SQ -> io.gvdb.proto.CreateCollectionRequest.IndexType.IVF_SQ;
            case IVF_TURBOQUANT -> io.gvdb.proto.CreateCollectionRequest.IndexType.IVF_TURBOQUANT;
            case TURBOQUANT -> io.gvdb.proto.CreateCollectionRequest.IndexType.TURBOQUANT;
            case AUTO -> io.gvdb.proto.CreateCollectionRequest.IndexType.AUTO;
        };
    }

    // ---- Proto -> Domain ----

    static GvdbVector fromProto(VectorWithId proto) {
        float[] values = toFloatArray(proto.getVector().getValuesList());
        Map<String, Object> metadata = fromProtoMetadata(proto.getMetadata());
        return new GvdbVector(proto.getId(), values, metadata);
    }

    static List<GvdbVector> fromProtoList(List<VectorWithId> protos) {
        var result = new ArrayList<GvdbVector>(protos.size());
        for (var p : protos) {
            result.add(fromProto(p));
        }
        return result;
    }

    static Map<String, Object> fromProtoMetadata(Metadata proto) {
        if (proto == null || proto.getFieldsCount() == 0) {
            return Map.of();
        }
        var result = new LinkedHashMap<String, Object>(proto.getFieldsCount());
        for (var entry : proto.getFieldsMap().entrySet()) {
            var mv = entry.getValue();
            var valueCase = mv.getValueCase();
            switch (valueCase) {
                case INT_VALUE -> result.put(entry.getKey(), mv.getIntValue());
                case DOUBLE_VALUE -> result.put(entry.getKey(), mv.getDoubleValue());
                case STRING_VALUE -> result.put(entry.getKey(), mv.getStringValue());
                case BOOL_VALUE -> result.put(entry.getKey(), mv.getBoolValue());
                default -> {} // VALUE_NOT_SET — skip
            }
        }
        return Map.copyOf(result);
    }

    static io.gvdb.client.model.CollectionInfo fromProto(io.gvdb.proto.CollectionInfo proto) {
        return new io.gvdb.client.model.CollectionInfo(
                proto.getCollectionName(),
                proto.getCollectionId(),
                proto.getDimension(),
                proto.getMetricType(),
                proto.getVectorCount()
        );
    }

    static ImportStatus fromProto(GetImportStatusResponse proto) {
        return new ImportStatus(
                proto.getImportId(),
                fromProtoImportState(proto.getState()),
                proto.getTotalVectors(),
                proto.getImportedVectors(),
                proto.getProgressPercent(),
                proto.getErrorMessage(),
                proto.getElapsedSeconds(),
                proto.getSegmentsCreated()
        );
    }

    static ImportStatus.ImportState fromProtoImportState(ImportState proto) {
        return switch (proto) {
            case IMPORT_PENDING -> ImportStatus.ImportState.PENDING;
            case IMPORT_RUNNING -> ImportStatus.ImportState.RUNNING;
            case IMPORT_COMPLETED -> ImportStatus.ImportState.COMPLETED;
            case IMPORT_FAILED -> ImportStatus.ImportState.FAILED;
            case IMPORT_CANCELLED -> ImportStatus.ImportState.CANCELLED;
            default -> ImportStatus.ImportState.PENDING;
        };
    }

    // ---- Helpers ----

    private static List<Float> toFloatList(float[] array) {
        var list = new ArrayList<Float>(array.length);
        for (float f : array) {
            list.add(f);
        }
        return list;
    }

    private static float[] toFloatArray(List<Float> list) {
        float[] array = new float[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }
        return array;
    }
}
