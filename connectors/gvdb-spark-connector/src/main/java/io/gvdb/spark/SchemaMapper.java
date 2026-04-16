package io.gvdb.spark;

import io.gvdb.client.model.GvdbVector;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps between Spark schemas and GVDB vector/metadata types.
 * Follows the GVDB Parquet schema convention: id + vector + remaining columns as metadata.
 */
public final class SchemaMapper {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaMapper.class);

    private SchemaMapper() {}

    /**
     * Extract a GvdbVector from a Spark InternalRow.
     *
     * @param row          the Spark row
     * @param schema       the row schema
     * @param idOrdinal    ordinal of the id column
     * @param vectorOrdinal ordinal of the vector column
     */
    public static GvdbVector extractVector(InternalRow row, StructType schema,
                                           int idOrdinal, int vectorOrdinal) {
        long id = schema.fields()[idOrdinal].dataType() instanceof IntegerType
                ? row.getInt(idOrdinal) : row.getLong(idOrdinal);
        var arrayData = row.getArray(vectorOrdinal);
        float[] values = new float[arrayData.numElements()];
        for (int i = 0; i < values.length; i++) {
            values[i] = arrayData.getFloat(i);
        }

        Map<String, Object> metadata = new LinkedHashMap<>();
        var fields = schema.fields();
        for (int i = 0; i < fields.length; i++) {
            if (i == idOrdinal || i == vectorOrdinal) continue;
            if (row.isNullAt(i)) continue;

            String name = fields[i].name();
            DataType dt = fields[i].dataType();
            Object value = extractMetadataValue(row, i, dt);
            if (value != null) {
                metadata.put(name, value);
            }
        }

        return new GvdbVector(id, values, metadata);
    }

    /**
     * Validate that the schema has the required id and vector columns.
     *
     * @return ordinals as int[2] = {idOrdinal, vectorOrdinal}
     */
    public static int[] resolveOrdinals(StructType schema, String idColumn, String vectorColumn) {
        int idOrdinal = -1;
        int vectorOrdinal = -1;

        var fields = schema.fields();
        for (int i = 0; i < fields.length; i++) {
            if (fields[i].name().equals(idColumn)) {
                idOrdinal = i;
                validateIdType(fields[i].dataType(), idColumn);
            } else if (fields[i].name().equals(vectorColumn)) {
                vectorOrdinal = i;
                validateVectorType(fields[i].dataType(), vectorColumn);
            }
        }

        if (idOrdinal == -1) {
            throw new IllegalArgumentException(
                    "Schema missing id column '" + idColumn + "'. Available: " + schema.simpleString());
        }
        if (vectorOrdinal == -1) {
            throw new IllegalArgumentException(
                    "Schema missing vector column '" + vectorColumn + "'. Available: " + schema.simpleString());
        }

        return new int[]{idOrdinal, vectorOrdinal};
    }

    /**
     * Check if a Spark DataType is a supported metadata type.
     */
    public static boolean isSupportedMetadataType(DataType dt) {
        return dt instanceof IntegerType
                || dt instanceof LongType
                || dt instanceof FloatType
                || dt instanceof DoubleType
                || dt instanceof StringType
                || dt instanceof BooleanType;
    }

    private static Object extractMetadataValue(InternalRow row, int ordinal, DataType dt) {
        if (dt instanceof IntegerType) return (long) row.getInt(ordinal);
        if (dt instanceof LongType) return row.getLong(ordinal);
        if (dt instanceof FloatType) return (double) row.getFloat(ordinal);
        if (dt instanceof DoubleType) return row.getDouble(ordinal);
        if (dt instanceof StringType) return row.getString(ordinal);
        if (dt instanceof BooleanType) return row.getBoolean(ordinal);
        LOG.warn("Skipping unsupported metadata type '{}' at ordinal {}", dt.simpleString(), ordinal);
        return null;
    }

    private static void validateIdType(DataType dt, String column) {
        if (!(dt instanceof LongType || dt instanceof IntegerType)) {
            throw new IllegalArgumentException(
                    "Id column '" + column + "' must be LongType or IntegerType, got " + dt.simpleString());
        }
    }

    private static void validateVectorType(DataType dt, String column) {
        if (dt instanceof ArrayType at) {
            if (!(at.elementType() instanceof FloatType)) {
                throw new IllegalArgumentException(
                        "Vector column '" + column + "' must be ArrayType<FloatType>, got ArrayType<"
                                + at.elementType().simpleString() + ">");
            }
        } else {
            throw new IllegalArgumentException(
                    "Vector column '" + column + "' must be ArrayType<FloatType>, got " + dt.simpleString());
        }
    }
}
