package io.gvdb.spark;

import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SchemaMapperTest {

    @Test
    void resolveOrdinals() {
        var schema = new StructType()
                .add("name", DataTypes.StringType)
                .add("id", DataTypes.LongType)
                .add("vector", new ArrayType(DataTypes.FloatType, false));

        int[] ordinals = SchemaMapper.resolveOrdinals(schema, "id", "vector");
        assertEquals(1, ordinals[0]); // id at index 1
        assertEquals(2, ordinals[1]); // vector at index 2
    }

    @Test
    void missingIdColumnThrows() {
        var schema = new StructType()
                .add("vector", new ArrayType(DataTypes.FloatType, false));

        assertThrows(IllegalArgumentException.class,
                () -> SchemaMapper.resolveOrdinals(schema, "id", "vector"));
    }

    @Test
    void missingVectorColumnThrows() {
        var schema = new StructType()
                .add("id", DataTypes.LongType);

        assertThrows(IllegalArgumentException.class,
                () -> SchemaMapper.resolveOrdinals(schema, "id", "vector"));
    }

    @Test
    void wrongIdTypeThrows() {
        var schema = new StructType()
                .add("id", DataTypes.StringType) // Wrong type
                .add("vector", new ArrayType(DataTypes.FloatType, false));

        assertThrows(IllegalArgumentException.class,
                () -> SchemaMapper.resolveOrdinals(schema, "id", "vector"));
    }

    @Test
    void wrongVectorTypeThrows() {
        var schema = new StructType()
                .add("id", DataTypes.LongType)
                .add("vector", DataTypes.StringType); // Wrong type

        assertThrows(IllegalArgumentException.class,
                () -> SchemaMapper.resolveOrdinals(schema, "id", "vector"));
    }

    @Test
    void vectorWithDoubleElementsThrows() {
        var schema = new StructType()
                .add("id", DataTypes.LongType)
                .add("vector", new ArrayType(DataTypes.DoubleType, false));

        assertThrows(IllegalArgumentException.class,
                () -> SchemaMapper.resolveOrdinals(schema, "id", "vector"));
    }

    @Test
    void integerIdTypeAllowed() {
        var schema = new StructType()
                .add("id", DataTypes.IntegerType)
                .add("vector", new ArrayType(DataTypes.FloatType, false));

        int[] ordinals = SchemaMapper.resolveOrdinals(schema, "id", "vector");
        assertEquals(0, ordinals[0]);
        assertEquals(1, ordinals[1]);
    }

    @Test
    void supportedMetadataTypes() {
        assertTrue(SchemaMapper.isSupportedMetadataType(DataTypes.IntegerType));
        assertTrue(SchemaMapper.isSupportedMetadataType(DataTypes.LongType));
        assertTrue(SchemaMapper.isSupportedMetadataType(DataTypes.FloatType));
        assertTrue(SchemaMapper.isSupportedMetadataType(DataTypes.DoubleType));
        assertTrue(SchemaMapper.isSupportedMetadataType(DataTypes.StringType));
        assertTrue(SchemaMapper.isSupportedMetadataType(DataTypes.BooleanType));
        assertFalse(SchemaMapper.isSupportedMetadataType(DataTypes.BinaryType));
        assertFalse(SchemaMapper.isSupportedMetadataType(DataTypes.DateType));
    }

    @Test
    void customColumnNames() {
        var schema = new StructType()
                .add("my_id", DataTypes.LongType)
                .add("embedding", new ArrayType(DataTypes.FloatType, false))
                .add("label", DataTypes.StringType);

        int[] ordinals = SchemaMapper.resolveOrdinals(schema, "my_id", "embedding");
        assertEquals(0, ordinals[0]);
        assertEquals(1, ordinals[1]);
    }
}
