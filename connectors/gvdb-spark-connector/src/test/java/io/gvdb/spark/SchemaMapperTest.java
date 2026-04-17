package io.gvdb.spark;

import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("SchemaMapper")
class SchemaMapperTest {

    @Nested
    @DisplayName("resolveOrdinals(schema, idColumn, vectorColumn)")
    class ResolveOrdinals {

        @Test
        @DisplayName("finds id and vector columns at their positions")
        void resolveOrdinals() {
            var schema = new StructType()
                    .add("name", DataTypes.StringType)
                    .add("id", DataTypes.LongType)
                    .add("vector", new ArrayType(DataTypes.FloatType, false));

            int[] ordinals = SchemaMapper.resolveOrdinals(schema, "id", "vector");
            assertEquals(1, ordinals[0]);
            assertEquals(2, ordinals[1]);
        }

        @Test
        @DisplayName("accepts IntegerType for id")
        void integerIdTypeAllowed() {
            var schema = new StructType()
                    .add("id", DataTypes.IntegerType)
                    .add("vector", new ArrayType(DataTypes.FloatType, false));

            int[] ordinals = SchemaMapper.resolveOrdinals(schema, "id", "vector");
            assertEquals(0, ordinals[0]);
            assertEquals(1, ordinals[1]);
        }

        @Test
        @DisplayName("resolves custom column names")
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

    @Nested
    @DisplayName("schema validation")
    class Validation {

        @Test
        @DisplayName("throws when id column is missing")
        void missingIdColumnThrows() {
            var schema = new StructType()
                    .add("vector", new ArrayType(DataTypes.FloatType, false));

            assertThrows(IllegalArgumentException.class,
                    () -> SchemaMapper.resolveOrdinals(schema, "id", "vector"));
        }

        @Test
        @DisplayName("throws when vector column is missing")
        void missingVectorColumnThrows() {
            var schema = new StructType()
                    .add("id", DataTypes.LongType);

            assertThrows(IllegalArgumentException.class,
                    () -> SchemaMapper.resolveOrdinals(schema, "id", "vector"));
        }

        @Test
        @DisplayName("throws when id is not integral")
        void wrongIdTypeThrows() {
            var schema = new StructType()
                    .add("id", DataTypes.StringType)
                    .add("vector", new ArrayType(DataTypes.FloatType, false));

            assertThrows(IllegalArgumentException.class,
                    () -> SchemaMapper.resolveOrdinals(schema, "id", "vector"));
        }

        @Test
        @DisplayName("throws when vector is not an array type")
        void wrongVectorTypeThrows() {
            var schema = new StructType()
                    .add("id", DataTypes.LongType)
                    .add("vector", DataTypes.StringType);

            assertThrows(IllegalArgumentException.class,
                    () -> SchemaMapper.resolveOrdinals(schema, "id", "vector"));
        }

        @Test
        @DisplayName("throws when vector elements are not floats")
        void vectorWithDoubleElementsThrows() {
            var schema = new StructType()
                    .add("id", DataTypes.LongType)
                    .add("vector", new ArrayType(DataTypes.DoubleType, false));

            assertThrows(IllegalArgumentException.class,
                    () -> SchemaMapper.resolveOrdinals(schema, "id", "vector"));
        }
    }

    @Test
    @DisplayName("isSupportedMetadataType covers primitives and strings, excludes binary and date")
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
}
