package io.gvdb.spark.write;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.io.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Ensures GvdbDataWriterFactory can be serialized to Spark executors.
 * If this fails, Spark will throw NotSerializableException at runtime.
 */
class GvdbDataWriterFactorySerializationTest {

    @Test
    void serializationRoundTrip() throws Exception {
        var schema = new StructType()
                .add("id", DataTypes.LongType)
                .add("vector", new ArrayType(DataTypes.FloatType, false))
                .add("label", DataTypes.StringType);

        var factory = new GvdbDataWriterFactory(
                "localhost:50051",
                "my-key",
                "test_collection",
                10000,
                3,
                30,
                WriteMode.UPSERT,
                schema,
                0, // idOrdinal
                1  // vectorOrdinal
        );

        var baos = new ByteArrayOutputStream();
        try (var oos = new ObjectOutputStream(baos)) {
            oos.writeObject(factory);
        }

        var bais = new ByteArrayInputStream(baos.toByteArray());
        GvdbDataWriterFactory restored;
        try (var ois = new ObjectInputStream(bais)) {
            restored = (GvdbDataWriterFactory) ois.readObject();
        }

        assertEquals("localhost:50051", restored.target());
        assertEquals("test_collection", restored.collection());
        assertEquals(10000, restored.batchSize());
        assertEquals(WriteMode.UPSERT, restored.writeMode());
    }

    @Test
    void serializationWithNullApiKey() throws Exception {
        var schema = new StructType()
                .add("id", DataTypes.LongType)
                .add("vector", new ArrayType(DataTypes.FloatType, false));

        var factory = new GvdbDataWriterFactory(
                "host:9090", null, "col", 5000, 0, 60, WriteMode.STREAM_INSERT,
                schema, 0, 1
        );

        var baos = new ByteArrayOutputStream();
        try (var oos = new ObjectOutputStream(baos)) {
            oos.writeObject(factory);
        }

        var bais = new ByteArrayInputStream(baos.toByteArray());
        try (var ois = new ObjectInputStream(bais)) {
            var restored = (GvdbDataWriterFactory) ois.readObject();
            assertEquals("host:9090", restored.target());
        }
    }
}
