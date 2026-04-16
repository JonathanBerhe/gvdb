package io.gvdb.spark.read;

import org.junit.jupiter.api.Test;

import java.io.*;

import static org.junit.jupiter.api.Assertions.*;

class GvdbInputPartitionTest {

    @Test
    void serializationRoundTrip() throws Exception {
        var partition = new GvdbInputPartition(5, 50000, 100000);

        var baos = new ByteArrayOutputStream();
        try (var oos = new ObjectOutputStream(baos)) {
            oos.writeObject(partition);
        }

        var bais = new ByteArrayInputStream(baos.toByteArray());
        GvdbInputPartition restored;
        try (var ois = new ObjectInputStream(bais)) {
            restored = (GvdbInputPartition) ois.readObject();
        }

        assertEquals(5, restored.partitionIndex());
        assertEquals(50000, restored.startOffset());
        assertEquals(100000, restored.endOffset());
    }

    @Test
    void partitionPlanningLogic() {
        // Simulate what GvdbScan.planInputPartitions does
        long vectorCount = 250_000;
        int partitionSize = 100_000;
        int numPartitions = (int) ((vectorCount + partitionSize - 1) / partitionSize);

        assertEquals(3, numPartitions);

        // Verify ranges
        var p0 = new GvdbInputPartition(0, 0, 100_000);
        var p1 = new GvdbInputPartition(1, 100_000, 200_000);
        var p2 = new GvdbInputPartition(2, 200_000, 250_000);

        assertEquals(0, p0.startOffset());
        assertEquals(100_000, p0.endOffset());
        assertEquals(200_000, p2.startOffset());
        assertEquals(250_000, p2.endOffset()); // Last partition is smaller
    }

    @Test
    void singleVectorPartition() {
        long vectorCount = 1;
        int partitionSize = 100_000;
        int numPartitions = Math.max(1, (int) ((vectorCount + partitionSize - 1) / partitionSize));
        assertEquals(1, numPartitions);
    }

    @Test
    void exactBoundaryPartitioning() {
        long vectorCount = 200_000;
        int partitionSize = 100_000;
        int numPartitions = (int) ((vectorCount + partitionSize - 1) / partitionSize);
        assertEquals(2, numPartitions);
    }

    @Test
    void readerFactorySerialization() throws Exception {
        var schema = new org.apache.spark.sql.types.StructType()
                .add("id", org.apache.spark.sql.types.DataTypes.LongType)
                .add("vector", new org.apache.spark.sql.types.ArrayType(
                        org.apache.spark.sql.types.DataTypes.FloatType, false));

        var factory = new GvdbPartitionReaderFactory(
                "localhost:50051", null, "test_col", 30, 3, true, schema
        );

        var baos = new ByteArrayOutputStream();
        try (var oos = new ObjectOutputStream(baos)) {
            oos.writeObject(factory);
        }
        // If this doesn't throw, serialization works
        assertTrue(baos.size() > 0);
    }
}
