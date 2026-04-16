package io.gvdb.spark.read;

import io.gvdb.client.GvdbClient;
import io.gvdb.client.model.CollectionInfo;
import io.gvdb.spark.GvdbOptions;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Plans the scan: determines partitions from the collection's vector count
 * and infers the schema from sample vectors if not provided.
 */
public class GvdbScan implements Scan, Batch {

    private static final Logger LOG = LoggerFactory.getLogger(GvdbScan.class);

    private final StructType schema;
    private final GvdbOptions options;

    GvdbScan(StructType schema, GvdbOptions options) {
        this.schema = schema != null ? schema : inferSchema(options);
        this.options = options;
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        long vectorCount = getVectorCount();
        int partitionSize = options.partitionSize();

        if (vectorCount == 0) {
            return new InputPartition[]{new GvdbInputPartition(0, 0, 0)};
        }

        int numPartitions = Math.max(1, (int) ((vectorCount + partitionSize - 1) / partitionSize));
        var partitions = new InputPartition[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            long start = (long) i * partitionSize;
            long end = Math.min(start + partitionSize, vectorCount);
            partitions[i] = new GvdbInputPartition(i, start, end);
        }

        LOG.info("Planned {} partitions for collection '{}' ({} vectors, partition size {})",
                numPartitions, options.collection(), vectorCount, partitionSize);
        return partitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new GvdbPartitionReaderFactory(
                options.target(),
                options.apiKey(),
                options.collection(),
                options.timeoutSeconds(),
                options.maxRetries(),
                options.includeMetadata(),
                schema
        );
    }

    private long getVectorCount() {
        try (var client = new GvdbClient(options.toClientConfig())) {
            return client.listCollections().stream()
                    .filter(c -> c.name().equals(options.collection()))
                    .mapToLong(CollectionInfo::vectorCount)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Collection '" + options.collection() + "' not found"));
        }
    }

    /**
     * Infer schema from the collection: id + vector + metadata fields from a sample.
     */
    static StructType inferSchema(GvdbOptions options) {
        try (var client = new GvdbClient(options.toClientConfig())) {
            var collections = client.listCollections();
            var collectionInfo = collections.stream()
                    .filter(c -> c.name().equals(options.collection()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Collection '" + options.collection() + "' not found for schema inference"));

            var builder = new StructType()
                    .add("id", DataTypes.LongType, false)
                    .add("vector", new ArrayType(DataTypes.FloatType, false), false);

            // Sample vectors to infer metadata schema
            if (options.includeMetadata() && collectionInfo.vectorCount() > 0) {
                var sample = client.listVectors(options.collection(), 100, 0, true);
                for (var vector : sample) {
                    for (var entry : vector.metadata().entrySet()) {
                        String name = entry.getKey();
                        // Skip if already added
                        if (builder.getFieldIndex(name).isDefined()) continue;

                        DataType dt = inferMetadataType(entry.getValue());
                        if (dt != null) {
                            builder = builder.add(name, dt, true);
                        }
                    }
                }
            }

            LOG.info("Inferred schema for '{}': {}", options.collection(), builder.simpleString());
            return builder;
        }
    }

    static DataType inferMetadataType(Object value) {
        if (value instanceof Long) return DataTypes.LongType;
        if (value instanceof Integer) return DataTypes.LongType;
        if (value instanceof Double) return DataTypes.DoubleType;
        if (value instanceof Float) return DataTypes.DoubleType;
        if (value instanceof String) return DataTypes.StringType;
        if (value instanceof Boolean) return DataTypes.BooleanType;
        return null;
    }
}
