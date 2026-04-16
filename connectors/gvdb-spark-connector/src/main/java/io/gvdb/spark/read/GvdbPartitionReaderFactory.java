package io.gvdb.spark.read;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

/**
 * Factory for creating partition readers on Spark executors.
 * Serializable — carries only primitive config, creates GvdbClient on the executor.
 */
public class GvdbPartitionReaderFactory implements PartitionReaderFactory, Serializable {

    private static final long serialVersionUID = 1L;

    private final String target;
    private final String apiKey;
    private final String collection;
    private final int timeoutSeconds;
    private final int maxRetries;
    private final boolean includeMetadata;
    private final StructType schema;

    GvdbPartitionReaderFactory(String target, String apiKey, String collection,
                               int timeoutSeconds, int maxRetries,
                               boolean includeMetadata, StructType schema) {
        this.target = target;
        this.apiKey = apiKey;
        this.collection = collection;
        this.timeoutSeconds = timeoutSeconds;
        this.maxRetries = maxRetries;
        this.includeMetadata = includeMetadata;
        this.schema = schema;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        var gvdbPartition = (GvdbInputPartition) partition;
        return new GvdbPartitionReader(
                target, apiKey, collection, timeoutSeconds, maxRetries,
                includeMetadata, schema,
                gvdbPartition.startOffset(), gvdbPartition.endOffset()
        );
    }
}
