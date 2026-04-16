package io.gvdb.spark.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

/**
 * Factory that creates {@link GvdbDataWriter} instances on Spark executors.
 * <p>
 * This class is serialized to executors — it carries only primitive config values.
 * The {@link io.gvdb.client.GvdbClient} is created inside {@link #createWriter},
 * which runs on the executor after deserialization.
 */
public class GvdbDataWriterFactory implements DataWriterFactory, Serializable {

    private static final long serialVersionUID = 1L;

    private final String target;
    private final String apiKey;
    private final String collection;
    private final int batchSize;
    private final int maxRetries;
    private final int timeoutSeconds;
    private final String writeMode;
    private final StructType schema;
    private final int idOrdinal;
    private final int vectorOrdinal;

    GvdbDataWriterFactory(String target, String apiKey, String collection,
                          int batchSize, int maxRetries, int timeoutSeconds,
                          String writeMode, StructType schema,
                          int idOrdinal, int vectorOrdinal) {
        this.target = target;
        this.apiKey = apiKey;
        this.collection = collection;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.timeoutSeconds = timeoutSeconds;
        this.writeMode = writeMode;
        this.schema = schema;
        this.idOrdinal = idOrdinal;
        this.vectorOrdinal = vectorOrdinal;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        return new GvdbDataWriter(
                target, apiKey, collection, batchSize, maxRetries, timeoutSeconds,
                writeMode, schema, idOrdinal, vectorOrdinal, partitionId, taskId
        );
    }

    // Accessors for testing
    String target() { return target; }
    String collection() { return collection; }
    int batchSize() { return batchSize; }
    String writeMode() { return writeMode; }
}
