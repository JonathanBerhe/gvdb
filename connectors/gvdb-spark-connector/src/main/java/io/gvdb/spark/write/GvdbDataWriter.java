package io.gvdb.spark.write;

import io.gvdb.client.GvdbClient;
import io.gvdb.client.GvdbClientConfig;
import io.gvdb.client.model.GvdbVector;
import io.gvdb.spark.SchemaMapper;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Per-partition writer that buffers vectors and flushes via gRPC Upsert.
 * Created on Spark executors after deserialization of {@link GvdbDataWriterFactory}.
 */
public class GvdbDataWriter implements DataWriter<InternalRow> {

    private static final Logger LOG = LoggerFactory.getLogger(GvdbDataWriter.class);

    private final GvdbClient client;
    private final String collection;
    private final String writeMode;
    private final StructType schema;
    private final int idOrdinal;
    private final int vectorOrdinal;
    private final int batchSize;
    private final int partitionId;
    private final long taskId;

    private final List<GvdbVector> buffer;
    private long totalWritten = 0;
    private boolean closed = false;

    GvdbDataWriter(String target, String apiKey, String collection,
                   int batchSize, int maxRetries, int timeoutSeconds,
                   String writeMode, StructType schema,
                   int idOrdinal, int vectorOrdinal,
                   int partitionId, long taskId) {
        var configBuilder = GvdbClientConfig.builder(target)
                .batchSize(batchSize)
                .maxRetries(maxRetries)
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .channelCount(1); // one channel per writer is sufficient
        if (apiKey != null) {
            configBuilder.apiKey(apiKey);
        }
        this.client = new GvdbClient(configBuilder.build());
        this.collection = collection;
        this.writeMode = writeMode;
        this.schema = schema;
        this.idOrdinal = idOrdinal;
        this.vectorOrdinal = vectorOrdinal;
        this.batchSize = batchSize;
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.buffer = new ArrayList<>(batchSize);
    }

    @Override
    public void write(InternalRow record) throws IOException {
        var vector = SchemaMapper.extractVector(record, schema, idOrdinal, vectorOrdinal);
        buffer.add(vector);
        if (buffer.size() >= batchSize) {
            flush();
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        flush();
        closeClient();
        LOG.info("Partition {} committed {} vectors to '{}'", partitionId, totalWritten, collection);
        return new GvdbWriterCommitMessage(partitionId, taskId, totalWritten);
    }

    @Override
    public void abort() throws IOException {
        closeClient();
        LOG.warn("Partition {} aborted after writing {} vectors to '{}'", partitionId, totalWritten, collection);
    }

    @Override
    public void close() throws IOException {
        closeClient();
    }

    private void closeClient() {
        if (!closed) {
            client.close();
            closed = true;
        }
    }

    private void flush() {
        if (buffer.isEmpty()) return;

        if ("stream_insert".equalsIgnoreCase(writeMode)) {
            client.streamInsert(collection, buffer.iterator(), batchSize);
        } else {
            client.upsert(collection, buffer);
        }
        totalWritten += buffer.size();
        buffer.clear();
    }
}
