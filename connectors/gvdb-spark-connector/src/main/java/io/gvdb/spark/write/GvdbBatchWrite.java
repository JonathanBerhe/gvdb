package io.gvdb.spark.write;

import io.gvdb.client.GvdbClient;
import io.gvdb.spark.GvdbOptions;
import io.gvdb.spark.SchemaMapper;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates a batch write to a GVDB collection.
 * Handles collection auto-creation and truncation (Overwrite mode).
 */
public class GvdbBatchWrite implements BatchWrite {

    private static final Logger LOG = LoggerFactory.getLogger(GvdbBatchWrite.class);

    private final StructType schema;
    private final GvdbOptions options;
    private final boolean truncate;

    public GvdbBatchWrite(StructType schema, GvdbOptions options, boolean truncate) {
        this.schema = schema;
        this.options = options;
        this.truncate = truncate;

        prepareCollection();
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        int[] ordinals = SchemaMapper.resolveOrdinals(schema, options.idColumn(), options.vectorColumn());
        return new GvdbDataWriterFactory(
                options.target(),
                options.apiKey().orElse(null),
                options.collection(),
                options.batchSize(),
                options.maxRetries(),
                options.timeoutSeconds(),
                options.writeMode(),
                schema,
                ordinals[0],
                ordinals[1]
        );
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        long total = 0;
        for (var msg : messages) {
            if (msg instanceof GvdbWriterCommitMessage m) {
                total += m.vectorsWritten();
            }
        }
        LOG.info("GVDB write committed: {} vectors to collection '{}'", total, options.collection());
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        LOG.warn("GVDB write aborted for collection '{}'. {} partitions had partial writes (at-least-once, no rollback).",
                options.collection(), messages.length);
    }

    private void prepareCollection() {
        try (var client = new GvdbClient(options.toClientConfig())) {
            var collections = client.listCollections();
            boolean exists = collections.stream()
                    .anyMatch(c -> c.name().equals(options.collection()));

            if (truncate && exists) {
                LOG.info("Truncate mode: dropping and recreating collection '{}'", options.collection());
                client.dropCollection(options.collection());
                exists = false;
            }

            if (!exists && options.autoCreate()) {
                int dim = options.dimension().orElseThrow(() -> new IllegalArgumentException(
                        "Collection '" + options.collection() + "' does not exist and " +
                                "'" + GvdbOptions.DIMENSION + "' was not set. Cannot auto-create."));
                if (dim <= 0) {
                    throw new IllegalArgumentException(
                            "'" + GvdbOptions.DIMENSION + "' must be > 0, got " + dim);
                }
                LOG.info("Auto-creating collection '{}' (dimension={}, metric={}, index={})",
                        options.collection(), dim, options.metric(), options.indexType());
                client.createCollection(options.collection(), dim, options.metric(), options.indexType());
            } else if (!exists) {
                throw new IllegalArgumentException(
                        "Collection '" + options.collection() + "' does not exist and auto_create is disabled.");
            }
        }
    }
}
