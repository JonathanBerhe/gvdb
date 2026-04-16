package io.gvdb.spark.write;

import io.gvdb.spark.GvdbOptions;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

/**
 * Builds write operations for the GVDB connector.
 * Implements {@link SupportsTruncate} so Overwrite mode can drop+recreate the collection.
 */
public class GvdbWriteBuilder implements WriteBuilder, SupportsTruncate {

    private final StructType schema;
    private final GvdbOptions options;
    private boolean truncate = false;

    public GvdbWriteBuilder(LogicalWriteInfo info, GvdbOptions options) {
        this.schema = info.schema();
        this.options = options;
    }

    @Override
    public WriteBuilder truncate() {
        this.truncate = true;
        return this;
    }

    @Override
    public BatchWrite buildForBatch() {
        return new GvdbBatchWrite(schema, options, truncate);
    }
}
