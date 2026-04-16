package io.gvdb.spark;

import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

import java.util.HashSet;
import java.util.Set;

/**
 * Represents a GVDB collection as a Spark table.
 * Supports batch write (Phase 2) and batch read (Phase 3).
 */
public class GvdbTable implements Table, SupportsWrite {

    private final StructType schema;
    private final GvdbOptions options;

    GvdbTable(StructType schema, GvdbOptions options) {
        this.schema = schema;
        this.options = options;
    }

    @Override
    public String name() {
        return "gvdb:" + options.collection();
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        var caps = new HashSet<TableCapability>();
        caps.add(TableCapability.BATCH_WRITE);
        caps.add(TableCapability.TRUNCATE);
        return caps;
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        return new io.gvdb.spark.write.GvdbWriteBuilder(info, options);
    }
}
