package io.gvdb.spark;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Set;

/**
 * Represents a GVDB collection as a Spark table.
 * Supports batch write and batch read.
 */
public class GvdbTable implements Table, SupportsWrite, SupportsRead {

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
        return Set.of(
                TableCapability.BATCH_WRITE,
                TableCapability.BATCH_READ,
                TableCapability.TRUNCATE
        );
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        return new io.gvdb.spark.write.GvdbWriteBuilder(info, options);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap caseInsensitiveStringMap) {
        return new io.gvdb.spark.read.GvdbScanBuilder(schema, options);
    }
}
