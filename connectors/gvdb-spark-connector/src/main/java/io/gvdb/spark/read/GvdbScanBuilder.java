package io.gvdb.spark.read;

import io.gvdb.spark.GvdbOptions;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;

/**
 * Builds a scan for reading vectors from a GVDB collection.
 */
public class GvdbScanBuilder implements ScanBuilder {

    private final StructType schema;
    private final GvdbOptions options;

    public GvdbScanBuilder(StructType schema, GvdbOptions options) {
        this.schema = schema;
        this.options = options;
    }

    @Override
    public Scan build() {
        return new GvdbScan(schema, options);
    }
}
