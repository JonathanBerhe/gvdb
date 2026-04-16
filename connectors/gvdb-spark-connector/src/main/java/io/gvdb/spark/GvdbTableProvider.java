package io.gvdb.spark;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

/**
 * Entry point for the GVDB Spark DataSource V2 connector.
 * Registered as {@code "io.gvdb.spark"} via META-INF/services.
 *
 * <pre>
 * df.write.format("io.gvdb.spark")
 *     .option("gvdb.target", "localhost:50051")
 *     .option("gvdb.collection", "embeddings")
 *     .mode("append")
 *     .save()
 * </pre>
 */
public class GvdbTableProvider implements TableProvider, DataSourceRegister {

    @Override
    public String shortName() {
        return "gvdb";
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        // Schema is inferred from the DataFrame being written or from the collection on read.
        // Return null to let Spark use the DataFrame's schema for writes.
        return null;
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new GvdbTable(schema, new GvdbOptions(properties));
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }
}
