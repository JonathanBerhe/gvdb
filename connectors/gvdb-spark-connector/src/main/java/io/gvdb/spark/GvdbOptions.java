package io.gvdb.spark;

import io.gvdb.client.GvdbClientConfig;
import io.gvdb.client.model.IndexType;
import io.gvdb.client.model.MetricType;
import io.gvdb.spark.write.WriteMode;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Parses Spark DataSource options into typed configuration.
 * All options are prefixed with {@code gvdb.}.
 */
public final class GvdbOptions {

    public static final String PREFIX = "gvdb.";
    public static final String TARGET = PREFIX + "target";
    public static final String COLLECTION = PREFIX + "collection";
    public static final String API_KEY = PREFIX + "api_key";
    public static final String ID_COLUMN = PREFIX + "id_column";
    public static final String VECTOR_COLUMN = PREFIX + "vector_column";
    public static final String BATCH_SIZE = PREFIX + "batch_size";
    public static final String WRITE_MODE = PREFIX + "write_mode";
    public static final String MAX_RETRIES = PREFIX + "max_retries";
    public static final String TIMEOUT_SECONDS = PREFIX + "timeout_seconds";
    public static final String METRIC = PREFIX + "metric";
    public static final String INDEX_TYPE = PREFIX + "index_type";
    public static final String DIMENSION = PREFIX + "dimension";
    public static final String AUTO_CREATE = PREFIX + "auto_create_collection";

    // Read options
    public static final String PARTITION_SIZE = PREFIX + "read.partition_size";
    public static final String INCLUDE_METADATA = PREFIX + "include_metadata";

    private final Map<String, String> options;

    public GvdbOptions(Map<String, String> options) {
        this.options = options;
    }

    public String target() {
        var t = options.get(TARGET);
        if (t == null || t.isBlank()) {
            throw new IllegalArgumentException("Option '" + TARGET + "' is required");
        }
        return t;
    }

    public String collection() {
        var c = options.get(COLLECTION);
        if (c == null || c.isBlank()) {
            throw new IllegalArgumentException("Option '" + COLLECTION + "' is required");
        }
        return c;
    }

    public Optional<String> apiKey() {
        return Optional.ofNullable(options.get(API_KEY)).filter(s -> !s.isBlank());
    }

    public String idColumn() {
        return options.getOrDefault(ID_COLUMN, "id");
    }

    public String vectorColumn() {
        return options.getOrDefault(VECTOR_COLUMN, "vector");
    }

    public int batchSize() {
        return Integer.parseInt(options.getOrDefault(BATCH_SIZE, "10000"));
    }

    public WriteMode writeMode() {
        return WriteMode.fromOption(options.get(WRITE_MODE));
    }

    public int maxRetries() {
        return Integer.parseInt(options.getOrDefault(MAX_RETRIES, "3"));
    }

    public int timeoutSeconds() {
        return Integer.parseInt(options.getOrDefault(TIMEOUT_SECONDS, "30"));
    }

    public MetricType metric() {
        var m = options.getOrDefault(METRIC, "COSINE").toUpperCase();
        return MetricType.valueOf(m);
    }

    public IndexType indexType() {
        var t = options.getOrDefault(INDEX_TYPE, "AUTO").toUpperCase();
        return IndexType.valueOf(t);
    }

    public OptionalInt dimension() {
        var d = options.get(DIMENSION);
        return d == null ? OptionalInt.empty() : OptionalInt.of(Integer.parseInt(d));
    }

    public boolean autoCreate() {
        return Boolean.parseBoolean(options.getOrDefault(AUTO_CREATE, "true"));
    }

    public int partitionSize() {
        return Integer.parseInt(options.getOrDefault(PARTITION_SIZE, "100000"));
    }

    public boolean includeMetadata() {
        return Boolean.parseBoolean(options.getOrDefault(INCLUDE_METADATA, "true"));
    }

    public GvdbClientConfig toClientConfig() {
        var builder = GvdbClientConfig.builder(target())
                .batchSize(batchSize())
                .maxRetries(maxRetries())
                .timeout(Duration.ofSeconds(timeoutSeconds()));
        apiKey().ifPresent(builder::apiKey);
        return builder.build();
    }
}
