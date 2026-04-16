package io.gvdb.flink;

import java.io.Serializable;
import java.util.Objects;

/**
 * Builder for {@link GvdbSink}. All configuration is serializable.
 *
 * <pre>
 * GvdbSink&lt;MyRecord&gt; sink = GvdbSink.&lt;MyRecord&gt;builder()
 *     .setTarget("localhost:50051")
 *     .setCollection("embeddings")
 *     .setBatchSize(10_000)
 *     .setRecordMapper(r -&gt; new GvdbVector(r.getId(), r.getEmbedding(), Map.of()))
 *     .build();
 * </pre>
 */
public final class GvdbSinkBuilder<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private String target;
    private String collection;
    private String apiKey;
    private int batchSize = 10_000;
    private int maxRetries = 3;
    private int timeoutSeconds = 30;
    private RecordMapper<T> recordMapper;

    GvdbSinkBuilder() {}

    public GvdbSinkBuilder<T> setTarget(String target) {
        this.target = target;
        return this;
    }

    public GvdbSinkBuilder<T> setCollection(String collection) {
        this.collection = collection;
        return this;
    }

    public GvdbSinkBuilder<T> setApiKey(String apiKey) {
        this.apiKey = apiKey;
        return this;
    }

    public GvdbSinkBuilder<T> setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public GvdbSinkBuilder<T> setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    public GvdbSinkBuilder<T> setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
        return this;
    }

    public GvdbSinkBuilder<T> setRecordMapper(RecordMapper<T> recordMapper) {
        this.recordMapper = recordMapper;
        return this;
    }

    public GvdbSink<T> build() {
        Objects.requireNonNull(target, "target is required");
        Objects.requireNonNull(collection, "collection is required");
        Objects.requireNonNull(recordMapper, "recordMapper is required");
        if (batchSize < 1) throw new IllegalArgumentException("batchSize must be >= 1");
        return new GvdbSink<>(target, collection, apiKey, batchSize, maxRetries, timeoutSeconds, recordMapper);
    }

    // Package-private accessors for GvdbSink
    String target() { return target; }
    String collection() { return collection; }
    String apiKey() { return apiKey; }
    int batchSize() { return batchSize; }
    int maxRetries() { return maxRetries; }
    int timeoutSeconds() { return timeoutSeconds; }
    RecordMapper<T> recordMapper() { return recordMapper; }
}
