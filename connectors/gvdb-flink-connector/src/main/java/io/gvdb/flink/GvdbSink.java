package io.gvdb.flink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;
import java.io.Serializable;

/**
 * Apache Flink Sink V2 (FLIP-191) for writing vectors to GVDB.
 * <p>
 * At-least-once semantics via upsert idempotency:
 * <ul>
 *   <li>{@code flush()} is called on checkpoint barriers</li>
 *   <li>Synchronous flush blocks the writer, propagating backpressure</li>
 *   <li>On failure, Flink replays from last checkpoint — re-upserted data is idempotent</li>
 * </ul>
 * <p>
 * No {@code Committer} or {@code GlobalCommitter} — upsert idempotency eliminates the need for 2PC.
 *
 * @param <T> the input element type
 */
public final class GvdbSink<T> implements Sink<T>, Serializable {

    private static final long serialVersionUID = 1L;

    private final String target;
    private final String collection;
    private final String apiKey;
    private final int batchSize;
    private final int maxRetries;
    private final int timeoutSeconds;
    private final RecordMapper<T> recordMapper;

    GvdbSink(String target, String collection, String apiKey,
             int batchSize, int maxRetries, int timeoutSeconds,
             RecordMapper<T> recordMapper) {
        this.target = target;
        this.collection = collection;
        this.apiKey = apiKey;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.timeoutSeconds = timeoutSeconds;
        this.recordMapper = recordMapper;
    }

    public static <T> GvdbSinkBuilder<T> builder() {
        return new GvdbSinkBuilder<>();
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        return new GvdbSinkWriter<>(
                target, collection, apiKey, batchSize, maxRetries, timeoutSeconds, recordMapper
        );
    }
}
