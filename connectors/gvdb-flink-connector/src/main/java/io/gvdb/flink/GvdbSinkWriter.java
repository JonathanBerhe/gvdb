package io.gvdb.flink;

import io.gvdb.client.GvdbClient;
import io.gvdb.client.GvdbClientConfig;
import io.gvdb.client.model.GvdbVector;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Buffered writer that flushes to GVDB via Upsert on size threshold or checkpoint barrier.
 * <p>
 * Backpressure: {@code flushBuffer()} is synchronous. When GVDB is slow, the Upsert RPC blocks,
 * {@code write()} blocks, Flink's mailbox thread blocks, backpressure propagates upstream.
 */
final class GvdbSinkWriter<T> implements SinkWriter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(GvdbSinkWriter.class);

    private final GvdbClient client;
    private final String collection;
    private final int batchSize;
    private final RecordMapper<T> recordMapper;
    private final List<GvdbVector> buffer;
    private long totalWritten = 0;

    GvdbSinkWriter(String target, String collection, String apiKey,
                   int batchSize, int maxRetries, int timeoutSeconds,
                   RecordMapper<T> recordMapper) {
        this(buildClient(target, apiKey, batchSize, maxRetries, timeoutSeconds),
                collection, batchSize, recordMapper);
    }

    // Test seam: inject a pre-built client.
    GvdbSinkWriter(GvdbClient client, String collection, int batchSize, RecordMapper<T> recordMapper) {
        this.client = client;
        this.collection = collection;
        this.batchSize = batchSize;
        this.recordMapper = recordMapper;
        this.buffer = new ArrayList<>(batchSize);
    }

    private static GvdbClient buildClient(String target, String apiKey,
                                          int batchSize, int maxRetries, int timeoutSeconds) {
        var configBuilder = GvdbClientConfig.builder(target)
                .batchSize(batchSize)
                .maxRetries(maxRetries)
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .channelCount(1);
        if (apiKey != null) {
            configBuilder.apiKey(apiKey);
        }
        return new GvdbClient(configBuilder.build());
    }

    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
        buffer.add(recordMapper.map(element));
        if (buffer.size() >= batchSize) {
            flushBuffer();
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        flushBuffer();
        if (endOfInput) {
            LOG.info("End of input: flushed {} total vectors to '{}'", totalWritten, collection);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            flushBuffer();
        } finally {
            client.close();
            LOG.info("GvdbSinkWriter closed after writing {} vectors to '{}'", totalWritten, collection);
        }
    }

    private void flushBuffer() {
        if (buffer.isEmpty()) return;

        client.upsert(collection, new ArrayList<>(buffer));
        totalWritten += buffer.size();
        buffer.clear();
    }

    // Package-private for testing
    long totalWritten() { return totalWritten; }
    int bufferSize() { return buffer.size(); }
}
