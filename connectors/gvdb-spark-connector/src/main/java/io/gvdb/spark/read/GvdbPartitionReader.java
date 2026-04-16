package io.gvdb.spark.read;

import io.gvdb.client.GvdbClient;
import io.gvdb.client.GvdbClientConfig;
import io.gvdb.client.model.GvdbVector;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;

/**
 * Reads vectors from a GVDB collection for a single partition's offset range.
 * Fetches pages lazily via {@code ListVectors} RPC.
 */
public class GvdbPartitionReader implements PartitionReader<InternalRow> {

    private static final int PAGE_SIZE = 10_000;

    private final GvdbClient client;
    private final String collection;
    private final boolean includeMetadata;
    private final StructType schema;
    private final long endOffset;

    private long currentOffset;
    private Iterator<GvdbVector> currentPage;
    private InternalRow currentRow;

    GvdbPartitionReader(String target, String apiKey, String collection,
                        int timeoutSeconds, int maxRetries,
                        boolean includeMetadata, StructType schema,
                        long startOffset, long endOffset) {
        var configBuilder = GvdbClientConfig.builder(target)
                .maxRetries(maxRetries)
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .channelCount(1);
        if (apiKey != null) {
            configBuilder.apiKey(apiKey);
        }
        this.client = new GvdbClient(configBuilder.build());
        this.collection = collection;
        this.includeMetadata = includeMetadata;
        this.schema = schema;
        this.currentOffset = startOffset;
        this.endOffset = endOffset;
    }

    @Override
    public boolean next() throws IOException {
        // Try current page first
        if (currentPage != null && currentPage.hasNext()) {
            currentRow = toInternalRow(currentPage.next());
            return true;
        }

        // Fetch next page
        if (currentOffset >= endOffset) return false;

        int limit = (int) Math.min(PAGE_SIZE, endOffset - currentOffset);
        List<GvdbVector> page = client.listVectors(collection, limit, currentOffset, includeMetadata);
        currentOffset += page.size();

        if (page.isEmpty()) return false;

        currentPage = page.iterator();
        currentRow = toInternalRow(currentPage.next());
        return true;
    }

    @Override
    public InternalRow get() {
        return currentRow;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    private InternalRow toInternalRow(GvdbVector vector) {
        var fields = schema.fields();
        Object[] values = new Object[fields.length];

        for (int i = 0; i < fields.length; i++) {
            String name = fields[i].name();
            DataType dt = fields[i].dataType();

            if (name.equals("id")) {
                values[i] = vector.id();
            } else if (name.equals("vector")) {
                values[i] = ArrayData.toArrayData(vector.values());
            } else {
                // Metadata field
                Object metaValue = vector.metadata().get(name);
                if (metaValue == null) {
                    values[i] = null;
                } else {
                    values[i] = convertMetadataValue(metaValue, dt);
                }
            }
        }

        return new GenericInternalRow(values);
    }

    private static Object convertMetadataValue(Object value, DataType targetType) {
        if (targetType instanceof LongType) {
            if (value instanceof Number n) return n.longValue();
        } else if (targetType instanceof DoubleType) {
            if (value instanceof Number n) return n.doubleValue();
        } else if (targetType instanceof StringType) {
            return UTF8String.fromString(value.toString());
        } else if (targetType instanceof BooleanType) {
            if (value instanceof Boolean b) return b;
        }
        return null;
    }
}
