package io.gvdb.spark.write;

import io.gvdb.client.GvdbClient;
import io.gvdb.client.model.GvdbVector;
import io.gvdb.client.model.UpsertResult;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@DisplayName("GvdbDataWriter")
class GvdbDataWriterTest {

    private static final StructType SCHEMA = new StructType()
            .add("id", DataTypes.LongType)
            .add("vector", new ArrayType(DataTypes.FloatType, false));

    private static InternalRow row(long id, float[] values) {
        ArrayData vec = new GenericArrayData(new Object[]{values[0], values[1]});
        return new GenericInternalRow(new Object[]{id, vec});
    }

    private GvdbDataWriter newWriter(GvdbClient client, int batchSize, WriteMode mode) {
        return new GvdbDataWriter(client, "col", batchSize, mode, SCHEMA, 0, 1, 0, 0L);
    }

    @Nested
    @DisplayName("UPSERT mode")
    class UpsertMode {

        @Test
        @DisplayName("flushes a batch once the buffer hits batchSize")
        void flushesAtBatchSize() throws Exception {
            var client = mock(GvdbClient.class);
            var flushedSize = new AtomicInteger();
            when(client.upsert(anyString(), any())).thenAnswer(inv -> {
                List<GvdbVector> arg = inv.getArgument(1);
                flushedSize.set(arg.size());
                return new UpsertResult(arg.size(), arg.size(), 0);
            });
            var writer = newWriter(client, 2, WriteMode.UPSERT);

            writer.write(row(1L, new float[]{1f, 2f}));
            verify(client, never()).upsert(anyString(), any());

            writer.write(row(2L, new float[]{3f, 4f}));

            verify(client, times(1)).upsert(eq("col"), any());
            assertEquals(2, flushedSize.get());
        }

        @Test
        @DisplayName("commit() flushes remaining buffered records and closes the client")
        void commitFlushesPartialBatchAndCloses() throws Exception {
            var client = mock(GvdbClient.class);
            when(client.upsert(anyString(), any())).thenReturn(new UpsertResult(1, 1, 0));
            var writer = newWriter(client, 10, WriteMode.UPSERT);

            writer.write(row(1L, new float[]{1f, 2f}));
            var message = writer.commit();

            verify(client, times(1)).upsert(eq("col"), any());
            verify(client, times(1)).close();
            assertInstanceOf(GvdbWriterCommitMessage.class, message);
            assertEquals(1, ((GvdbWriterCommitMessage) message).vectorsWritten());
        }

        @Test
        @DisplayName("commit() with an empty buffer does not call upsert")
        void commitWithoutWritesSkipsFlush() throws Exception {
            var client = mock(GvdbClient.class);
            var writer = newWriter(client, 10, WriteMode.UPSERT);

            writer.commit();

            verify(client, never()).upsert(anyString(), any());
            verify(client, times(1)).close();
        }
    }

    @Nested
    @DisplayName("STREAM_INSERT mode")
    class StreamInsertMode {

        @Test
        @DisplayName("routes flush through streamInsert instead of upsert")
        void usesStreamInsert() throws Exception {
            var client = mock(GvdbClient.class);
            when(client.streamInsert(anyString(), any(), anyInt())).thenReturn(1L);
            var writer = newWriter(client, 10, WriteMode.STREAM_INSERT);

            writer.write(row(1L, new float[]{1f, 2f}));
            writer.commit();

            verify(client, times(1)).streamInsert(eq("col"), any(Iterator.class), eq(10));
            verify(client, never()).upsert(anyString(), any());
        }
    }

    @Test
    @DisplayName("abort() closes the client without flushing the buffer")
    void abortDoesNotFlush() throws Exception {
        var client = mock(GvdbClient.class);
        var writer = newWriter(client, 10, WriteMode.UPSERT);
        writer.write(row(1L, new float[]{1f, 2f}));

        writer.abort();

        verify(client, never()).upsert(anyString(), any());
        verify(client, times(1)).close();
    }

    @Test
    @DisplayName("close() only closes the underlying client once across repeated calls")
    void closeIsIdempotent() throws Exception {
        var client = mock(GvdbClient.class);
        var writer = newWriter(client, 10, WriteMode.UPSERT);

        writer.close();
        writer.close();

        verify(client, times(1)).close();
    }
}
