package io.gvdb.flink;

import io.gvdb.client.GvdbClient;
import io.gvdb.client.model.GvdbVector;
import io.gvdb.client.model.UpsertResult;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@DisplayName("GvdbSinkWriter")
class GvdbSinkWriterTest {

    private static final RecordMapper<Integer> MAPPER =
            i -> new GvdbVector(i, new float[]{i.floatValue()});

    private static GvdbSinkWriter<Integer> newWriter(GvdbClient client, int batchSize) {
        return new GvdbSinkWriter<>(client, "col", batchSize, MAPPER);
    }

    @Nested
    @DisplayName("batching")
    class Batching {

        @Test
        @DisplayName("flushes when buffer fills to batchSize")
        void autoFlushAtBatchSize() throws Exception {
            var client = mock(GvdbClient.class);
            when(client.upsert(anyString(), any())).thenReturn(new UpsertResult(2, 2, 0));
            var writer = newWriter(client, 2);

            writer.write(1, null);
            verify(client, never()).upsert(anyString(), any());

            writer.write(2, null);

            @SuppressWarnings("unchecked")
            ArgumentCaptor<List<GvdbVector>> captor = ArgumentCaptor.forClass(List.class);
            verify(client, times(1)).upsert(eq("col"), captor.capture());
            assertEquals(2, captor.getValue().size());
            assertEquals(0, writer.bufferSize());
        }

        @Test
        @DisplayName("flush(endOfInput=false) drains the partial buffer")
        void flushDrainsBuffer() throws Exception {
            var client = mock(GvdbClient.class);
            when(client.upsert(anyString(), any())).thenReturn(new UpsertResult(1, 1, 0));
            var writer = newWriter(client, 100);

            writer.write(42, null);
            writer.flush(false);

            verify(client, times(1)).upsert(eq("col"), any());
            assertEquals(1L, writer.totalWritten());
        }

        @Test
        @DisplayName("flush() on an empty buffer is a no-op")
        void flushOnEmptyBufferSkipsRpc() throws Exception {
            var client = mock(GvdbClient.class);
            var writer = newWriter(client, 10);

            writer.flush(false);

            verify(client, never()).upsert(anyString(), any());
        }
    }

    @Test
    @DisplayName("close() drains the buffer and then closes the client")
    void closeFlushesThenCloses() throws Exception {
        var client = mock(GvdbClient.class);
        when(client.upsert(anyString(), any())).thenReturn(new UpsertResult(1, 1, 0));
        var writer = newWriter(client, 100);
        writer.write(7, null);

        writer.close();

        var inOrder = inOrder(client);
        inOrder.verify(client).upsert(eq("col"), any());
        inOrder.verify(client).close();
    }

    @Test
    @DisplayName("close() still closes the client when the buffer is empty")
    void closeWithEmptyBufferStillClosesClient() throws Exception {
        var client = mock(GvdbClient.class);
        var writer = newWriter(client, 10);

        writer.close();

        verify(client, never()).upsert(anyString(), any());
        verify(client, times(1)).close();
    }

    @Test
    @DisplayName("implements Flink SinkWriter API")
    void implementsSinkWriter() {
        SinkWriter<Integer> w = new GvdbSinkWriter<>(mock(GvdbClient.class), "col", 10, MAPPER);
        assertNotNull(w);
    }
}
