package io.gvdb.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("VectorBatcher")
class VectorBatcherTest {

    @Nested
    @DisplayName("partition(List, batchSize)")
    class Partition {

        @Test
        @DisplayName("returns empty list for empty input")
        void emptyList() {
            var result = VectorBatcher.partition(List.of(), 10);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("wraps a single element in one batch")
        void singleElement() {
            var result = VectorBatcher.partition(List.of("a"), 10);
            assertEquals(1, result.size());
            assertEquals(List.of("a"), result.get(0));
        }

        @Test
        @DisplayName("splits evenly when size is a multiple of batchSize")
        void exactBatchBoundary() {
            var items = List.of(1, 2, 3, 4, 5, 6);
            var result = VectorBatcher.partition(items, 3);
            assertEquals(2, result.size());
            assertEquals(List.of(1, 2, 3), result.get(0));
            assertEquals(List.of(4, 5, 6), result.get(1));
        }

        @Test
        @DisplayName("leaves a smaller trailing batch when size is not divisible")
        void nonDivisible() {
            var items = List.of(1, 2, 3, 4, 5);
            var result = VectorBatcher.partition(items, 3);
            assertEquals(2, result.size());
            assertEquals(List.of(1, 2, 3), result.get(0));
            assertEquals(List.of(4, 5), result.get(1));
        }

        @Test
        @DisplayName("returns a single batch when batchSize exceeds input size")
        void batchSizeLargerThanList() {
            var items = List.of(1, 2, 3);
            var result = VectorBatcher.partition(items, 100);
            assertEquals(1, result.size());
            assertEquals(List.of(1, 2, 3), result.get(0));
        }

        @Test
        @DisplayName("yields one element per batch when batchSize is 1")
        void batchSizeOne() {
            var items = List.of("a", "b", "c");
            var result = VectorBatcher.partition(items, 1);
            assertEquals(3, result.size());
        }

        @Test
        @DisplayName("rejects non-positive batchSize")
        void invalidBatchSize() {
            assertThrows(IllegalArgumentException.class, () -> VectorBatcher.partition(List.of(1), 0));
            assertThrows(IllegalArgumentException.class, () -> VectorBatcher.partition(List.of(1), -1));
        }
    }

    @Nested
    @DisplayName("partitionLazy(Iterator, batchSize)")
    class PartitionLazy {

        @Test
        @DisplayName("produces batches in order with a smaller tail")
        void lazyPartitionBasic() {
            var items = List.of(1, 2, 3, 4, 5).iterator();
            Iterator<List<Integer>> batches = VectorBatcher.partitionLazy(items, 2);

            assertTrue(batches.hasNext());
            assertEquals(List.of(1, 2), batches.next());
            assertTrue(batches.hasNext());
            assertEquals(List.of(3, 4), batches.next());
            assertTrue(batches.hasNext());
            assertEquals(List.of(5), batches.next());
            assertFalse(batches.hasNext());
        }

        @Test
        @DisplayName("exposes no batches for empty input")
        void lazyPartitionEmpty() {
            Iterator<List<String>> batches = VectorBatcher.partitionLazy(List.<String>of().iterator(), 10);
            assertFalse(batches.hasNext());
        }

        @Test
        @DisplayName("consumes only as many source elements as needed")
        void lazyPartitionIsLazy() {
            var consumed = new ArrayList<Integer>();
            var source = new Iterator<Integer>() {
                int i = 0;
                @Override
                public boolean hasNext() { return i < 100; }
                @Override
                public Integer next() { consumed.add(i); return i++; }
            };

            var batches = VectorBatcher.partitionLazy(source, 10);
            batches.next();

            assertEquals(10, consumed.size());
        }
    }
}
