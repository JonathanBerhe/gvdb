package io.gvdb.client;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class VectorBatcherTest {

    @Test
    void emptyList() {
        var result = VectorBatcher.partition(List.of(), 10);
        assertTrue(result.isEmpty());
    }

    @Test
    void singleElement() {
        var result = VectorBatcher.partition(List.of("a"), 10);
        assertEquals(1, result.size());
        assertEquals(List.of("a"), result.get(0));
    }

    @Test
    void exactBatchBoundary() {
        var items = List.of(1, 2, 3, 4, 5, 6);
        var result = VectorBatcher.partition(items, 3);
        assertEquals(2, result.size());
        assertEquals(List.of(1, 2, 3), result.get(0));
        assertEquals(List.of(4, 5, 6), result.get(1));
    }

    @Test
    void nonDivisible() {
        var items = List.of(1, 2, 3, 4, 5);
        var result = VectorBatcher.partition(items, 3);
        assertEquals(2, result.size());
        assertEquals(List.of(1, 2, 3), result.get(0));
        assertEquals(List.of(4, 5), result.get(1));
    }

    @Test
    void batchSizeLargerThanList() {
        var items = List.of(1, 2, 3);
        var result = VectorBatcher.partition(items, 100);
        assertEquals(1, result.size());
        assertEquals(List.of(1, 2, 3), result.get(0));
    }

    @Test
    void batchSizeOne() {
        var items = List.of("a", "b", "c");
        var result = VectorBatcher.partition(items, 1);
        assertEquals(3, result.size());
    }

    @Test
    void invalidBatchSize() {
        assertThrows(IllegalArgumentException.class, () -> VectorBatcher.partition(List.of(1), 0));
        assertThrows(IllegalArgumentException.class, () -> VectorBatcher.partition(List.of(1), -1));
    }

    @Test
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
    void lazyPartitionEmpty() {
        Iterator<List<String>> batches = VectorBatcher.partitionLazy(List.<String>of().iterator(), 10);
        assertFalse(batches.hasNext());
    }

    @Test
    void lazyPartitionIsLazy() {
        // Verify the iterator doesn't consume all elements eagerly
        var consumed = new ArrayList<Integer>();
        var source = new Iterator<Integer>() {
            int i = 0;
            @Override
            public boolean hasNext() { return i < 100; }
            @Override
            public Integer next() { consumed.add(i); return i++; }
        };

        var batches = VectorBatcher.partitionLazy(source, 10);
        batches.next(); // consume first batch only

        assertEquals(10, consumed.size()); // only 10 elements consumed, not 100
    }
}
