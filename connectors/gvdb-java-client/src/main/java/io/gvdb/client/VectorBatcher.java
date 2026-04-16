package io.gvdb.client;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Utility for partitioning vectors into batches.
 */
public final class VectorBatcher {

    private VectorBatcher() {}

    /**
     * Partition a list into fixed-size sublists. The last batch may be smaller.
     */
    public static <T> List<List<T>> partition(List<T> items, int batchSize) {
        if (batchSize < 1) throw new IllegalArgumentException("batchSize must be >= 1");
        if (items.isEmpty()) return List.of();

        var result = new ArrayList<List<T>>((items.size() + batchSize - 1) / batchSize);
        for (int start = 0; start < items.size(); start += batchSize) {
            int end = Math.min(start + batchSize, items.size());
            result.add(items.subList(start, end));
        }
        return result;
    }

    /**
     * Lazily partition an iterator into batches.
     */
    public static <T> Iterator<List<T>> partitionLazy(Iterator<T> items, int batchSize) {
        if (batchSize < 1) throw new IllegalArgumentException("batchSize must be >= 1");

        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return items.hasNext();
            }

            @Override
            public List<T> next() {
                if (!items.hasNext()) throw new NoSuchElementException();
                var batch = new ArrayList<T>(batchSize);
                while (items.hasNext() && batch.size() < batchSize) {
                    batch.add(items.next());
                }
                return batch;
            }
        };
    }
}
