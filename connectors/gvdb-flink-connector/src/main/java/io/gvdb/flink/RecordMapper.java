package io.gvdb.flink;

import io.gvdb.client.model.GvdbVector;

import java.io.Serializable;

/**
 * Maps a stream element to a {@link GvdbVector} for writing to GVDB.
 *
 * @param <T> the input element type
 */
@FunctionalInterface
public interface RecordMapper<T> extends Serializable {

    /**
     * Convert a stream element to a GVDB vector.
     *
     * @param element the input element
     * @return the vector to upsert
     */
    GvdbVector map(T element);
}
