package io.gvdb.client.model;

import java.util.Map;
import java.util.Objects;

/**
 * A vector with its ID and optional metadata.
 *
 * @param id       unique vector identifier
 * @param values   dense float vector
 * @param metadata key-value metadata (values must be Long, Double, String, or Boolean)
 */
public record GvdbVector(long id, float[] values, Map<String, Object> metadata) {

    public GvdbVector {
        Objects.requireNonNull(values, "values must not be null");
    }

    public GvdbVector(long id, float[] values) {
        this(id, values, Map.of());
    }

    public int dimension() {
        return values.length;
    }
}
