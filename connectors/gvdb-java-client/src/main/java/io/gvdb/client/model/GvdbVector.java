package io.gvdb.client.model;

import java.util.Arrays;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GvdbVector other)) return false;
        return id == other.id
                && Arrays.equals(values, other.values)
                && Objects.equals(metadata, other.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, Arrays.hashCode(values), metadata);
    }

    @Override
    public String toString() {
        return "GvdbVector[id=" + id
                + ", values=" + Arrays.toString(values)
                + ", metadata=" + metadata + "]";
    }
}
