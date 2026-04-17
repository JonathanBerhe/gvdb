package io.gvdb.client.model;

import java.util.Objects;

/**
 * Metadata about a GVDB collection.
 */
public record CollectionInfo(
        String name,
        int id,
        int dimension,
        MetricType metricType,
        long vectorCount
) {
    public CollectionInfo {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(metricType, "metricType");
    }
}
