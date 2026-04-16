package io.gvdb.client.model;

/**
 * Metadata about a GVDB collection.
 */
public record CollectionInfo(
        String name,
        int id,
        int dimension,
        String metricType,
        long vectorCount
) {}
