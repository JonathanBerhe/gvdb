package io.gvdb.client.model;

/**
 * Result of an upsert operation.
 */
public record UpsertResult(long upsertedCount, long insertedCount, long updatedCount) {}
