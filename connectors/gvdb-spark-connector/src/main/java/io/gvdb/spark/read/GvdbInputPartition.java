package io.gvdb.spark.read;

import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;

/**
 * Represents a range of vectors to read from a GVDB collection.
 * Serialized to Spark executors for parallel reading.
 */
public record GvdbInputPartition(
        int partitionIndex,
        long startOffset,
        long endOffset
) implements InputPartition, Serializable {}
