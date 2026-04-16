package io.gvdb.spark.write;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.io.Serializable;

/**
 * Commit message returned by each DataWriter partition on success.
 */
public record GvdbWriterCommitMessage(
        int partitionId,
        long taskId,
        long vectorsWritten
) implements WriterCommitMessage, Serializable {}
