package io.gvdb.client.model;

/**
 * Status of a server-side bulk import job.
 */
public record ImportStatus(
        String importId,
        ImportState state,
        long totalVectors,
        long importedVectors,
        float progressPercent,
        String errorMessage,
        float elapsedSeconds,
        int segmentsCreated
) {
    public enum ImportState {
        PENDING, RUNNING, COMPLETED, FAILED, CANCELLED
    }
}
