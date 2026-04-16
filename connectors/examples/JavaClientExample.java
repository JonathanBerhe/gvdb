/*
 * Standalone Java client — CRUD operations without Spark or Flink.
 *
 * Add to your project:
 *   implementation("io.gvdb:gvdb-java-client:0.14.0")
 */
package io.gvdb.examples;

import io.gvdb.client.GvdbClient;
import io.gvdb.client.GvdbClientConfig;
import io.gvdb.client.model.*;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class JavaClientExample {

    public static void main(String[] args) {
        var config = GvdbClientConfig.builder("localhost:50051")
                // .apiKey("your-api-key")   // uncomment for RBAC
                .batchSize(10_000)
                .build();

        try (var client = new GvdbClient(config)) {
            // Health check
            client.healthCheck();
            System.out.println("Server is healthy");

            // Create collection
            String collection = "java_example";
            client.createCollection(collection, 128, MetricType.COSINE, IndexType.AUTO);
            System.out.println("Created collection: " + collection);

            // Insert vectors
            var random = new Random(42);
            var vectors = new java.util.ArrayList<GvdbVector>();
            for (int i = 0; i < 1000; i++) {
                float[] values = new float[128];
                for (int j = 0; j < 128; j++) values[j] = (float) random.nextGaussian();
                vectors.add(new GvdbVector(i, values, Map.of(
                        "category", i % 2 == 0 ? "A" : "B",
                        "score", random.nextDouble()
                )));
            }

            var result = client.upsert(collection, vectors);
            System.out.printf("Upserted %d vectors (%d inserted, %d updated)%n",
                    result.upsertedCount(), result.insertedCount(), result.updatedCount());

            // List vectors
            var page = client.listVectors(collection, 5, 0, true);
            System.out.println("First 5 vectors:");
            for (var v : page) {
                System.out.printf("  id=%d dim=%d metadata=%s%n", v.id(), v.dimension(), v.metadata());
            }

            // List collections
            var collections = client.listCollections();
            System.out.println("Collections: " + collections);

            // Cleanup
            client.dropCollection(collection);
            System.out.println("Dropped collection: " + collection);
        }
    }
}
