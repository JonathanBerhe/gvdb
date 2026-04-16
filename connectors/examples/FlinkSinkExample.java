/*
 * Flink — Sink vectors to GVDB from a streaming source.
 *
 * Build and run:
 *   flink run -c io.gvdb.examples.FlinkSinkExample \
 *       -C /path/to/gvdb-flink-connector-0.14.0-all.jar \
 *       /path/to/your-job.jar
 */
package io.gvdb.examples;

import io.gvdb.client.model.GvdbVector;
import io.gvdb.flink.GvdbSink;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;
import java.util.Random;

public class FlinkSinkExample {

    // Simple record representing an ML embedding
    public record Embedding(long id, float[] vector, String category) {}

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // checkpoint every 5 seconds

        // --- Generate sample embeddings ---
        int dimension = 128;
        int count = 5000;
        var random = new Random(42);

        var embeddings = new Embedding[count];
        for (int i = 0; i < count; i++) {
            float[] vec = new float[dimension];
            for (int j = 0; j < dimension; j++) vec[j] = (float) random.nextGaussian();
            embeddings[i] = new Embedding(i, vec, i % 2 == 0 ? "electronics" : "clothing");
        }

        var stream = env.fromCollection(
                java.util.Arrays.asList(embeddings),
                TypeInformation.of(Embedding.class)
        );

        // --- Sink to GVDB ---
        var sink = GvdbSink.<Embedding>builder()
                .setTarget("localhost:50051")
                .setCollection("flink_embeddings")
                .setBatchSize(1000)
                .setMaxRetries(3)
                .setRecordMapper(e -> new GvdbVector(
                        e.id(),
                        e.vector(),
                        Map.of("category", e.category())
                ))
                .build();

        stream.sinkTo(sink);

        env.execute("gvdb-flink-sink-example");
        System.out.println("Wrote " + count + " vectors to GVDB collection 'flink_embeddings'");
    }
}
