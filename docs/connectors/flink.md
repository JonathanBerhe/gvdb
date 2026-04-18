# Flink connector

First-party Apache Flink sink connector. Write streaming embeddings to GVDB with exactly-once semantics via Flink checkpoints + GVDB upsert idempotency.

## Coordinates

=== "Gradle"

    ```kotlin
    dependencies {
        implementation("io.gvdb:gvdb-flink-connector:0.16.0")
    }
    ```

=== "Maven"

    ```xml
    <dependency>
      <groupId>io.gvdb</groupId>
      <artifactId>gvdb-flink-connector</artifactId>
      <version>0.16.0</version>
    </dependency>
    ```

=== "flink run"

    ```bash
    flink run -c io.gvdb.examples.FlinkSinkExample \
        -C /path/to/gvdb-flink-connector-0.16.0-all.jar \
        /path/to/your-job.jar
    ```

Built for **Flink 1.18+** on **JVM 17**.

## Example

```java title="FlinkSinkExample.java"
package io.gvdb.examples;

import io.gvdb.client.model.GvdbVector;
import io.gvdb.flink.GvdbSink;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;
import java.util.Random;

public class FlinkSinkExample {

    public record Embedding(long id, float[] vector, String category) {}

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // checkpoint every 5 seconds

        int dimension = 128;
        int count = 5000;
        var random = new Random(42);

        var embeddings = new Embedding[count];
        for (int i = 0; i < count; i++) {
            float[] vec = new float[dimension];
            for (int j = 0; j < dimension; j++) vec[j] = (float) random.nextGaussian();
            embeddings[i] = new Embedding(i, vec,
                i % 2 == 0 ? "electronics" : "clothing");
        }

        var stream = env.fromCollection(
                java.util.Arrays.asList(embeddings),
                TypeInformation.of(Embedding.class)
        );

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
    }
}
```

## Builder options

| Method | Default | Description |
|--------|---------|-------------|
| `setTarget(host:port)` | required | GVDB proxy or single-node endpoint |
| `setCollection(name)` | required | Target collection |
| `setApiKey(key)` | — | API key for RBAC |
| `setTls(bool)` | `false` | Enable TLS |
| `setBatchSize(int)` | `1000` | Records per insert RPC |
| `setMaxRetries(int)` | `3` | Retries on transient errors |
| `setRecordMapper(fn)` | required | Maps your record type → `GvdbVector` |
| `setWriteMode(mode)` | `UPSERT` | `INSERT` or `UPSERT` |

## Exactly-once

Combine Flink checkpointing with GVDB's upsert idempotency:

```java
env.enableCheckpointing(5000);
// Sink uses default UPSERT mode — retried records produce the same result
```

## Back-pressure

The sink blocks on `invoke()` when in-flight batches exceed the configured limit. Flink's back-pressure mechanism propagates this upstream.

## Source

- [`connectors/gvdb-flink-connector/`](https://github.com/JonathanBerhe/gvdb/tree/main/connectors/gvdb-flink-connector)
- [`connectors/examples/FlinkSinkExample.java`](https://github.com/JonathanBerhe/gvdb/blob/main/connectors/examples/FlinkSinkExample.java)

## See also

- [Spark connector](spark.md) for batch workloads
- [Java client](java-client.md) for direct gRPC
- [Streaming ingestion use case](../use-cases/streaming-ingestion.md)
