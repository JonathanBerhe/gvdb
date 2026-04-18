# Java client

Standalone Java/Kotlin client — CRUD, search, and hybrid search without Spark or Flink. Use it when you're writing a JVM service that talks directly to GVDB.

## Coordinates

=== "Gradle"

    ```kotlin
    dependencies {
        implementation("io.gvdb:gvdb-java-client:0.16.0")
    }
    ```

=== "Maven"

    ```xml
    <dependency>
      <groupId>io.gvdb</groupId>
      <artifactId>gvdb-java-client</artifactId>
      <version>0.16.0</version>
    </dependency>
    ```

Requires **JVM 17+**.

## Example

```java title="JavaClientExample.java"
package io.gvdb.examples;

import io.gvdb.client.GvdbClient;
import io.gvdb.client.GvdbClientConfig;
import io.gvdb.client.model.*;

import java.util.Map;
import java.util.Random;

public class JavaClientExample {
    public static void main(String[] args) {
        var config = GvdbClientConfig.builder("localhost:50051")
                // .apiKey("your-api-key")
                .batchSize(10_000)
                .build();

        try (var client = new GvdbClient(config)) {
            client.healthCheck();

            String collection = "java_example";
            client.createCollection(collection, 128, MetricType.COSINE, IndexType.AUTO);

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
            System.out.printf("Upserted %d (inserted=%d, updated=%d)%n",
                    result.upsertedCount(), result.insertedCount(), result.updatedCount());

            var page = client.listVectors(collection, 5, 0, true);
            for (var v : page) {
                System.out.printf("id=%d dim=%d metadata=%s%n",
                        v.id(), v.dimension(), v.metadata());
            }

            client.dropCollection(collection);
        }
    }
}
```

## Config builder

```java
var config = GvdbClientConfig.builder("localhost:50051")
        .apiKey("your-key")         // RBAC
        .tls(true)                  // enable TLS
        .timeoutSeconds(30)
        .batchSize(10_000)
        .build();
```

## Core methods

- **Collections**: `createCollection`, `dropCollection`, `listCollections`, `describeCollection`
- **Writes**: `insert`, `upsert`, `delete`, `updateMetadata`
- **Reads**: `get`, `listVectors`, `search`, `rangeSearch`, `hybridSearch`
- **Health**: `healthCheck`, `getStats`

See the JavaDoc on GitHub Packages for the full signature.

## Source

- [`connectors/gvdb-java-client/`](https://github.com/JonathanBerhe/gvdb/tree/main/connectors/gvdb-java-client)
- [`connectors/examples/JavaClientExample.java`](https://github.com/JonathanBerhe/gvdb/blob/main/connectors/examples/JavaClientExample.java)

## See also

- [Spark connector](spark.md) for batch DataFrames
- [Flink connector](flink.md) for streaming
- [Python SDK](../python-sdk/index.md) — same operations from Python
