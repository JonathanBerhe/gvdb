# Spark connector

First-party Apache Spark DataSource v2 connector. Read and write GVDB collections from Spark (Scala, Java, or PySpark) for batch and streaming workloads.

## Coordinates

=== "Gradle"

    ```kotlin
    dependencies {
        implementation("io.gvdb:gvdb-spark-connector_2.13:0.19.0") // x-release-please-version
    }
    ```

=== "Maven"

    ```xml
    <dependency>
      <groupId>io.gvdb</groupId>
      <artifactId>gvdb-spark-connector_2.13</artifactId>
      <version>0.19.0</version> <!-- x-release-please-version -->
    </dependency>
    ```

=== "spark-submit"

    ```bash
    spark-submit --packages io.gvdb:gvdb-spark-connector_2.13:0.19.0 my_job.py # x-release-please-version
    ```

Built for **Spark 3.5+** on **Scala 2.13**. JVM 17.

## Write

```python title="spark_write.py"
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, LongType, FloatType, ArrayType, StringType
)
import random

spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("gvdb-spark-write-example")
        .getOrCreate()
)

DIMENSION = 128
NUM_VECTORS = 10_000

data = [
    (i, [random.gauss(0, 1) for _ in range(DIMENSION)], f"item_{i}", random.random())
    for i in range(NUM_VECTORS)
]

schema = StructType([
    StructField("id", LongType(), False),
    StructField("vector", ArrayType(FloatType()), False),
    StructField("name", StringType(), True),
    StructField("score", FloatType(), True),
])

df = spark.createDataFrame(data, schema)

df.write.format("io.gvdb.spark") \
    .option("gvdb.target", "localhost:50051") \
    .option("gvdb.collection", "spark_embeddings") \
    .option("gvdb.dimension", str(DIMENSION)) \
    .option("gvdb.metric", "cosine") \
    .option("gvdb.index_type", "auto") \
    .option("gvdb.batch_size", "5000") \
    .mode("append") \
    .save()
```

## Read

```python
df_read = spark.read.format("io.gvdb.spark") \
    .option("gvdb.target", "localhost:50051") \
    .option("gvdb.collection", "spark_embeddings") \
    .option("gvdb.include_metadata", "true") \
    .load()

df_read.show(5, truncate=True)
```

## Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `gvdb.target` | yes | — | `host:port` of the proxy or single-node server |
| `gvdb.collection` | yes | — | Collection name |
| `gvdb.dimension` | writes only | — | Vector dimension (on auto-create) |
| `gvdb.metric` | no | `cosine` | `l2`, `ip`, or `cosine` |
| `gvdb.index_type` | no | `auto` | Any supported index type |
| `gvdb.batch_size` | no | `1000` | Rows per insert RPC |
| `gvdb.api_key` | no | — | API key for RBAC |
| `gvdb.tls` | no | `false` | Enable TLS |
| `gvdb.include_metadata` | reads only | `false` | Include metadata columns |

## Schema mapping

- `id` → GVDB vector ID (`LongType`)
- `vector` → dense vector (`ArrayType(FloatType)`)
- All other columns → per-vector metadata (JSON-serialized)

Custom column names can be configured via `gvdb.id_column` / `gvdb.vector_column`.

## Write modes

- **`append`** — inserts new vectors; existing IDs will error.
- **`overwrite`** — drops and recreates the collection, then writes.
- **`upsert`** (via `.option("gvdb.write_mode", "upsert")`) — idempotent, safe to re-run.

## Streaming

Structured Streaming writes are supported:

```python
stream = spark.readStream...
stream.writeStream.format("io.gvdb.spark") \
    .option("gvdb.target", "localhost:50051") \
    .option("gvdb.collection", "stream_embeddings") \
    .option("checkpointLocation", "/tmp/gvdb-checkpoint") \
    .start()
```

## Source

- [`connectors/gvdb-spark-connector/`](https://github.com/JonathanBerhe/gvdb/tree/main/connectors/gvdb-spark-connector)
- [`connectors/examples/spark_write.py`](https://github.com/JonathanBerhe/gvdb/blob/main/connectors/examples/spark_write.py)

## See also

- [Flink connector](flink.md) for real-time streaming
- [Java client](java-client.md) for direct gRPC usage
- [Streaming ingestion use case](../use-cases/streaming-ingestion.md)
