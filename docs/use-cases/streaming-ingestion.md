# Streaming ingestion

Continuously index embeddings as they arrive. GVDB ships first-party connectors for **Apache Spark** and **Apache Flink**.

## When to choose which

| Workload | Choose |
|----------|--------|
| Batch ETL from data lakes (Parquet, Delta, Iceberg) | [Spark](../connectors/spark.md) |
| Real-time streaming (Kafka, Kinesis) with exactly-once | [Flink](../connectors/flink.md) |
| One-shot bulk load from a file | [Python SDK bulk import](../python-sdk/bulk-import.md) |
| gRPC client-streaming from your own producer | Python/Java SDK streaming inserts |

## Spark (batch + streaming)

PySpark example writing a DataFrame to GVDB:

```python
df.write.format("io.gvdb.spark") \
    .option("gvdb.target", "proxy.gvdb.svc:50051") \
    .option("gvdb.collection", "embeddings") \
    .option("gvdb.dimension", "768") \
    .option("gvdb.metric", "cosine") \
    .option("gvdb.index_type", "auto") \
    .option("gvdb.batch_size", "5000") \
    .mode("append") \
    .save()
```

Full walkthrough: [Spark connector](../connectors/spark.md).

## Flink (streaming)

Java sink consuming an embedding stream:

```java
var sink = GvdbSink.<Embedding>builder()
    .setTarget("proxy.gvdb.svc:50051")
    .setCollection("events")
    .setBatchSize(1000)
    .setMaxRetries(3)
    .setRecordMapper(e -> new GvdbVector(
        e.id(), e.vector(), Map.of("category", e.category())))
    .build();

stream.sinkTo(sink);
```

Full walkthrough: [Flink connector](../connectors/flink.md).

## Exactly-once semantics

- **Flink**: the GVDB sink participates in Flink checkpoints. Combined with GVDB's upsert idempotency, you get exactly-once end-to-end.
- **Spark**: batch jobs are idempotent via upsert mode; retried tasks produce the same result.

## Back-pressure and retries

Both connectors:

- Batch inserts (default 1,000–5,000 records)
- Retry on transient errors with exponential backoff (default 3 attempts)
- Fail the task on permanent errors, letting Spark/Flink re-schedule

## Distributed clusters

Point the `target` at your [Helm-deployed proxy](../getting-started/distributed-cluster.md). The proxy fan-outs inserts to the appropriate shard's primary data node; replication is handled server-side.

## See also

- [Spark connector](../connectors/spark.md)
- [Flink connector](../connectors/flink.md)
- [Java client](../connectors/java-client.md) for raw gRPC from JVM apps
