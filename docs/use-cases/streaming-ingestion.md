# Streaming ingestion

Index embeddings continuously as your producer creates them, for example from a Kafka consumer. The [Python SDK](../python-sdk/index.md) has two calls for this.

## When to choose which

| Workload | Choose |
|----------|--------|
| Micro-batches from a consumer that can retry | `upsert` |
| Large batches at the highest rate, without retries | `stream_insert` |
| One-time load from a file | [Python SDK bulk import](../python-sdk/bulk-import.md) |

## Upsert micro-batches

`upsert` inserts new ids and replaces existing ones. A retried batch gives the same result, so a consumer can commit its offset after the call returns and retry the batch on an error.

```python
from gvdb import GVDBClient

with GVDBClient("proxy.gvdb.svc:50051") as client:
    for batch in consumer:  # your producer, for example a Kafka consumer
        client.upsert(
            "events",
            ids=[e.id for e in batch],
            vectors=[e.vector for e in batch],
            metadata=[{"category": e.category} for e in batch],
        )
        consumer.commit()
```

## Stream insert

`stream_insert` splits the ids and vectors into pieces of `batch_size` and sends them over one gRPC client-streaming call. It returns the number of inserted vectors. `upsert` is the call that replaces an existing id, so use `upsert` when the producer can send a record again.

```python
total = client.stream_insert(
    "events",
    ids=ids,
    vectors=vectors,
    batch_size=10_000,
    metadata=metadata,
)
```

## Distributed clusters

Point the client at your [Helm-deployed proxy](../getting-started/distributed-cluster.md). The proxy sends each insert to the primary data node of its shard. The server handles replication.

## See also

- [Python SDK client API](../python-sdk/client.md)
- [Bulk ingestion](bulk-ingestion.md)
