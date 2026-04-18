# Per-vector TTL

Attach a time-to-live to individual vectors. Expired vectors are skipped at query time and physically removed by a background sweep.

## Why

- **Session data**, **chat history**, **cache entries** that should auto-expire
- **Regulatory retention** windows
- **A/B embeddings** that age out after an experiment

## Insert with TTL

```python
import time
from gvdb import GVDBClient

client = GVDBClient("localhost:50051")
client.create_collection("sessions", dimension=384)

# Vectors expire 1 hour from now
now = int(time.time())
ttl_seconds = 3600

client.insert(
    "sessions",
    ids=[1, 2, 3],
    vectors=[[0.1]*384, [0.2]*384, [0.3]*384],
    expire_at=[now + ttl_seconds] * 3,   # absolute unix timestamps
)
```

You can also pass a single `expire_at` to apply to every vector in the batch.

## Query-time filtering

Expired vectors are **atomically excluded** from every search — no stale results, even before the sweep runs:

```python
results = client.search("sessions", query_vector=[...], top_k=10)
# Only non-expired vectors returned
```

## Background sweep

A background thread periodically scans segments and permanently removes expired vectors. Configure the interval in the server YAML:

```yaml
storage:
  ttl_sweep_interval_seconds: 60
```

## Guarantees

- **Atomic insert + TTL**: expiry is part of the same WAL entry as the vector.
- **Serialization-safe**: expired vectors survive flush/reload and are filtered consistently.
- **No double-delete**: the sweep is idempotent.

## Further reading

- [Python SDK — client API](../python-sdk/client.md#insert)
- [Architecture — storage](../architecture/storage.md) for segment lifecycle
