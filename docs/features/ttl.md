# Per-vector TTL

Attach a time-to-live to individual vectors. Expired vectors are skipped at query time and physically removed by a background sweep.

## Why

- **Session data**, **chat history**, **cache entries** that should auto-expire
- **Regulatory retention** windows
- **A/B embeddings** that age out after an experiment

## Insert with TTL

Pass a per-vector `ttl_seconds` list to `insert`. The TTL is a **relative duration** (seconds from now) — a value of `0` means "no expiration":

```python
from gvdb import GVDBClient

client = GVDBClient("localhost:50051")
client.create_collection("sessions", dimension=384)

client.insert(
    "sessions",
    ids=[1, 2, 3],
    vectors=[[0.1]*384, [0.2]*384, [0.3]*384],
    ttl_seconds=[3600, 3600, 0],   # first two expire in 1 hour; third never
)
```

## Query-time filtering

Expired vectors are **atomically excluded** from every search — no stale results, even before the sweep runs:

```python
results = client.search("sessions", query_vector=[0.1]*384, top_k=10)
# Only non-expired vectors returned
```

## Background sweep

A background thread in each `gvdb-single-node` and `gvdb-data-node` process periodically scans segments and permanently removes expired vectors. The sweep runs on a fixed cadence — see [`src/main/data_node_main.cpp`](https://github.com/JonathanBerhe/gvdb/blob/main/src/main/data_node_main.cpp) for the current interval.

## Guarantees

- **Atomic insert + TTL**: expiry is part of the same WAL entry as the vector.
- **Serialization-safe**: expired vectors survive flush/reload and are filtered consistently.
- **No double-delete**: the sweep is idempotent.

## See also

- [Client API — insert](../python-sdk/client.md#insert)
- [Architecture — storage](../architecture/storage.md) for segment lifecycle
