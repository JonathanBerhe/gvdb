# Tiered storage (S3 / MinIO)

Offload sealed segments to object storage automatically. Hot data stays on local disk; cold segments live in S3 or MinIO with on-demand download.

## Why

- **Unbounded capacity**: store far more vectors than will fit on local disks
- **Lower cost**: object storage is ~10× cheaper per GB than SSD
- **Durability**: S3's 11-nines durability backs your most critical data

## Architecture

```
┌─────────────┐  async upload  ┌─────────────┐
│ Data node   │ ─────────────▶ │ S3 / MinIO  │
│             │                │ (cold tier) │
│ Local disk  │ ◀───────────── │             │
│ (hot cache) │   lazy download└─────────────┘
└─────────────┘
```

- `TieredSegmentManager` composes the local `SegmentManager` + `IObjectStore` + an LRU `SegmentCache`.
- **Sealed** segments upload asynchronously after local flush.
- **Reads** hit local disk first; on miss, the segment downloads to the cache.
- A **manifest** in the bucket lists every segment for fast startup discovery (no `ListObjects` scan).

## Enable at build time

S3 support is behind a CMake flag:

```bash
make build CMAKE_EXTRA="-DGVDB_WITH_S3=ON"
```

Runtime deps: `libssl-dev`, `libcurl4-openssl-dev`. The Docker image includes them.

## Configuration

```yaml
storage:
  data_dir: /var/lib/gvdb

  object_store:
    enabled: true
    endpoint: https://s3.amazonaws.com   # or http://minio:9000 for MinIO
    region: us-east-1
    bucket: gvdb-cold
    prefix: segments/
    access_key_id: ${AWS_ACCESS_KEY_ID}
    secret_access_key: ${AWS_SECRET_ACCESS_KEY}
    upload_threads: 4
    cache_size_gb: 50
```

## MinIO locally

For testing, run MinIO via Docker Compose:

```bash
docker compose -f test/integration/docker-compose.minio.yml up -d
```

Then point `endpoint` at `http://localhost:9000`.

Run the S3 integration tests:

```bash
make test-s3
```

## Cache behaviour

The local cache is an **LRU** with a configurable size. On miss, GVDB blocks until the segment is downloaded, then serves reads from the cached copy.

Evictions are background; in-flight queries are never interrupted.

## Further reading

- [Architecture — storage](../architecture/storage.md) for segment lifecycle
- [Deploy with Helm](../operations/deploy-helm.md) for configuring S3 via Helm values
