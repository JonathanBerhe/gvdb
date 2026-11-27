# Tiered storage

Offload sealed segments to an object store automatically. Hot data stays on local disk; cold segments live in the object store with on-demand download.

## Backends

| Backend | Build flag | When to use |
|---------|-----------|-------------|
| **S3** | `-DGVDB_WITH_S3=ON` | AWS S3, production |
| **MinIO** | `-DGVDB_WITH_S3=ON` | Self-hosted S3-compatible storage |
| **GCS** | `-DGVDB_WITH_GCS=ON` | Google Cloud Storage, GKE with Workload Identity |
| **Filesystem** | always on | Dev, CI, single-node, NFS-mounted cold tier |

All backends implement the same `IObjectStore` interface and are exercised by a shared contract test suite, so the same tiered-storage behavior holds regardless of which one you pick.

## Why

- **Unbounded capacity**: store far more vectors than will fit on local disks
- **Lower cost**: object storage is ~10× cheaper per GB than SSD
- **Durability**: S3's 11-nines durability backs your most critical data; the Filesystem backend uses `fsync` on both the temp file and the parent directory so writes survive power loss

## Architecture

```mermaid
graph LR
    subgraph DN[gvdb-data-node]
        direction TB
        Local[(Local disk<br/>hot cache)]
    end

    Object[(S3 / MinIO<br/>cold tier)]

    Local -- "async upload<br/>on seal" --> Object
    Object -- "lazy download<br/>on read miss" --> Local

    style Local fill:#51cf66,color:#fff
    style Object fill:#4a9eff,color:#fff
```

- `TieredSegmentManager` composes the local `SegmentManager` + `IObjectStore` + an LRU `SegmentCache`.
- **Sealed** segments upload asynchronously after local flush.
- **Reads** hit local disk first; on miss, the segment downloads to the cache.
- A **manifest** in the bucket lists every segment for fast startup discovery (no `ListObjects` scan).

## Enable at build time

Cloud backends are behind CMake flags:

```bash
# S3 / MinIO: the AWS SDK is fetched from source
make build CMAKE_EXTRA="-DGVDB_WITH_S3=ON"

# GCS: google-cloud-cpp (storage) must be provided via find_package
# (vcpkg feature "storage", a system package, or a source install)
make build CMAKE_EXTRA="-DGVDB_WITH_GCS=ON"
```

S3 runtime deps: `libssl-dev`, `libcurl4-openssl-dev`. Both flags can be enabled together. The Filesystem backend is always available and needs no flag.

## Server configuration

Object store settings live in a nested `object_store` block under `storage` in the server YAML. Omitting the block (or leaving `type` empty with no endpoint) disables tiered storage.

=== "S3 / MinIO"

    ```yaml
    storage:
      data_dir: "/var/lib/gvdb"

      object_store:
        type: "s3"                        # "s3" or "minio"
        endpoint: "https://s3.amazonaws.com"
        region: "us-east-1"
        bucket: "gvdb-cold"
        prefix: "segments"
        access_key: "..."
        secret_key: "..."
        use_ssl: true
        local_cache_size_mb: 50000        # 50 GB
        upload_threads: 4
    ```

=== "GCS"

    ```yaml
    storage:
      data_dir: "/var/lib/gvdb"

      object_store:
        type: "gcs"
        bucket: "gvdb-cold"
        prefix: "segments"
        # Auth is Application Default Credentials. On GKE, bind a Google
        # service account via Workload Identity and leave these empty. For
        # local/dev, set credentials_path (or GOOGLE_APPLICATION_CREDENTIALS)
        # to a service-account JSON.
        project: ""                       # optional; ADC usually supplies it
        credentials_path: ""              # optional service-account JSON
        local_cache_size_mb: 50000
        upload_threads: 4
    ```

    No access/secret keys and no region: GCS auth is identity-based (Workload
    Identity on GKE) and a bucket's location is a property of the bucket. Set
    `endpoint` only to target a local fake-gcs-server emulator.

=== "Filesystem"

    ```yaml
    storage:
      data_dir: "/var/lib/gvdb"

      object_store:
        type: "filesystem"
        bucket: "/mnt/gvdb-cold"          # absolute root directory
        prefix: "segments"
        local_cache_size_mb: 50000
        upload_threads: 4
    ```

    The `bucket` field is interpreted as the root directory. The backend reserves a `.gvdb-tmp/` subdirectory for atomic writes; do **not** put application data there. Use this for dev, CI, single-node deployments, and NFS-mounted cold tiers where standing up MinIO would be overkill.

## Enable via Helm

The chart renders the `storage.object_store` block from the `objectStore` values section and starts the config-consuming workloads with `--config`:

```yaml
objectStore:
  enabled: true
  type: s3                 # "s3" | "minio" | "gcs"
  endpoint: http://minio.storage.svc.cluster.local:9000
  bucket: gvdb-segments
  region: us-east-1
  prefix: gvdb
  useSsl: false            # plain-HTTP MinIO
  cacheSizeMb: 256
  uploadThreads: 2
```

!!! warning "Static credentials are for dev only"
    `objectStore.accessKey` / `objectStore.secretKey` render into the ConfigMap the pods read. Use them only for dev/MinIO. In production leave both empty and grant bucket access via IRSA (EKS) or Workload Identity (GKE), or mount your own config Secret.

## S3 endpoint locally

For testing, run an S3-compatible endpoint (adobe/s3mock, with the bucket `gvdb-test`) through Docker Compose. MinIO no longer publishes public container images.

```bash
docker compose -f test/integration/docker-compose.s3.yml up -d --wait
```

Then point the server at it:

```yaml
storage:
  object_store:
    type: "minio"
    endpoint: "http://localhost:9000"
    bucket: "gvdb-test"
    access_key: "test"
    secret_key: "test"
    use_ssl: false
```

The endpoint accepts any access key and secret key. To run the S3 tests, build with S3 first, then run them against the endpoint:

```bash
make build CMAKE_EXTRA="-DGVDB_WITH_S3=ON"
make test-s3
```

## fake-gcs-server locally

For GCS testing without a real bucket, run the fake-gcs-server emulator:

```bash
docker compose -f test/integration/docker-compose.fake-gcs.yml up -d
```

Then point the server at it:

```yaml
storage:
  object_store:
    type: "gcs"
    endpoint: "http://localhost:4443"
    bucket: "gvdb-test"
```

To run the object store tests against the emulator, build the `gcs` preset first. It needs vcpkg with `VCPKG_ROOT` set, because google-cloud-cpp comes from vcpkg.

```bash
cmake --preset gcs
cmake --build --preset gcs --target storage_tests object_store_gcs_tests
GVDB_TEST_GCS_ENDPOINT=http://localhost:4443 ctest --preset gcs \
  -R '^object_store_gcs_tests$|^storage_tests:(ObjectStore contract|InMemoryObjectStore|FilesystemObjectStore|CreateObjectStore)'
```

## Cache behaviour

The local cache is an **LRU** with a configurable size. On miss, GVDB blocks until the segment is downloaded, then serves reads from the cached copy.

Evictions are background; in-flight queries are never interrupted.

## See also

- [Configuration](../operations/configuration.md): full YAML schema
- [Deploy with Helm](../operations/deploy-helm.md)
