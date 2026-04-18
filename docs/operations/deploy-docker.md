# Deploy with Docker

Run a single-node GVDB server in a container. Best for development and small workloads.

## Run

```bash
docker run -d \
  --name gvdb \
  -p 50051:50051 \
  -v "$PWD/gvdb-data:/var/lib/gvdb" \
  ghcr.io/jonathanberhe/gvdb:latest \
  gvdb-single-node --port 50051 --data-dir /var/lib/gvdb
```

The image publishes both GVDB binaries and supporting libraries. The same image is used for every node type — change the `CMD` to pick the role.

## Environment variables

All binaries accept env vars for cloud-native deployments:

| Var | Effect |
|-----|--------|
| `GVDB_BIND_ADDRESS` | Override `--bind-address` |
| `GVDB_ADVERTISE_ADDRESS` | Override `--advertise-address` |
| `GVDB_DATA_DIR` | Override `--data-dir` |
| `GVDB_CONFIG` | Path to YAML config file |

Example:

```bash
docker run -d \
  -e GVDB_BIND_ADDRESS=0.0.0.0:50051 \
  -e GVDB_ADVERTISE_ADDRESS=gvdb.example.com:50051 \
  -e GVDB_DATA_DIR=/var/lib/gvdb \
  -v "$PWD/gvdb-data:/var/lib/gvdb" \
  -p 50051:50051 \
  ghcr.io/jonathanberhe/gvdb:latest \
  gvdb-single-node
```

## Config file

Mount a YAML config:

```bash
docker run -d \
  -v "$PWD/config.yaml:/etc/gvdb/config.yaml" \
  -v "$PWD/gvdb-data:/var/lib/gvdb" \
  -p 50051:50051 \
  ghcr.io/jonathanberhe/gvdb:latest \
  gvdb-single-node --config /etc/gvdb/config.yaml
```

Example `config.yaml`:

```yaml
server:
  grpc_port: 50051
  auth:
    enabled: true
    rbac:
      users:
        - api_key: "admin-key"
          role: admin
          collections: ["*"]

storage:
  data_dir: /var/lib/gvdb

logging:
  level: info
```

See the [configuration reference](configuration.md) for every setting.

## docker-compose

```yaml title="docker-compose.yml"
services:
  gvdb:
    image: ghcr.io/jonathanberhe/gvdb:latest
    command: gvdb-single-node --port 50051 --data-dir /var/lib/gvdb
    ports:
      - "50051:50051"
    volumes:
      - gvdb-data:/var/lib/gvdb
    environment:
      GVDB_BIND_ADDRESS: "0.0.0.0:50051"

volumes:
  gvdb-data:
```

## Distributed deployments

For multi-node clusters, use the [Helm chart](deploy-helm.md). Running the distributed binaries directly with Docker is supported but requires manual coordination of node addresses, ports, and config.

## Metal GPU

The prebuilt image is Linux x86_64. Apple Metal acceleration is macOS-only and requires a [source build](../getting-started/installation.md#build-from-source) with `-DGVDB_WITH_METAL=ON`.

## See also

- [Installation](../getting-started/installation.md)
- [Helm deploy](deploy-helm.md) for clusters
- [Configuration](configuration.md)
