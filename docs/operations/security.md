# Security

GVDB supports **TLS** (optionally mutual), **API key authentication**, **RBAC**, and **structured audit logging**. Production deployments should enable all four.

## TLS

```yaml
server:
  tls:
    enabled: true
    cert_path: /etc/gvdb/tls/server.crt
    key_path: /etc/gvdb/tls/server.key
    # for mutual TLS:
    client_ca_path: /etc/gvdb/tls/ca.crt
```

Clients connect with TLS enabled:

=== "Python"

    ```python
    client = GVDBClient("gvdb.example.com:50051", tls=True, api_key="...")
    ```

=== "Java"

    ```java
    var config = GvdbClientConfig.builder("gvdb.example.com:50051")
        .tls(true)
        .apiKey("...")
        .build();
    ```

## API keys

Two ways to declare keys:

### 1. RBAC users (recommended)

```yaml
auth:
  enabled: true
  rbac:
    users:
      - api_key: "admin-key-abc123"
        role: admin
        collections: ["*"]

      - api_key: "reader-key-def456"
        role: readonly
        collections: ["products", "reviews"]
```

### 2. Legacy `api_keys` list

```yaml
auth:
  enabled: true
  api_keys:
    - "legacy-admin-1"
    - "legacy-admin-2"
```

Keys in this list are treated as `admin`. Migrate to RBAC when you can — legacy keys have no per-collection scoping.

## RBAC roles

| Role | Permissions |
|------|-------------|
| `admin` | All operations on all collections |
| `readwrite` | insert, search, get, delete, upsert, update on assigned collections |
| `readonly` | search, get, range_search, hybrid_search, list on assigned collections |
| `collection_admin` | All ops **except** create/drop on assigned collections |

`HealthCheck` and `GetStats` are always allowed without authentication.

See [RBAC](../features/rbac.md) for the full documentation.

## Audit logging

Every non-public RPC emits a structured JSON line:

```yaml
logging:
  audit:
    enabled: true
    path: /var/log/gvdb/audit.jsonl
    rotation_mb: 100
    max_files: 10
```

```jsonl
{"timestamp":"2026-04-18T10:23:01.412Z","api_key_id":"reader-key-***","operation":"Search","collection":"products","status":"OK","grpc_code":0,"latency_ms":4,"item_count":10}
```

The audit logger flushes synchronously — audit events must not be dropped.

## Secrets management

- **Kubernetes**: store keys and TLS material in `Secret` resources, mount into pods, reference via env or file paths. See the [Helm chart](deploy-helm.md).
- **Docker**: use Docker secrets, mounted volumes, or env files with restricted permissions.
- **Never** commit API keys or TLS private keys to git.

## Network isolation

- In Kubernetes, use `NetworkPolicy` to restrict traffic to the proxy from known app namespaces.
- For multi-tenant setups, combine RBAC collection scoping with per-tenant API keys.

## Multi-tenancy

Phase 1 (`tenant_id` on collection metadata, RBAC restricts keys to a tenant's collections) is in progress — see the [roadmap](../roadmap.md).

## See also

- [RBAC feature](../features/rbac.md)
- [Configuration](configuration.md)
- [Monitoring](monitoring.md) — audit log schema
