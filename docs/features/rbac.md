# RBAC

Role-based access control with per-collection scoping. Configured via the server YAML, enforced by a gRPC interceptor.

## Roles

| Role | Permissions |
|------|-------------|
| `admin` | All operations on all collections |
| `readwrite` | insert, search, get, delete, upsert, update, range_search, hybrid_search, list on assigned collections |
| `readonly` | search, get, range_search, hybrid_search, list on assigned collections |
| `collection_admin` | All ops **except** create/drop on assigned collections |

`HealthCheck` and `GetStats` are always allowed without authentication.

## YAML configuration

Auth is **not** a Helm value — configure it in the server config mounted from your own ConfigMap / Secret. See [Configuration](../operations/configuration.md) for the full schema.

```yaml
server:
  auth:
    enabled: true
    roles:
      - key: "admin-key-abc123"
        role: admin
        collections: ["*"]

      - key: "analyst-key-xyz789"
        role: readonly
        collections: ["products", "reviews"]

      - key: "writer-key-def456"
        role: readwrite
        collections: ["sessions"]
```

Wildcards (`["*"]`) grant access to every collection.

## Connect with an API key

=== "Python"

    ```python
    from gvdb import GVDBClient
    client = GVDBClient("localhost:50051", api_key="analyst-key-xyz789")
    ```

=== "Java"

    ```java
    var config = GvdbClientConfig.builder("localhost:50051")
        .apiKey("analyst-key-xyz789")
        .build();
    var client = new GvdbClient(config);
    ```

The client sends `authorization: Bearer <key>` as gRPC metadata.

## Legacy `api_keys` list

For backward compatibility, a flat list is still accepted. Keys declared this way are treated as `admin`:

```yaml
server:
  auth:
    enabled: true
    api_keys:
      - "legacy-key-1"
      - "legacy-key-2"
```

Migrate to `auth.roles` as soon as you can — legacy keys have no scoping.

## Audit logging

Enable structured JSON audit logs for every non-public RPC:

```yaml
logging:
  audit:
    enabled: true
    file_path: "/var/log/gvdb/audit.jsonl"
```

Each entry records `timestamp`, `api_key_id`, `operation`, `collection`, `status`, `grpc_code`, `latency_ms`, `item_count`.

## See also

- [Security](../operations/security.md) — TLS, API keys, and RBAC in production
- [Configuration](../operations/configuration.md) — full YAML schema
