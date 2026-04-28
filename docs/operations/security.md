# Security

GVDB supports **TLS** (optionally mutual), **API key authentication**, **RBAC**, and **structured audit logging**. Production deployments should enable all four.

!!! note
    Auth, TLS, and audit logging are configured in the **server-side YAML**, not as Helm values. The [Helm chart](deploy-helm.md) renders a ConfigMap — override it by mounting your own. See [Configuration](configuration.md) for the full schema.

## TLS

```yaml
server:
  tls:
    enabled: true
    cert_path: "/etc/gvdb/tls/server.crt"
    key_path: "/etc/gvdb/tls/server.key"
    ca_cert_path: "/etc/gvdb/tls/ca.crt"   # only needed if mutual_tls
    mutual_tls: true
```

Clients connect over an insecure channel by default. For TLS endpoints, use a custom gRPC channel:

=== "Python"

    ```python
    import grpc
    from gvdb.pb import vectordb_pb2_grpc as stub_pb
    from gvdb import GVDBClient

    creds = grpc.ssl_channel_credentials(open("ca.crt", "rb").read())
    # The GVDBClient currently constructs an insecure channel internally.
    # For TLS today, use the generated gRPC stubs directly. A
    # channel-factory kwarg on GVDBClient is planned.
    ```

=== "Java"

    ```java
    // Configure TLS via Netty channel options before building GvdbClient
    ```

## API keys

Two ways to declare keys:

### 1. RBAC via `server.auth.roles` (recommended)

```yaml
server:
  auth:
    enabled: true
    roles:
      - key: "admin-key-abc123"
        role: admin
        collections: ["*"]

      - key: "reader-key-def456"
        role: readonly
        collections: ["products", "reviews"]
```

### 2. Legacy `server.auth.api_keys` list

```yaml
server:
  auth:
    enabled: true
    api_keys:
      - "legacy-admin-1"
      - "legacy-admin-2"
```

Keys in this list are treated as `admin`. Migrate to `auth.roles` — legacy keys have no per-collection scoping.

### Passing the key from clients

=== "Python"

    ```python
    from gvdb import GVDBClient
    client = GVDBClient("gvdb.example.com:50051", api_key="admin-key-abc123")
    ```

=== "Java"

    ```java
    var config = GvdbClientConfig.builder("gvdb.example.com:50051")
        .apiKey("admin-key-abc123")
        .build();
    var client = new GvdbClient(config);
    ```

The client sends `authorization: Bearer <key>` as gRPC metadata.

## RBAC roles

| Role | Permissions |
|------|-------------|
| `admin` | All operations on all collections |
| `readwrite` | insert, search, get, delete, upsert, update on assigned collections |
| `readonly` | search, get, range_search, hybrid_search, list on assigned collections |
| `collection_admin` | All ops **except** create/drop on assigned collections |

`HealthCheck` and `GetStats` are always allowed without authentication.

See [RBAC](../features/rbac.md) for details.

## Audit logging

Every non-public RPC emits a structured JSON line when enabled:

```yaml
logging:
  audit:
    enabled: true
    file_path: "/var/log/gvdb/audit.jsonl"
    max_file_size_mb: 100
    max_files: 10
```

Each entry contains `timestamp`, `api_key_id`, `operation`, `collection`, `status`, `grpc_code`, `latency_ms`, `item_count`. The audit logger flushes synchronously — audit events must not be dropped.

## Secrets management

- **Kubernetes**: keep API keys and TLS material in `Secret` resources, mount into pods, reference via env or file paths.
- **Docker**: use Docker secrets, mounted volumes, or env files with restricted permissions.
- **Never** commit API keys or TLS private keys to git.

## Network isolation

The Helm chart ships per-workload `NetworkPolicy` templates that allow only the gRPC, Raft, and metrics traffic the chart documents, plus DNS egress to kube-system CoreDNS. Default off — enable in production:

```yaml
networkPolicy:
  enabled: true
  proxy:
    clientCIDRs: ["10.0.0.0/8"]   # VPC-only; empty list blocks ALL external traffic
```

Requires a CNI that enforces NetworkPolicy (kindnet 0.20+, Calico, Cilium, EKS VPC CNI with NP enabled, GKE NetworkPolicy add-on, AKS Calico, etc.). Override `networkPolicy.dns.*` if your cluster lacks the K8s 1.22+ automatic kube-system namespace label or runs CoreDNS elsewhere. See [Deploy with Helm — `networkPolicy`](deploy-helm.md#networkpolicy) for the full values reference.

For multi-tenant setups, combine the chart's NetworkPolicy with RBAC collection scoping and per-tenant API keys.

## Multi-tenancy

Phase 1 (`tenant_id` on collection metadata, RBAC restricted to a tenant's collections) is in progress.

## See also

- [RBAC feature](../features/rbac.md)
- [Configuration](configuration.md) — full YAML schema
- [Monitoring](monitoring.md) — audit log details
