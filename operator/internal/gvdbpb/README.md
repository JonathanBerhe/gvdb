# gvdbpb

Generated Go gRPC stubs for GVDB's public `VectorDBService`.

These files are a copy of `test/e2e/pb/vectordb.{pb,_grpc.pb}.go` with the
package renamed to `gvdbpb`. The operator uses only `ListCollections` and
`GetStats` today for `GVDBClusterStatus`; the full stubs are copied so future
reconciler enhancements (e.g. 0b.6.B backup/restore RPCs) don't need a second
code-gen pass.

## Regenerating

Regenerate both locations from the canonical `proto/vectordb.proto`:

```
make generate-operator-pb
```

(defined in the repo root Makefile once CI integration lands in commit 7).

Any change to `proto/vectordb.proto` must be rolled into both
`test/e2e/pb/` and `operator/internal/gvdbpb/` so stubs don't drift.
