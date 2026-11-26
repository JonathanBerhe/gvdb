# gvdbpb

Generated Go gRPC stubs for GVDB's public `VectorDBService` and the internal
`InternalService`, in package `gvdbpb`.

The operator calls `ListCollections` and `GetStats` for `GVDBClusterStatus`,
and `GetLeaderInfo` and `GetClusterHealth` on the internal service. The full
stubs are generated, so a new reconciler call needs no second code generation
step.

## Regenerating

Regenerate the stubs from `proto/vectordb.proto` and `proto/internal.proto`
with:

```
make generate-operator-pb
```

Run it after every change to the files in `proto/`, so that the stubs do not
drift from the server.
