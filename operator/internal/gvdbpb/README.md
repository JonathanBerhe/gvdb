# gvdbpb

Generated Go gRPC stubs for GVDB's public `VectorDBService` and the internal
`InternalService`, in package `gvdbpb`.

The operator calls `ListCollections` and `GetStats` for `GVDBClusterStatus`,
and `GetLeaderInfo` and `GetClusterHealth` on the internal service. The full
stubs are generated, so a new reconciler call needs no second code generation
step.

## Regenerating

Do not edit these files. After a change to `proto/`, run this command from the
repository root:

```
make proto
```

It regenerates these stubs, the Python SDK stubs and the UI gateway stubs with
the generator versions in `tools/proto/versions.env`. CI fails when a checked-in
stub differs from the generated one.
