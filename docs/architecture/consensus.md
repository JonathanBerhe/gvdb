# Consensus

GVDB replicates **metadata** (collection definitions, shard assignments, node membership) through the **Raft** protocol, implemented with **NuRaft**.

Data (vectors) is **not** replicated through Raft — it uses direct segment replication over gRPC. Only the metadata state machine is consensus-managed.

## Roles

- **Leader** — the single coordinator that accepts writes
- **Followers** — stay in sync via log replication
- **Candidates** — briefly exist during elections

With 3 coordinators, GVDB tolerates a single-node failure while remaining writable.

## State machine

The Raft log applies entries to an in-memory metadata store:

- `CreateCollection`, `DropCollection`
- `AssignShard`, `UpdateShardState` (ACTIVE ↔ MIGRATING)
- `RegisterNode`, `DeregisterNode`
- `UpdateReplicaSet`

## Persistent log

Each coordinator stores its Raft log in **RocksDB**:

- Log entries survive restarts
- Snapshotting trims old entries once applied
- Point-in-time recovery by replaying from snapshot + tail

## Timestamp oracle (TSO)

The leader also acts as a timestamp oracle. Every metadata change receives a monotonically increasing timestamp, providing **total ordering** for distributed operations — e.g. for collection-version-based cache invalidation in the query node.

## Failure handling

- **Leader failure** — followers elect a new leader; clients retry via the proxy
- **Follower failure** — leader continues with quorum; followers catch up via log replication on recovery
- **Split brain** — impossible by construction (requires majority to make progress)

## Related code

- `src/consensus/` — NuRaft state machine, log store, RocksDB adapter
- `src/cluster/coordinator.cpp` — uses the consensus layer
- `include/consensus/` — public API

See also the module's `CLAUDE.md` in the repo.

## See also

- [Modules](modules.md)
- [Distributed cluster](../getting-started/distributed-cluster.md)
