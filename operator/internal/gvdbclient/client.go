/*
Copyright 2026 GVDB.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package gvdbclient wraps GVDB's gRPC surface in a small, reconciler-friendly
// facade. Today it exposes only the two calls the GVDBCluster status block
// needs (collection + vector totals); future follow-ups (0b.6.B backup/restore)
// extend this file.
package gvdbclient

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "gvdb/operator/internal/gvdbpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Pool caches long-lived gRPC clients keyed by target so reconciler passes
// don't re-dial every 30s. Holds both proxy-fronted VectorDBService clients
// and coordinator-targeted InternalService clients.
type Pool struct {
	mu           sync.Mutex
	clients      map[string]*Client
	coordinators map[string]*CoordinatorClient
}

// NewPool returns an empty client pool. Call Close to release all underlying
// connections (typically from the operator's shutdown path).
func NewPool() *Pool {
	return &Pool{
		clients:      map[string]*Client{},
		coordinators: map[string]*CoordinatorClient{},
	}
}

// Get returns a cached client for the target, dialing lazily on first use.
func (p *Pool) Get(target string) (*Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.clients[target]; ok {
		return c, nil
	}
	c, err := Dial(target)
	if err != nil {
		return nil, err
	}
	p.clients[target] = c
	return c, nil
}

// GetCoordinator returns a cached InternalService client for the target,
// dialing lazily on first use. Callers pass a coordinator pod FQDN like
// "prod-coordinator-0.prod-coordinator.gvdb.svc.cluster.local:50051".
func (p *Pool) GetCoordinator(target string) (*CoordinatorClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.coordinators[target]; ok {
		return c, nil
	}
	c, err := DialCoordinator(target)
	if err != nil {
		return nil, err
	}
	p.coordinators[target] = c
	return c, nil
}

// Close tears down every pooled connection.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.clients {
		_ = c.Close()
	}
	for _, c := range p.coordinators {
		_ = c.Close()
	}
	p.clients = map[string]*Client{}
	p.coordinators = map[string]*CoordinatorClient{}
}

// CloseTarget tears down the cached connections (proxy + coordinator) for
// a single target. Called by the reconciler on CR deletion so renamed /
// re-created clusters don't leak connections from the old identity.
func (p *Pool) CloseTarget(target string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.clients[target]; ok {
		_ = c.Close()
		delete(p.clients, target)
	}
	if c, ok := p.coordinators[target]; ok {
		_ = c.Close()
		delete(p.coordinators, target)
	}
}

// CloseClusterTargets tears down every pooled connection whose target matches
// any of the given candidate addresses. Used by the reconciler's deletion
// path: we feed in the proxy address and all coordinator pod addresses so
// the pool loses its handle on a cluster that's going away.
func (p *Pool) CloseClusterTargets(targets []string) {
	for _, t := range targets {
		p.CloseTarget(t)
	}
}

// Client is a thin facade over the generated VectorDBService gRPC client.
// Used for proxy-fronted calls (GetStats / ListCollections). For internal
// RPCs (GetLeaderInfo / GetClusterHealth) dialed against a coordinator pod
// directly, use CoordinatorClient.
type Client struct {
	conn   *grpc.ClientConn
	stub   pb.VectorDBServiceClient
	target string
}

// CoordinatorClient wraps InternalService against a specific coordinator
// endpoint. The operator's rolling-upgrade state machine needs to keep
// asking "is there a leader?" even while the coordinator it asks first is
// being rolled — callers should cycle through CoordinatorClients dialed to
// different pods via Pool.GetCoordinator.
type CoordinatorClient struct {
	conn   *grpc.ClientConn
	stub   pb.InternalServiceClient
	target string
}

// Stats is the reconciler-relevant subset of data returned by the coordinator.
type Stats struct {
	CollectionCount int32
	TotalVectors    int64
}

// Dial opens a gRPC connection to the given "host:port" address using an
// insecure transport. In-cluster traffic between the operator pod and GVDB
// coordinator/proxy flows over the cluster pod network — TLS for that path
// is a follow-up (roadmap Tier 1.Full).
//
// Dial itself is non-blocking (grpc.NewClient establishes the connection
// lazily on first RPC); callers apply their own context timeout on the
// per-RPC call instead.
func Dial(target string) (*Client, error) {
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("grpc dial %s: %w", target, err)
	}
	return &Client{conn: conn, stub: pb.NewVectorDBServiceClient(conn), target: target}, nil
}

// Target returns the "host:port" the client was dialed for. The reconciler
// uses this to cache clients by target across reconciles.
func (c *Client) Target() string {
	if c == nil {
		return ""
	}
	return c.target
}

// DialCoordinator opens an InternalService connection to a coordinator pod.
func DialCoordinator(target string) (*CoordinatorClient, error) {
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("grpc dial %s: %w", target, err)
	}
	return &CoordinatorClient{
		conn:   conn,
		stub:   pb.NewInternalServiceClient(conn),
		target: target,
	}, nil
}

// Close releases the underlying gRPC connection.
func (c *CoordinatorClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// Target returns the "host:port" this coordinator client was dialed for.
func (c *CoordinatorClient) Target() string {
	if c == nil {
		return ""
	}
	return c.target
}

// LeaderInfo is the reconciler-relevant subset of GetLeaderInfoResponse.
type LeaderInfo struct {
	// LeaderID is the Raft node id of the current leader. <= 0 when unknown.
	LeaderID int32
	// LeaderAddress is a best-effort resolved pod-name or endpoint. Empty
	// when the coordinator couldn't resolve it.
	LeaderAddress string
	// IsLeaderSelf is true when the coordinator that answered is itself the
	// current leader (useful for debugging, not used by the state machine).
	IsLeaderSelf bool
	// CurrentTerm is the Raft term the responding coordinator believes is
	// current. 0 when the coordinator hasn't exposed it (single-node mode
	// or pre-start). The rollout state machine records this in a STS
	// annotation and refuses to advance partition when the term has
	// changed since the previous reconcile — the belt-and-braces check
	// that complements the mere "leader present" gate.
	CurrentTerm uint64
}

// HasLeader reports whether the response names a live leader. The rollout
// gate uses this: "safe to drain the next pod" iff HasLeader.
func (l LeaderInfo) HasLeader() bool { return l.LeaderID > 0 }

// FetchLeaderInfo asks the coordinator who the current Raft leader is.
// Bounded timeout keeps a reconcile pass from blocking on a pod that's
// mid-rollout.
func (c *CoordinatorClient) FetchLeaderInfo(ctx context.Context) (LeaderInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	resp, err := c.stub.GetLeaderInfo(ctx, &pb.GetLeaderInfoRequest{})
	if err != nil {
		return LeaderInfo{}, fmt.Errorf("get leader info: %w", err)
	}
	return LeaderInfo{
		LeaderID:      resp.GetLeaderId(),
		LeaderAddress: resp.GetLeaderAddress(),
		IsLeaderSelf:  resp.GetIsLeaderSelf(),
		CurrentTerm:   resp.GetCurrentTerm(),
	}, nil
}

// NodeHealth is the reconciler-relevant subset of NodeInfo. The data-node
// rollout state machine cross-references this against ShardAssignment.node_ids
// to decide whether a remaining replica is actually a safe failover target —
// coordinator.cpp:903 only promotes replicas that are NODE_STATUS_READY.
type NodeHealth struct {
	NodeID uint32
	// Ready is true when the coordinator last heartbeat-observed this node as
	// NODE_STATUS_READY. Other statuses (STARTING, BUSY, DEGRADED, DOWN,
	// DRAINING) are all "not safe to treat as a failover replica."
	Ready bool
	// IsDataNode is true when NodeInfo.node_type == NODE_TYPE_DATA_NODE.
	// Callers that reason about data-node-specific invariants (e.g. the
	// rollout's replica-safety gate) filter on this rather than on node-id
	// ranges — node-id conventions are coupled to the render layer and
	// could drift, but node_type is authoritative.
	IsDataNode bool
}

// ClusterHealth is the reconciler-relevant subset of GetClusterHealthResponse.
type ClusterHealth struct {
	ClusterStatus       string
	LastRebalanceUnixMs int64
	// TotalShards is the number of shards the coordinator is tracking across
	// all collections. Zero means "cluster has no data yet" — the data-node
	// rollout gate short-circuits in that case to avoid bootstrap deadlock.
	TotalShards uint32
	// HealthyShards counts shards with primary + all replicas reachable. When
	// HealthyShards < TotalShards the rollout gate holds: a migration is
	// either in flight or a replica is down.
	HealthyShards uint32
	// Nodes is the per-node health snapshot used by the Gate C replica-safety
	// check. Every entry keys by NodeID; a replica whose NodeID is missing or
	// not Ready is ignored when counting healthy failover candidates.
	Nodes []NodeHealth
}

// FetchClusterHealth returns coordinator-reported cluster health, including
// the last-rebalance timestamp used to populate status.lastRebalance.
func (c *CoordinatorClient) FetchClusterHealth(ctx context.Context) (ClusterHealth, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	resp, err := c.stub.GetClusterHealth(ctx, &pb.GetClusterHealthRequest{})
	if err != nil {
		return ClusterHealth{}, fmt.Errorf("get cluster health: %w", err)
	}
	nodes := make([]NodeHealth, 0, len(resp.GetNodes()))
	for _, n := range resp.GetNodes() {
		nodes = append(nodes, NodeHealth{
			NodeID:     n.GetNodeId(),
			Ready:      n.GetStatus() == pb.NodeStatus_NODE_STATUS_READY,
			IsDataNode: n.GetNodeType() == pb.NodeType_NODE_TYPE_DATA_NODE,
		})
	}
	return ClusterHealth{
		ClusterStatus:       resp.GetClusterStatus(),
		LastRebalanceUnixMs: resp.GetLastRebalanceUnixMs(),
		TotalShards:         resp.GetTotalShards(),
		HealthyShards:       resp.GetHealthyShards(),
		Nodes:               nodes,
	}, nil
}

// ShardAssignment is the reconciler-relevant subset of proto.ShardAssignment.
// The data-node rollout's Gate C reads this to verify that dropping the next
// pod to be drained still leaves every shard with a healthy replica elsewhere.
type ShardAssignment struct {
	ShardID       uint32
	PrimaryNodeID uint32
	NodeIDs       []uint32
}

// FetchShardAssignments returns the cluster-wide shard layout (collection_id=0
// means "all collections" per proto/internal.proto:84). Safety check for the
// data-node rollout gate: len(node_ids \ {target, non-ready}) >= 1 per shard.
func (c *CoordinatorClient) FetchShardAssignments(ctx context.Context) ([]ShardAssignment, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	resp, err := c.stub.GetShardAssignments(ctx, &pb.GetShardAssignmentsRequest{CollectionId: 0})
	if err != nil {
		return nil, fmt.Errorf("get shard assignments: %w", err)
	}
	assignments := make([]ShardAssignment, 0, len(resp.GetAssignments()))
	for _, a := range resp.GetAssignments() {
		nodeIDs := make([]uint32, len(a.GetNodeIds()))
		copy(nodeIDs, a.GetNodeIds())
		assignments = append(assignments, ShardAssignment{
			ShardID:       a.GetShardId(),
			PrimaryNodeID: a.GetPrimaryNodeId(),
			NodeIDs:       nodeIDs,
		})
	}
	return assignments, nil
}

// RaftMember is the reconciler-relevant subset of proto.RaftMember.
// Used by the coordinator scale reconciler (roadmap 1.8) to detect ghost
// peers after a hard-killed pod never ran its SIGTERM self-remove handler.
type RaftMember struct {
	NodeID       uint32
	RaftEndpoint string
	IsLearner    bool
}

// RaftMembership is the reconciler-relevant subset of
// GetRaftMembershipResponse. The CurrentLeaderID field mirrors the leader
// GetLeaderInfo returns, saving an extra round-trip when the scale
// reconciler needs both pieces of state.
type RaftMembership struct {
	Members         []RaftMember
	CurrentLeaderID int32
}

// FetchRaftMembership reports the responding coordinator's view of Raft
// cluster_config. Safe on any pod (leader or follower); all members
// observe config changes via Raft log replication.
func (c *CoordinatorClient) FetchRaftMembership(ctx context.Context) (RaftMembership, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	resp, err := c.stub.GetRaftMembership(ctx, &pb.GetRaftMembershipRequest{})
	if err != nil {
		return RaftMembership{}, fmt.Errorf("get raft membership: %w", err)
	}
	out := RaftMembership{
		CurrentLeaderID: resp.GetCurrentLeaderId(),
		Members:         make([]RaftMember, 0, len(resp.GetMembers())),
	}
	for _, m := range resp.GetMembers() {
		out.Members = append(out.Members, RaftMember{
			NodeID:       m.GetNodeId(),
			RaftEndpoint: m.GetRaftEndpoint(),
			IsLearner:    m.GetIsLearner(),
		})
	}
	return out, nil
}

// TransferLeadership asks the coordinator to yield leader role to a
// specific successor. Leader-only: when called on a follower, returns
// (currentLeaderID, nil) so the caller can retry against the leader.
// On transport failure returns (0, err). On acceptance-but-timeout
// (NuRaft transfer didn't commit within 3s) returns (leaderID, error)
// where leaderID is whoever answered.
func (c *CoordinatorClient) TransferLeadership(ctx context.Context, targetNodeID uint32) (int32, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := c.stub.TransferLeadership(ctx, &pb.TransferLeadershipRequest{
		TargetNodeId: targetNodeID,
	})
	if err != nil {
		return 0, fmt.Errorf("transfer leadership: %w", err)
	}
	if !resp.GetSuccess() {
		return resp.GetCurrentLeaderId(), fmt.Errorf("transfer leadership denied: %s", resp.GetMessage())
	}
	return resp.GetCurrentLeaderId(), nil
}

// RemovePeer asks the coordinator to drop a node from Raft membership.
// Leader-only; non-leader returns (currentLeaderID, error). Idempotent
// per 1.7b MapNuRaftCode: SERVER_NOT_FOUND and SERVER_IS_LEAVING both
// surface as success=true with a distinguishing message.
func (c *CoordinatorClient) RemovePeer(ctx context.Context, nodeID uint32) (int32, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := c.stub.RemovePeer(ctx, &pb.RemovePeerRequest{NodeId: nodeID})
	if err != nil {
		return 0, fmt.Errorf("remove peer: %w", err)
	}
	if !resp.GetSuccess() {
		return resp.GetCurrentLeaderId(), fmt.Errorf("remove peer denied: %s", resp.GetMessage())
	}
	return resp.GetCurrentLeaderId(), nil
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// FetchStats returns the cluster's collection count and total vector count
// via a single GetStats RPC. Callers should pass a bounded context; a short
// (~2s) timeout is appropriate for a reconciler that polls status frequently.
func (c *Client) FetchStats(ctx context.Context) (Stats, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	resp, err := c.stub.GetStats(ctx, &pb.GetStatsRequest{})
	if err != nil {
		return Stats{}, fmt.Errorf("get stats: %w", err)
	}
	return Stats{
		//nolint:gosec // server returns uint64 counts that fit int32/int64 at realistic cluster sizes.
		CollectionCount: int32(resp.GetTotalCollections()),
		//nolint:gosec // see above.
		TotalVectors: int64(resp.GetTotalVectors()),
	}, nil
}
