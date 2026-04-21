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
// don't re-dial every 30s.
type Pool struct {
	mu      sync.Mutex
	clients map[string]*Client
}

// NewPool returns an empty client pool. Call Close to release all underlying
// connections (typically from the operator's shutdown path).
func NewPool() *Pool {
	return &Pool{clients: map[string]*Client{}}
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

// Close tears down every pooled connection.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.clients {
		_ = c.Close()
	}
	p.clients = map[string]*Client{}
}

// Client is a thin facade over the generated VectorDBService gRPC client.
type Client struct {
	conn   *grpc.ClientConn
	stub   pb.VectorDBServiceClient
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
