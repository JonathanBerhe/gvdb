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
	"time"

	pb "gvdb/operator/internal/gvdbpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

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
func Dial(ctx context.Context, target string) (*Client, error) {
	// Use DialContext so the reconciler can impose its own timeout via ctx.
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("grpc dial %s: %w", target, err)
	}
	return &Client{conn: conn, stub: pb.NewVectorDBServiceClient(conn), target: target}, nil
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
