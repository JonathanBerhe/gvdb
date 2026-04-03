package main

import (
	"context"
	"time"

	pb "gvdb-ui/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type Gateway struct {
	conn   *grpc.ClientConn
	client pb.VectorDBServiceClient
	apiKey string
}

func NewGateway(addr, apiKey string) (*Gateway, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(256*1024*1024),
			grpc.MaxCallSendMsgSize(256*1024*1024),
		),
	)
	if err != nil {
		return nil, err
	}
	return &Gateway{
		conn:   conn,
		client: pb.NewVectorDBServiceClient(conn),
		apiKey: apiKey,
	}, nil
}

func (g *Gateway) Close() {
	g.conn.Close()
}

func (g *Gateway) ctx() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	if g.apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+g.apiKey)
	}
	return ctx
}
