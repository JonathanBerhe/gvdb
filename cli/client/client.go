package client

import (
	"context"
	"time"

	pb "gvdb-cli/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type Client struct {
	conn    *grpc.ClientConn
	Service pb.VectorDBServiceClient
	apiKey  string
	timeout time.Duration
	address string
}

type Options struct {
	Address string
	APIKey  string
	Timeout time.Duration
}

func New(opts Options) (*Client, error) {
	conn, err := grpc.NewClient(opts.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(256*1024*1024),
			grpc.MaxCallSendMsgSize(256*1024*1024),
		),
	)
	if err != nil {
		return nil, err
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	return &Client{
		conn:    conn,
		Service: pb.NewVectorDBServiceClient(conn),
		apiKey:  opts.APIKey,
		timeout: timeout,
		address: opts.Address,
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Address() string {
	return c.address
}

func (c *Client) Ctx() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), c.timeout)
	if c.apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.apiKey)
	}
	return ctx
}

func (c *Client) Stream() context.Context {
	ctx := context.Background()
	if c.apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.apiKey)
	}
	return ctx
}
