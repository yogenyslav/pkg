// Package grpcclient is a wrapper for grpc client methods.
package grpcclient

import (
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GrpcClient holds available methods of grpc client.
type GrpcClient interface {
	GetConn() *grpc.ClientConn
	Close() error
}

// GrpcClient is an internal wrap for grpc.ClientConn.
type grpcClient struct {
	conn *grpc.ClientConn
}

// NewGrpcClient creates new GrpcClient.
func NewGrpcClient(cfg *Config, opts ...grpc.DialOption) (GrpcClient, error) {
	var grpcOpts []grpc.DialOption
	grpcOpts = append(grpcOpts, opts...)
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	conn, err := grpc.NewClient(addr, grpcOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create grpc client: %w", err)
	}

	return &grpcClient{conn: conn}, nil
}

// NewGrpcClientWithInsecure creates new GrpcClient with insecure credentials option enabled.
func NewGrpcClientWithInsecure(cfg *Config) (GrpcClient, error) {
	return NewGrpcClient(cfg, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// Close grpc.ClientConn.
func (c *grpcClient) Close() error {
	return fmt.Errorf("error closing conn: %w", c.conn.Close())
}

// GetConn returns grpc.ClientConn.
func (c *grpcClient) GetConn() *grpc.ClientConn {
	return c.conn
}
