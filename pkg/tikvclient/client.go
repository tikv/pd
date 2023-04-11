// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikvclient

import (
	"context"
	"crypto/tls"
	"sync"

	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pkg/errors"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type clientConn struct {
	// The target host
	target string
	conn   *grpc.ClientConn
}

// Close gprc connection.
func (c *clientConn) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Get grpc.ClientConn.
func (c *clientConn) Get() *grpc.ClientConn {
	return c.conn
}

func newClientConn(ctx context.Context, addr string, tlsConfig *tls.Config, grpcDialOpt []grpc.DialOption) (*clientConn, error) {
	c, err := grpcutil.GetClientConn(ctx, addr, tlsConfig, grpcDialOpt...)
	if err != nil {
		return nil, err
	}
	return &clientConn{
		target: addr,
		conn:   c,
	}, nil
}

type option struct {
	gRPCDialOptions []grpc.DialOption
	tlsConfig       grpcutil.TLSConfig
}

// Opt is the option for the client.
type Opt func(*option)

// WithSecurity is used to set the TLSConfig.
func WithSecurity(tlsConfig grpcutil.TLSConfig) Opt {
	return func(c *option) {
		c.tlsConfig = tlsConfig
	}
}

// WithGRPCDialOptions is used to set the grpc.DialOption.
func WithGRPCDialOptions(grpcDialOptions ...grpc.DialOption) Opt {
	return func(c *option) {
		c.gRPCDialOptions = grpcDialOptions
	}
}

// RPCClient is RPC client struct used to manages connections with tikv-servers.
type RPCClient struct {
	sync.RWMutex

	clientConns map[string]*clientConn
	isClosed    bool
	option      *option
}

// NewRPCClient creates a RPC client
func NewRPCClient(opts ...Opt) *RPCClient {
	cli := &RPCClient{
		clientConns: make(map[string]*clientConn),
		option:      new(option),
		isClosed:    false,
	}
	for _, opt := range opts {
		opt(cli.option)
	}
	return cli
}

// Close closes all connections.
func (r *RPCClient) Close() {
	r.Lock()
	defer r.Unlock()
	if !r.isClosed {
		r.isClosed = true
		for _, cc := range r.clientConns {
			cc.Close()
		}
	}
}

func (r *RPCClient) getClientConn(ctx context.Context, addr string) (*clientConn, error) {
	r.Lock()
	defer r.Unlock()

	if r.isClosed {
		return nil, errors.Errorf("rpcClient is closed")
	}
	cc, ok := r.clientConns[addr]
	if ok {
		if cc.conn.GetState() != connectivity.Shutdown {
			return cc, nil
		}
		cc.Close()
	}

	var (
		tlsConfig *tls.Config
		err       error
	)
	if r.option != nil && len(r.option.tlsConfig.CAPath) != 0 {
		tlsConfig, err = r.option.tlsConfig.ToTLSConfig()
		if err != nil {
			return nil, err
		}
	}

	cc, err = newClientConn(ctx, addr, tlsConfig, r.option.gRPCDialOptions)
	if err != nil {
		return nil, err
	}
	r.clientConns[addr] = cc

	return cc, nil
}

// GetTiKVClient returns a raw TiKV RPC client.
func (r *RPCClient) GetTiKVClient(ctx context.Context, addr string) (tikvpb.TikvClient, error) {
	clientConn, err := r.getClientConn(ctx, addr)
	if err != nil {
		return nil, err
	}

	return tikvpb.NewTikvClient(clientConn.conn), nil
}
