// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// BaseServer is a basic server that provides some common functionality.
type BaseServer struct {
	ctx context.Context
	// etcd client
	etcdClient *clientv3.Client
	// http client
	httpClient *http.Client
	grpcServer *grpc.Server
	httpServer *http.Server
	// Store as map[string]*grpc.ClientConn
	clientConns sync.Map
	secure      bool
	muxListener net.Listener
	// Callback functions for different stages
	// startCallbacks will be called after the server is started.
	startCallbacks []func()
	startTimestamp int64
}

// NewBaseServer creates a new BaseServer.
func NewBaseServer(ctx context.Context) *BaseServer {
	return &BaseServer{
		ctx:            ctx,
		startTimestamp: time.Now().Unix(),
	}
}

// Context returns the context of server.
func (bs *BaseServer) Context() context.Context {
	return bs.ctx
}

// GetDelegateClient returns grpc client connection talking to the forwarded host.
func (bs *BaseServer) GetDelegateClient(ctx context.Context, tlsCfg *grpcutil.TLSConfig, forwardedHost string) (*grpc.ClientConn, error) {
	client, ok := bs.clientConns.Load(forwardedHost)
	if !ok {
		tlsConfig, err := tlsCfg.ToTLSConfig()
		if err != nil {
			return nil, err
		}
		cc, err := grpcutil.GetClientConn(ctx, forwardedHost, tlsConfig)
		if err != nil {
			return nil, err
		}
		client = cc
		bs.clientConns.Store(forwardedHost, cc)
	}
	return client.(*grpc.ClientConn), nil
}

// GetClientConns returns the client connections.
func (bs *BaseServer) GetClientConns() *sync.Map {
	return &bs.clientConns
}

// GetClient returns builtin etcd client.
func (bs *BaseServer) GetClient() *clientv3.Client {
	return bs.etcdClient
}

// GetHTTPClient returns builtin http client.
func (bs *BaseServer) GetHTTPClient() *http.Client {
	return bs.httpClient
}

// SetEtcdClient sets the etcd client.
func (bs *BaseServer) SetEtcdClient(etcdClient *clientv3.Client) {
	bs.etcdClient = etcdClient
}

// GetEtcdClient returns the etcd client.
func (bs *BaseServer) GetEtcdClient() *clientv3.Client {
	return bs.etcdClient
}

// SetHTTPClient sets the http client.
func (bs *BaseServer) SetHTTPClient(httpClient *http.Client) {
	bs.httpClient = httpClient
}

// AddStartCallback adds a callback in the startServer phase.
func (bs *BaseServer) AddStartCallback(callbacks ...func()) {
	bs.startCallbacks = append(bs.startCallbacks, callbacks...)
}

// GetStartCallbacks returns the start callbacks.
func (bs *BaseServer) GetStartCallbacks() []func() {
	return bs.startCallbacks
}

// GetHTTPServer returns the http server.
func (bs *BaseServer) GetHTTPServer() *http.Server {
	return bs.httpServer
}

// SetHTTPServer sets the http server.
func (bs *BaseServer) SetHTTPServer(httpServer *http.Server) {
	bs.httpServer = httpServer
}

// GetGRPCServer returns the grpc server.
func (bs *BaseServer) GetGRPCServer() *grpc.Server {
	return bs.grpcServer
}

// SetGRPCServer sets the grpc server.
func (bs *BaseServer) SetGRPCServer(grpcServer *grpc.Server) {
	bs.grpcServer = grpcServer
}

// InitListener initializes the listener.
func (bs *BaseServer) InitListener(tlsCfg *grpcutil.TLSConfig, listenAddr string) error {
	listenURL, err := url.Parse(listenAddr)
	if err != nil {
		return err
	}
	tlsConfig, err := tlsCfg.ToTLSConfig()
	if err != nil {
		return err
	}
	if tlsConfig != nil {
		bs.secure = true
		bs.muxListener, err = tls.Listen(constant.TCPNetworkStr, listenURL.Host, tlsConfig)
	} else {
		bs.muxListener, err = net.Listen(constant.TCPNetworkStr, listenURL.Host)
	}
	return err
}

// GetListener returns the listener.
func (bs *BaseServer) GetListener() net.Listener {
	return bs.muxListener
}

// IsSecure checks if the server enable TLS.
func (bs *BaseServer) IsSecure() bool {
	return bs.secure
}

// StartTimestamp returns the start timestamp of this server
func (bs *BaseServer) StartTimestamp() int64 {
	return bs.startTimestamp
}

// CloseClientConns closes all client connections.
func (bs *BaseServer) CloseClientConns() {
	bs.clientConns.Range(func(_, value any) bool {
		conn := value.(*grpc.ClientConn)
		if err := conn.Close(); err != nil {
			log.Error("close client connection meet error")
		}
		return true
	})
}
