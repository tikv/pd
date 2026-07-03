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

package server

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/grpcutil"
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
	listenAddr  string
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
		tlsConfig, err := tlsCfg.ToClientTLSConfig()
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
	tlsConfig, err := tlsCfg.ToServerTLSConfig()
	if err != nil {
		return err
	}
	if tlsConfig != nil {
		bs.secure = true
		bs.muxListener, err = tls.Listen(constant.TCPNetworkStr, listenURL.Host, tlsConfig)
	} else {
		bs.muxListener, err = net.Listen(constant.TCPNetworkStr, listenURL.Host)
	}
	if err != nil {
		return err
	}
	bs.listenAddr = buildActualListenAddr(listenURL, bs.muxListener.Addr())
	return nil
}

// GetListener returns the listener.
func (bs *BaseServer) GetListener() net.Listener {
	return bs.muxListener
}

// GetActualListenAddr returns the listener address after binding.
func (bs *BaseServer) GetActualListenAddr() string {
	return bs.listenAddr
}

// ResolveListenAddr returns actualListenAddr only when listenAddr points at a
// kernel-selected port.
func ResolveListenAddr(listenAddr, actualListenAddr string) string {
	if hasZeroPort(listenAddr) {
		return actualListenAddr
	}
	return listenAddr
}

// ResolveAdvertiseListenAddr returns actualListenAddr when advertiseAddr was left
// unspecified. If advertiseAddr points at a kernel-selected port, it preserves
// the explicit advertise host and replaces only the port.
func ResolveAdvertiseListenAddr(advertiseAddr, actualListenAddr string) string {
	if advertiseAddr == "" {
		return actualListenAddr
	}
	if hasZeroPort(advertiseAddr) {
		return replacePort(advertiseAddr, actualListenAddr)
	}
	return advertiseAddr
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

func buildActualListenAddr(listenURL *url.URL, addr net.Addr) string {
	host, _, err := net.SplitHostPort(listenURL.Host)
	if err != nil || host == "" {
		host, _, _ = net.SplitHostPort(addr.String())
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		if ip.To4() == nil {
			host = "::1"
		} else {
			host = "127.0.0.1"
		}
	}
	_, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		return listenURL.String()
	}
	actualURL := *listenURL
	actualURL.Host = net.JoinHostPort(host, port)
	return actualURL.String()
}

func hasZeroPort(addr string) bool {
	_, port, ok := splitHostPort(addr)
	return ok && port == "0"
}

func replacePort(addr, actualListenAddr string) string {
	_, actualPort, ok := splitHostPort(actualListenAddr)
	if !ok {
		return actualListenAddr
	}

	parsed, err := url.Parse(addr)
	if err == nil && parsed.Host != "" {
		host, _, ok := splitHostPort(parsed.Host)
		if !ok {
			return actualListenAddr
		}
		parsed.Host = net.JoinHostPort(host, actualPort)
		return parsed.String()
	}

	host, _, ok := splitHostPort(addr)
	if !ok {
		return actualListenAddr
	}
	return net.JoinHostPort(host, actualPort)
}

func splitHostPort(addr string) (string, string, bool) {
	parsed, err := url.Parse(addr)
	host := addr
	if err == nil && parsed.Host != "" {
		host = parsed.Host
	}
	_, port, err := net.SplitHostPort(host)
	if err != nil {
		return "", "", false
	}
	host, _, err = net.SplitHostPort(host)
	return host, port, err == nil
}
