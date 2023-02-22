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
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/pkg/versioninfo"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// If server doesn't implement all methods of bs.Server, this line will result in a clear
// error message like "*Server does not implement bs.Server (missing Method method)"
var _ bs.Server = (*Server)(nil)

// Server is the TSO server, and it implements bs.Server and tso.GrpcServer.
type Server struct {
	diagnosticspb.DiagnosticsServer

	// Server start timestamp
	startTimestamp int64

	ctx       context.Context
	name      string
	clusterID uint64
	// etcd client
	client *clientv3.Client
	// http client
	httpClient          *http.Client
	tsoAllocatorManager *tso.AllocatorManager
	// Store as map[string]*grpc.ClientConn
	clientConns sync.Map
	// Store as map[string]chan *tsoRequest
	tsoDispatcher sync.Map
	// Callback functions for different stages
	// startCallbacks will be called after the server is started.
	startCallbacks []func()
}

// NewServer creates a new TSO server.
func NewServer(ctx context.Context, client *clientv3.Client, httpClient *http.Client,
	dxs diagnosticspb.DiagnosticsServer) *Server {
	return &Server{
		DiagnosticsServer: dxs,
		startTimestamp:    time.Now().Unix(),
		ctx:               ctx,
		name:              "TSO",
		client:            client,
		httpClient:        httpClient,
	}
}

// TODO: Implement the following methods defined in bs.Server

// Name returns the unique etcd Name for this server in etcd cluster.
func (s *Server) Name() string {
	return s.name
}

// Context returns the context of server.
func (s *Server) Context() context.Context {
	return s.ctx
}

// Run runs the TSO server.
func (s *Server) Run() error {
	return nil
}

// Close closes the server.
func (s *Server) Close() {
}

// GetClient returns builtin etcd client.
func (s *Server) GetClient() *clientv3.Client {
	return s.client
}

// GetHTTPClient returns builtin http client.
func (s *Server) GetHTTPClient() *http.Client {
	return s.httpClient
}

// AddStartCallback adds a callback in the startServer phase.
func (s *Server) AddStartCallback(callbacks ...func()) {
	s.startCallbacks = append(s.startCallbacks, callbacks...)
}

// IsServing returns whether the server is the leader, if there is embedded etcd, or the primary otherwise.
func (s *Server) IsServing() bool {
	// TODO: implement this
	return true
}

// AddServiceReadyCallback adds the callback function when the server becomes the leader, if there is embedded etcd, or the primary otherwise.
// the global TSO allocator after the flag 'enable-local-tso' is set to true.
func (s *Server) AddServiceReadyCallback(callbacks ...func(context.Context)) {
	// Leave it empty
	// TODO: implment it when integerating with the Local/Global TSO Allocator.
}

// Implement the other methods

// ClusterID returns the cluster ID of this server.
func (s *Server) ClusterID() uint64 {
	return s.clusterID
}

// IsClosed checks if the server loop is closed
func (s *Server) IsClosed() bool {
	// TODO: implement it
	return true
}

// GetTSOAllocatorManager returns the manager of TSO Allocator.
func (s *Server) GetTSOAllocatorManager() *tso.AllocatorManager {
	return s.tsoAllocatorManager
}

// GetTSODispatcher gets the TSO Dispatcher
func (s *Server) GetTSODispatcher() *sync.Map {
	return &s.tsoDispatcher
}

// IsLocalRequest checks if the forwarded host is the current host
func (s *Server) IsLocalRequest(forwardedHost string) bool {
	// TODO: Check if the forwarded host is the current host
	// and we can't use ClientUrls because that's for the remote
	// etcd cluster. The TSO microservice doesn't use embedded etcd.
	return forwardedHost == ""
}

// CreateTsoForwardStream creats the forward stream
func (s *Server) CreateTsoForwardStream(client *grpc.ClientConn) (tsopb.TSO_TsoClient, context.CancelFunc, error) {
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(s.ctx)
	go checkStream(ctx, cancel, done)
	forwardStream, err := tsopb.NewTSOClient(client).Tso(ctx)
	done <- struct{}{}
	return forwardStream, cancel, err
}

// GetDelegateClient returns grpc client connection talking to the forwarded host
func (s *Server) GetDelegateClient(ctx context.Context, forwardedHost string) (*grpc.ClientConn, error) {
	client, ok := s.clientConns.Load(forwardedHost)
	if !ok {
		tlsConfig, err := s.GetTLSConfig().ToTLSConfig()
		if err != nil {
			return nil, err
		}
		cc, err := grpcutil.GetClientConn(ctx, forwardedHost, tlsConfig)
		if err != nil {
			return nil, err
		}
		client = cc
		s.clientConns.Store(forwardedHost, cc)
	}
	return client.(*grpc.ClientConn), nil
}

// ValidateInternalRequest checks if server is closed, which is used to validate
// the gRPC communication between TSO servers internally.
// TODO: Check if the sender is from the global TSO allocator
func (s *Server) ValidateInternalRequest(_ *tsopb.RequestHeader, _ bool) error {
	if s.IsClosed() {
		return ErrNotStarted
	}
	return nil
}

// ValidateRequest checks if the keyspace replica is the primary and clusterID is matched.
// TODO: Check if the keyspace replica is the primary
func (s *Server) ValidateRequest(header *tsopb.RequestHeader) error {
	if s.IsClosed() {
		return ErrNotLeader
	}
	if header.GetClusterId() != s.clusterID {
		return status.Errorf(codes.FailedPrecondition, "mismatch cluster id, need %d but got %d", s.clusterID, header.GetClusterId())
	}
	return nil
}

// Implement other methods

// GetGlobalTS returns global tso.
func (s *Server) GetGlobalTS() (uint64, error) {
	ts, err := s.tsoAllocatorManager.GetGlobalTSO()
	if err != nil {
		return 0, err
	}
	return tsoutil.GenerateTS(ts), nil
}

// GetExternalTS returns external timestamp from the cache or the persistent storage.
// TODO: Implement GetExternalTS
func (s *Server) GetExternalTS() uint64 {
	return 0
}

// SetExternalTS saves external timestamp to cache and the persistent storage.
// TODO: Implement SetExternalTS
func (s *Server) SetExternalTS(externalTS uint64) error {
	return nil
}

// TODO: If goroutine here timeout after a stream is created successfully, we need to handle it correctly.
func checkStream(streamCtx context.Context, cancel context.CancelFunc, done chan struct{}) {
	select {
	case <-done:
		return
	case <-time.After(3 * time.Second):
		cancel()
	case <-streamCtx.Done():
	}
	<-done
}

// GetTLSConfig get the security config.
// TODO: implement it
func (s *Server) GetTLSConfig() *grpcutil.TLSConfig {
	return nil
}

// CreateServerWrapper encapsulates the configuration/log/metrics initialization and create the server
func CreateServerWrapper(cmd *cobra.Command, args []string) {
	cmd.Flags().Parse(args)
	cfg := tso.NewConfig()
	flagSet := cmd.Flags()
	err := cfg.Parse(flagSet)
	defer logutil.LogPanic()

	if err != nil {
		cmd.Println(err)
		return
	}

	if printVersion, err := flagSet.GetBool("version"); err != nil {
		cmd.Println(err)
		return
	} else if printVersion {
		versioninfo.Print()
		exit(0)
	}

	// New zap logger
	err = logutil.SetupLogger(cfg.Log, &cfg.Logger, &cfg.LogProps, cfg.Security.RedactInfoLog)
	if err == nil {
		log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
	} else {
		log.Fatal("initialize logger error", errs.ZapError(err))
	}
	// Flushing any buffered log entries
	defer log.Sync()

	versioninfo.Log("TSO")
	log.Info("TSO Config", zap.Reflect("config", cfg))

	grpcprometheus.EnableHandlingTimeHistogram()

	metricutil.Push(&cfg.Metric)

	// TODO: Create the server
	ctx, cancel := context.WithCancel(context.Background())
	svr := &Server{}
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var sig os.Signal
	go func() {
		sig = <-sc
		cancel()
	}()

	if err := svr.Run(); err != nil {
		log.Fatal("run server failed", errs.ZapError(err))
	}

	<-ctx.Done()
	log.Info("Got signal to exit", zap.String("signal", sig.String()))

	svr.Close()
	switch sig {
	case syscall.SIGTERM:
		exit(0)
	default:
		exit(1)
	}
}

func exit(code int) {
	log.Sync()
	os.Exit(code)
}
