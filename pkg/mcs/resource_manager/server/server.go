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
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pingcap/log"
	"github.com/soheilhy/cmux"
	"github.com/spf13/cobra"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	tcp = "tcp"
	// defaultGRPCGracefulStopTimeout is the default timeout to wait for grpc server to gracefully stop
	defaultGRPCGracefulStopTimeout = 5 * time.Second
	// defaultHTTPGracefulShutdownTimeout is the default timeout to wait for http server to gracefully shutdown
	defaultHTTPGracefulShutdownTimeout = 5 * time.Second
)

// Server is the resource manager server, and it implements bs.Server.
// nolint
type Server struct {
	// Server state. 0 is not serving, 1 is serving.
	isServing int64

	ctx          context.Context
	serverLoopWg sync.WaitGroup

	cfg         *Config
	name        string
	backendUrls []url.URL

	etcdClient *clientv3.Client
	httpClient *http.Client

	muxListener net.Listener
	service     *Service

	// Callback functions for different stages
	// startCallbacks will be called after the server is started.
	startCallbacks []func()
	// primaryCallbacks will be called after the server becomes leader.
	primaryCallbacks []func(context.Context)
}

// Name returns the unique etcd Name for this server in etcd cluster.
func (s *Server) Name() string {
	return s.name
}

// Context returns the context.
func (s *Server) Context() context.Context {
	return s.ctx
}

// Run runs the pd server.
func (s *Server) Run() error {
	if err := s.initClient(); err != nil {
		return err
	}
	return s.startServer()
}

// Close closes the server.
func (s *Server) Close() {
	if !atomic.CompareAndSwapInt64(&s.isServing, 1, 0) {
		// server is already closed
		return
	}

	log.Info("closing resource manager server ...")
	s.muxListener.Close()
	s.serverLoopWg.Wait()

	if s.etcdClient != nil {
		if err := s.etcdClient.Close(); err != nil {
			log.Error("close etcd client meet error", errs.ZapError(errs.ErrCloseEtcdClient, err))
		}
	}

	if s.httpClient != nil {
		s.httpClient.CloseIdleConnections()
	}

	log.Info("resource manager server is closed")
}

// GetClient returns builtin etcd client.
func (s *Server) GetClient() *clientv3.Client {
	return s.etcdClient
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
	// TODO: implement this function with primary.
	return atomic.LoadInt64(&s.isServing) == 1
}

// IsClosed checks if the server loop is closed
func (s *Server) IsClosed() bool {
	return atomic.LoadInt64(&s.isServing) == 0
}

// AddServiceReadyCallback adds the callback function when the server becomes the leader, if there is embedded etcd, or the primary otherwise.
func (s *Server) AddServiceReadyCallback(callbacks ...func(context.Context)) {
	s.primaryCallbacks = append(s.primaryCallbacks, callbacks...)
}

func (s *Server) initClient() error {
	// TODO: We need to keep all backend endpoints and keep updating them to the latest. Once one of them failed, need to try another one.
	tlsConfig, err := s.cfg.Security.ToTLSConfig()
	if err != nil {
		return err
	}
	u, err := types.NewURLs(strings.Split(s.cfg.BackendEndpoints, ","))
	if err != nil {
		return err
	}
	s.backendUrls = []url.URL(u)
	s.etcdClient, s.httpClient, err = etcdutil.CreateClients(tlsConfig, s.backendUrls)
	return err
}

func (s *Server) startGRPCServer(l net.Listener) {
	defer s.serverLoopWg.Done()

	gs := grpc.NewServer()
	s.service.RegisterGRPCService(gs)
	err := gs.Serve(l)
	log.Info("gRPC server stop serving")

	// Attempt graceful stop (waits for pending RPCs), but force a stop if
	// it doesn't happen in a reasonable amount of time.
	done := make(chan struct{})
	go func() {
		log.Info("try to gracefully stop the server now")
		gs.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(defaultGRPCGracefulStopTimeout):
		log.Info("stopping grpc gracefully is taking longer than expected and force stopping now", zap.Duration("default", defaultGRPCGracefulStopTimeout))
		gs.Stop()
	}
	if s.IsClosed() {
		log.Info("grpc server stopped")
	} else {
		log.Fatal("grpc server stopped unexpectedly", errs.ZapError(err))
	}
}

func (s *Server) startHTTPServer(l net.Listener) {
	defer s.serverLoopWg.Done()

	handler, _ := SetUpRestHandler(s.service)
	hs := &http.Server{
		Handler:           handler,
		ReadTimeout:       5 * time.Minute,
		ReadHeaderTimeout: 5 * time.Second,
	}
	err := hs.Serve(l)
	log.Info("http server stop serving")

	ctx, cancel := context.WithTimeout(context.Background(), defaultHTTPGracefulShutdownTimeout)
	defer cancel()
	if err := hs.Shutdown(ctx); err != nil {
		log.Error("http server shutdown encountered problem", errs.ZapError(err))
	} else {
		log.Info("all http(s) requests finished")
	}
	if s.IsClosed() {
		log.Info("http server stopped")
	} else {
		log.Fatal("http server stopped unexpectedly", errs.ZapError(err))
	}
}

func (s *Server) startGRPCAndHTTPServers(l net.Listener) {
	defer s.serverLoopWg.Done()

	mux := cmux.New(l)
	grpcL := mux.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	httpL := mux.Match(cmux.Any())

	s.serverLoopWg.Add(2)
	go s.startGRPCServer(grpcL)
	go s.startHTTPServer(httpL)

	if err := mux.Serve(); err != nil {
		if s.IsClosed() {
			log.Info("mux stop serving", errs.ZapError(err))
		} else {
			log.Fatal("mux stop serving unexpectedly", errs.ZapError(err))
		}
	}
}

func (s *Server) startServer() error {
	manager := NewManager(s)
	s.service = &Service{
		ctx:     s.ctx,
		manager: manager,
	}

	tlsConfig, err := s.cfg.Security.ToTLSConfig()
	if err != nil {
		return err
	}
	if tlsConfig != nil {
		s.muxListener, err = tls.Listen(tcp, s.cfg.ListenAddr, tlsConfig)
	} else {
		s.muxListener, err = net.Listen(tcp, s.cfg.ListenAddr)
	}
	if err != nil {
		return err
	}

	s.serverLoopWg.Add(1)
	go s.startGRPCAndHTTPServers(s.muxListener)

	// Run callbacks
	log.Info("triggering the start callback functions")
	for _, cb := range s.startCallbacks {
		cb()
	}
	// TODO: resolve callback for the primary
	for _, cb := range s.primaryCallbacks {
		cb(s.ctx)
	}

	// Server has started.
	atomic.StoreInt64(&s.isServing, 1)
	return nil
}

// NewServer creates a new resource manager server.
func NewServer(ctx context.Context, cfg *Config) *Server {
	return &Server{
		name: cfg.Name,
		ctx:  ctx,
		cfg:  cfg,
	}
}

// CreateServerWrapper encapsulates the configuration/log/metrics initialization and create the server
func CreateServerWrapper(cmd *cobra.Command, args []string) {
	cmd.Flags().Parse(args)
	cfg := NewConfig()
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

	versioninfo.Log("resource manager")
	log.Info("resource manager config", zap.Reflect("config", cfg))

	grpcprometheus.EnableHandlingTimeHistogram()

	metricutil.Push(&cfg.Metric)

	ctx, cancel := context.WithCancel(context.Background())
	svr := NewServer(ctx, cfg)

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
