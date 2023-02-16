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
	"flag"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/soheilhy/cmux"
	"github.com/spf13/cobra"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Server is the resource manager server, and it implements bs.Server.
// nolint
type Server struct {
	cfg *Config
	mux cmux.CMux
	// Server state. 0 is not serving, 1 is serving.
	isServing int64
	ctx       context.Context
	name      string

	etcdClient *clientv3.Client
	httpClient *http.Client

	grpcServer *grpc.Server
	httpServer *http.Server
	service    *Service

	serverLoopWg sync.WaitGroup
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
	if err := s.startServer(); err != nil {
		return err
	}
	return s.mux.Serve()
}

// Close closes the server.
func (s *Server) Close() {
	if !atomic.CompareAndSwapInt64(&s.isServing, 1, 0) {
		// server is already closed
		return
	}

	log.Info("closing server")

	s.grpcServer.Stop()
	s.httpServer.Shutdown(s.ctx)
	s.serverLoopWg.Wait()

	if s.etcdClient != nil {
		if err := s.etcdClient.Close(); err != nil {
			log.Error("close etcd client meet error", errs.ZapError(errs.ErrCloseEtcdClient, err))
		}
	}

	if s.httpClient != nil {
		s.httpClient.CloseIdleConnections()
	}
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

// IsPrimary returns whether the server is primary, if there is etcd server in the server, it will return whether it is leader.
func (s *Server) IsPrimary() bool {
	// TODO: implement this
	return true
}

// AddPrimaryCallback adds a callback when the server becomes primary, if there is etcd server in the server, it means the server becomes leader.
func (s *Server) AddPrimaryCallback(callbacks ...func(context.Context)) {
	s.primaryCallbacks = append(s.primaryCallbacks, callbacks...)
}

func (s *Server) initClient() error {
	tlsConfig, err := s.cfg.Security.ToTLSConfig()
	if err != nil {
		return err
	}
	endpoint, err := url.Parse(s.cfg.BackendEndpoints)
	if err != nil {
		return err
	}
	s.etcdClient, s.httpClient, err = etcdutil.CreateClients(tlsConfig, []url.URL{*endpoint})
	return err
}

func (s *Server) startServer() error {
	tlsConfig, err := s.cfg.Security.ToTLSConfig()
	if err != nil {
		return err
	}
	if tlsConfig != nil {
		l, err := tls.Listen("tcp", s.cfg.ListenAddr, tlsConfig)
		if err != nil {
			return err
		}
		s.mux = cmux.New(l)
	} else {
		l, err := net.Listen("tcp", s.cfg.ListenAddr)
		if err != nil {
			return err
		}
		s.mux = cmux.New(l)
	}

	grpcL := s.mux.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	httpL := s.mux.Match(cmux.Any())

	manager := NewManager(s)
	s.service = &Service{
		ctx:     s.ctx,
		manager: manager,
	}

	s.grpcServer = grpc.NewServer()
	s.service.RegisterGRPCService(s.grpcServer)

	handler, _ := SetUpRestHandler(s.service)
	s.httpServer = &http.Server{
		Handler:           handler,
		ReadTimeout:       5 * time.Minute,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		defer s.serverLoopWg.Done()
		s.grpcServer.Serve(grpcL)
	}()
	go func() {
		defer s.serverLoopWg.Done()
		s.httpServer.Serve(httpL)
	}()
	s.serverLoopWg.Add(2)

	// Run callbacks
	log.Info("triggering the start callback functions")
	for _, cb := range s.startCallbacks {
		cb()
	}
	// Server has started.
	atomic.StoreInt64(&s.isServing, 1)
	return nil
}

// NewServer creates a new resource manager server.
func NewServer(name string, ctx context.Context, cfg *Config) *Server {
	return &Server{
		name: name,
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
	if err != nil {
		cmd.Println(err)
		return
	}

	printVersion, err := flagSet.GetBool("version")
	if err != nil {
		cmd.Println(err)
		return
	}
	if printVersion {
		// TODO: support printing resource manager server info
		// server.PrintPDInfo()
		exit(0)
	}

	defer logutil.LogPanic()

	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		exit(0)
	default:
		log.Fatal("parse cmd flags error", errs.ZapError(err))
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

	// TODO: support printing resource manager server info
	// server.LogPDInfo()

	grpcprometheus.EnableHandlingTimeHistogram()

	metricutil.Push(&cfg.Metric)

	ctx, cancel := context.WithCancel(context.Background())
	svr := NewServer("ResourceManager", ctx, cfg)

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
