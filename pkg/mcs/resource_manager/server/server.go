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
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/soheilhy/cmux"
	"github.com/spf13/cobra"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const tcp = "tcp"

// Server is the resource manager server, and it implements bs.Server.
// nolint
type Server struct {
	cfg *Config
	// Server state. 0 is not serving, 1 is serving.
	isServing   int64
	ctx         context.Context
	name        string
	backendUrls []*url.URL

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
	return nil
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

// IsServing returns whether the server is the leader, if there is embedded etcd, or the primary otherwise.
func (s *Server) IsServing() bool {
	// TODO: implement this function with primary.
	return atomic.LoadInt64(&s.isServing) == 1
}

// AddServiceReadyCallback adds the callback function when the server becomes the leader, if there is embedded etcd, or the primary otherwise.
func (s *Server) AddServiceReadyCallback(callbacks ...func(context.Context)) {
	s.primaryCallbacks = append(s.primaryCallbacks, callbacks...)
}

func (s *Server) initClient() error {
	tlsConfig, err := s.cfg.Security.ToTLSConfig()
	if err != nil {
		return err
	}
	endpoints := strings.Split(s.cfg.BackendEndpoints, ",")
	for _, endpoint := range endpoints {
		e, err := url.Parse(endpoint)
		if err != nil {
			return err
		}
		s.backendUrls = append(s.backendUrls, e)
	}
	if len(s.backendUrls) == 0 {
		return errs.ErrURLParse.Wrap(errors.New("no backend url found"))
	}
	s.etcdClient, s.httpClient, err = etcdutil.CreateClients(tlsConfig, []url.URL{*s.backendUrls[0]})
	// TODO: support multi-endpoints.
	return err
}

func (s *Server) startServer() error {
	var mux cmux.CMux
	tlsConfig, err := s.cfg.Security.ToTLSConfig()
	if err != nil {
		return err
	}
	if tlsConfig != nil {
		l, err := tls.Listen(tcp, s.cfg.ListenAddr, tlsConfig)
		if err != nil {
			return err
		}
		mux = cmux.New(l)
	} else {
		l, err := net.Listen(tcp, s.cfg.ListenAddr)
		if err != nil {
			return err
		}
		mux = cmux.New(l)
	}

	grpcL := mux.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	httpL := mux.Match(cmux.Any())

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

	s.serverLoopWg.Add(2)
	go func() {
		defer s.serverLoopWg.Done()
		s.grpcServer.Serve(grpcL)
	}()
	go func() {
		defer s.serverLoopWg.Done()
		s.httpServer.Serve(httpL)
	}()

	// Run callbacks
	log.Info("triggering the start callback functions")
	for _, cb := range s.startCallbacks {
		cb()
	}
	// Server has started.
	atomic.StoreInt64(&s.isServing, 1)
	return mux.Serve()
}

// NewServer creates a new resource manager server.
func NewServer(ctx context.Context, cfg *Config, name string) *Server {
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

	ctx, cancel := context.WithCancel(context.Background())
	svr := NewServer(ctx, cfg, "ResourceManager")

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
