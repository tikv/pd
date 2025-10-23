// Copyright 2025 TiKV Project Authors.
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
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/log"
	"github.com/pingcap/sysutil"

	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/mcs/router/server/config"
	"github.com/tikv/pd/pkg/mcs/router/server/meta"
	"github.com/tikv/pd/pkg/mcs/server"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"github.com/tikv/pd/pkg/versioninfo"
)

var _ bs.Server = (*Server)(nil)

const (
	serviceName = "Router Service"
)

// Server is the router server, and it implements bs.Server.
type Server struct {
	*server.BaseServer
	diagnosticspb.DiagnosticsServer

	// Server state. 0 is not running, 1 is running.
	isRunning int64

	serverLoopCtx    context.Context
	serverLoopCancel func()
	serverLoopWg     sync.WaitGroup

	cfg          *config.Config
	basicCluster *core.BasicCluster

	service *Service

	// for service registry
	serviceID       *discovery.ServiceRegistryEntry
	serviceRegister *discovery.ServiceRegister
	cluster         *Cluster

	regionSyncer *RegionSyncer

	metaWatcher *meta.Watcher
}

// Name returns the unique name for this server in the router cluster.
func (s *Server) Name() string {
	return s.cfg.Name
}

// GetAddr returns the server address.
func (s *Server) GetAddr() string {
	return s.cfg.ListenAddr
}

// GetAdvertiseListenAddr returns the advertise address of the server.
func (s *Server) GetAdvertiseListenAddr() string {
	return s.cfg.AdvertiseListenAddr
}

// GetBackendEndpoints returns the backend endpoints.
func (s *Server) GetBackendEndpoints() string {
	return s.cfg.BackendEndpoints
}

// SetLogLevel sets log level.
func (s *Server) SetLogLevel(level string) error {
	if !logutil.IsLevelLegal(level) {
		return errors.Errorf("log level %s is illegal", level)
	}
	s.cfg.Log.Level = level
	log.SetLevel(logutil.StringToZapLogLevel(level))
	log.Warn("log level changed", zap.String("level", log.GetLevel().String()))
	return nil
}

// Run runs the router server.
func (s *Server) Run() (err error) {
	if err = utils.InitClient(s); err != nil {
		return err
	}

	if s.serviceID, s.serviceRegister, err = utils.Register(s, constant.RouterServiceName); err != nil {
		return err
	}

	return s.startServer()
}

func (s *Server) startServerLoop() {
	s.serverLoopCtx, s.serverLoopCancel = context.WithCancel(s.Context())
	s.regionSyncer = NewRegionSyncer(s.serverLoopCtx, s.basicCluster, s.GetEtcdClient(), s.GetTLSConfig(), s.Name(),
		s.GetAdvertiseListenAddr())
	go s.regionSyncer.syncLoop()
	go s.regionSyncer.updatePDMemberLoop()
	s.regionSyncer.startSyncWithLeader()
}

// Close closes the server.
func (s *Server) Close() {
	s.stopCluster()
	if !atomic.CompareAndSwapInt64(&s.isRunning, 1, 0) {
		// server is already closed
		return
	}

	log.Info("closing router server ...")
	if err := s.serviceRegister.Deregister(); err != nil {
		log.Error("failed to deregister the service", errs.ZapError(err))
	}
	utils.StopHTTPServer(s)
	utils.StopGRPCServer(s)
	if err := s.GetListener().Close(); err != nil {
		log.Error("close listener meet error", errs.ZapError(err))
	}
	s.CloseClientConns()
	s.serverLoopWg.Wait()

	if s.GetClient() != nil {
		if err := s.GetClient().Close(); err != nil {
			log.Error("close etcd client meet error", errs.ZapError(errs.ErrCloseEtcdClient, err))
		}
	}

	if s.GetHTTPClient() != nil {
		s.GetHTTPClient().CloseIdleConnections()
	}
	log.Info("router server is closed")
}

// IsClosed checks if the server loop is closed
func (s *Server) IsClosed() bool {
	return s != nil && atomic.LoadInt64(&s.isRunning) == 0
}

// GetTLSConfig gets the security config.
func (s *Server) GetTLSConfig() *grpcutil.TLSConfig {
	return &s.cfg.Security.TLSConfig
}

// GetCluster returns the cluster.
func (s *Server) GetCluster() *Cluster {
	return s.cluster
}

// GetBasicCluster returns the basic cluster.
func (s *Server) GetBasicCluster() *core.BasicCluster {
	return s.basicCluster
}

// IsReady returns whether the server is ready.
func (s *Server) IsReady() bool {
	return s.regionSyncer.streamingRunning.Load()
}

// AddServiceReadyCallback adds a callback function that will be called when the service is ready.
func (*Server) AddServiceReadyCallback(...func(context.Context) error) {}

// IsServing returns whether the server is serving.
// TODO: implement it.
func (*Server) IsServing() bool { return false }

// ServerLoopWgDone decreases the server loop wait group.
func (s *Server) ServerLoopWgDone() {
	s.serverLoopWg.Done()
}

// ServerLoopWgAdd increases the server loop wait group.
func (s *Server) ServerLoopWgAdd(n int) {
	s.serverLoopWg.Add(n)
}

// SetUpRestHandler sets up the REST handler.
func (s *Server) SetUpRestHandler() (http.Handler, apiutil.APIServiceGroup) {
	return SetUpRestHandler(s.service)
}

// RegisterGRPCService registers the grpc service.
func (s *Server) RegisterGRPCService(grpcServer *grpc.Server) {
	s.service.RegisterGRPCService(grpcServer)
}

// GetServingUrls gets service endpoints.
func (s *Server) GetServingUrls() []string {
	return []string{s.cfg.AdvertiseListenAddr}
}

func (s *Server) startServer() (err error) {
	// The independent router service still reuses PD version info since PD and router are just
	// different service modes provided by the same pd-server binary
	bs.ServerInfoGauge.WithLabelValues(versioninfo.PDReleaseVersion, versioninfo.PDGitHash).Set(float64(time.Now().Unix()))
	bs.ServerMaxProcsGauge.Set(float64(runtime.GOMAXPROCS(0)))

	s.service = &Service{Server: s}
	if err := s.InitListener(s.GetTLSConfig(), s.cfg.GetListenAddr()); err != nil {
		return err
	}

	serverReadyChan := make(chan struct{})
	defer close(serverReadyChan)
	if err := s.startCluster(); err != nil {
		return err
	}
	s.startServerLoop()
	s.serverLoopWg.Add(1)
	go utils.StartGRPCAndHTTPServers(s, serverReadyChan, s.GetListener())
	<-serverReadyChan

	// Run callbacks
	log.Info("triggering the start callback functions")
	for _, cb := range s.GetStartCallbacks() {
		cb()
	}

	atomic.StoreInt64(&s.isRunning, 1)
	return nil
}

func (s *Server) startCluster() (err error) {
	s.basicCluster = core.NewBasicCluster()
	s.metaWatcher, err = meta.NewWatcher(s.Context(), s.GetClient(), s.basicCluster)
	if err != nil {
		return err
	}
	s.cluster = NewCluster(s.Context(), s.basicCluster)
	return nil
}

func (s *Server) stopCluster() {
	if s.serverLoopCancel != nil {
		s.serverLoopCancel()
	}
	s.regionSyncer.Stop()
	s.metaWatcher.Close()
}

// CreateServer creates the Server
func CreateServer(ctx context.Context, cfg *config.Config) *Server {
	svr := &Server{
		BaseServer:        server.NewBaseServer(ctx),
		DiagnosticsServer: sysutil.NewDiagnosticsServer(cfg.Log.File.Filename),
		cfg:               cfg,
	}
	return svr
}

// CreateServerWrapper encapsulates the configuration/log/metrics initialization and create the server
func CreateServerWrapper(cmd *cobra.Command, args []string) {
	schedulers.Register()
	err := cmd.Flags().Parse(args)
	if err != nil {
		cmd.Println(err)
		return
	}
	cfg := config.NewConfig()
	flagSet := cmd.Flags()
	err = cfg.Parse(flagSet)
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
		utils.Exit(0)
	}

	// New zap logger
	err = logutil.SetupLogger(&cfg.Log, &cfg.Logger, &cfg.LogProps, cfg.Security.RedactInfoLog)
	if err == nil {
		log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
	} else {
		log.Fatal("initialize logger error", errs.ZapError(err))
	}
	// Flushing any buffered log entries
	log.Sync()

	versioninfo.Log(serviceName)
	log.Info("router service config", zap.Reflect("config", cfg))

	grpcprometheus.EnableHandlingTimeHistogram()
	ctx, cancel := context.WithCancel(context.Background())
	metricutil.Push(ctx, &cfg.Metric)

	svr := CreateServer(ctx, cfg)

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
	log.Info("got signal to exit", zap.String("signal", sig.String()))

	svr.Close()
	switch sig {
	case syscall.SIGTERM:
		utils.Exit(0)
	default:
		utils.Exit(1)
	}
}
