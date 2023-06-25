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
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strconv"
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
	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/memberutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Server is the resource manager server, and it implements bs.Server.
type Server struct {
	// Server state. 0 is not running, 1 is running.
	isRunning int64

	ctx              context.Context
	serverLoopCtx    context.Context
	serverLoopCancel func()
	serverLoopWg     sync.WaitGroup

	cfg       *Config
	clusterID uint64
	name      string
	listenURL *url.URL

	// for the primary election of resource manager
	participant *member.Participant
	etcdClient  *clientv3.Client
	httpClient  *http.Client

	muxListener net.Listener
	service     *Service

	// Callback functions for different stages
	// startCallbacks will be called after the server is started.
	startCallbacks []func()
	// primaryCallbacks will be called after the server becomes leader.
	primaryCallbacks []func(context.Context)

	serviceRegister *discovery.ServiceRegister
}

// Name returns the unique etcd name for this server in etcd cluster.
func (s *Server) Name() string {
	return s.name
}

// Context returns the context.
func (s *Server) Context() context.Context {
	return s.ctx
}

// GetAddr returns the server address.
func (s *Server) GetAddr() string {
	return s.cfg.ListenAddr
}

// Run runs the Resource Manager server.
func (s *Server) Run() (err error) {
	if err = s.initClient(); err != nil {
		return err
	}
	if err = s.startServer(); err != nil {
		return err
	}

	s.startServerLoop()

	return nil
}

func (s *Server) startServerLoop() {
	s.serverLoopCtx, s.serverLoopCancel = context.WithCancel(s.ctx)
	s.serverLoopWg.Add(1)
	go s.primaryElectionLoop()
}

func (s *Server) primaryElectionLoop() {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	for {
		if s.IsClosed() {
			log.Info("server is closed, exit resource manager primary election loop")
			return
		}

		primary, checkAgain := s.participant.CheckLeader()
		if checkAgain {
			continue
		}
		if primary != nil {
			log.Info("start to watch the primary", zap.Stringer("resource-manager-primary", primary))
			// Watch will keep looping and never return unless the primary/leader has changed.
			primary.Watch(s.serverLoopCtx)
			log.Info("the resource manager primary has changed, try to re-campaign a primary")
		}

		s.campaignLeader()
	}
}

func (s *Server) campaignLeader() {
	log.Info("start to campaign the primary/leader", zap.String("campaign-resource-manager-primary-name", s.participant.Name()))
	if err := s.participant.CampaignLeader(s.cfg.LeaderLease); err != nil {
		if err.Error() == errs.ErrEtcdTxnConflict.Error() {
			log.Info("campaign resource manager primary meets error due to txn conflict, another server may campaign successfully",
				zap.String("campaign-resource-manager-primary-name", s.participant.Name()))
		} else {
			log.Error("campaign resource manager primary meets error due to etcd error",
				zap.String("campaign-resource-manager-primary-name", s.participant.Name()),
				errs.ZapError(err))
		}
		return
	}

	// Start keepalive the leadership and enable Resource Manager service.
	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	var resetLeaderOnce sync.Once
	defer resetLeaderOnce.Do(func() {
		cancel()
		s.participant.ResetLeader()
	})

	// maintain the leadership, after this, Resource Manager could be ready to provide service.
	s.participant.KeepLeader(ctx)
	log.Info("campaign resource manager primary ok", zap.String("campaign-resource-manager-primary-name", s.participant.Name()))

	log.Info("triggering the primary callback functions")
	for _, cb := range s.primaryCallbacks {
		cb(ctx)
	}

	s.participant.EnableLeader()
	log.Info("resource manager primary is ready to serve", zap.String("resource-manager-primary-name", s.participant.Name()))

	leaderTicker := time.NewTicker(utils.LeaderTickInterval)
	defer leaderTicker.Stop()

	for {
		select {
		case <-leaderTicker.C:
			if !s.participant.IsLeader() {
				log.Info("no longer a primary/leader because lease has expired, the resource manager primary/leader will step down")
				return
			}
		case <-ctx.Done():
			// Server is closed and it should return nil.
			log.Info("server is closed")
			return
		}
	}
}

// Close closes the server.
func (s *Server) Close() {
	if !atomic.CompareAndSwapInt64(&s.isRunning, 1, 0) {
		// server is already closed
		return
	}

	log.Info("closing resource manager server ...")
	s.serviceRegister.Deregister()
	s.muxListener.Close()
	s.serverLoopCancel()
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

// GetControllerConfig returns the controller config.
func (s *Server) GetControllerConfig() *ControllerConfig {
	return &s.cfg.Controller
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
	return !s.IsClosed() && s.participant.IsLeader()
}

// IsClosed checks if the server loop is closed
func (s *Server) IsClosed() bool {
	return s != nil && atomic.LoadInt64(&s.isRunning) == 0
}

// AddServiceReadyCallback adds callbacks when the server becomes the leader, if there is embedded etcd, or the primary otherwise.
func (s *Server) AddServiceReadyCallback(callbacks ...func(context.Context)) {
	s.primaryCallbacks = append(s.primaryCallbacks, callbacks...)
}

func (s *Server) initClient() error {
	tlsConfig, err := s.cfg.Security.ToTLSConfig()
	if err != nil {
		return err
	}
	u, err := types.NewURLs(strings.Split(s.cfg.BackendEndpoints, ","))
	if err != nil {
		return err
	}
	s.etcdClient, s.httpClient, err = etcdutil.CreateClients(tlsConfig, []url.URL(u))
	return err
}

func (s *Server) startGRPCServer(l net.Listener) {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	gs := grpc.NewServer()
	s.service.RegisterGRPCService(gs)
	err := gs.Serve(l)
	log.Info("gRPC server stop serving")

	// Attempt graceful stop (waits for pending RPCs), but force a stop if
	// it doesn't happen in a reasonable amount of time.
	done := make(chan struct{})
	go func() {
		defer logutil.LogPanic()
		log.Info("try to gracefully stop the server now")
		gs.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(utils.DefaultGRPCGracefulStopTimeout):
		log.Info("stopping grpc gracefully is taking longer than expected and force stopping now", zap.Duration("default", utils.DefaultGRPCGracefulStopTimeout))
		gs.Stop()
	}
	if s.IsClosed() {
		log.Info("grpc server stopped")
	} else {
		log.Fatal("grpc server stopped unexpectedly", errs.ZapError(err))
	}
}

func (s *Server) startHTTPServer(l net.Listener) {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	handler, _ := SetUpRestHandler(s.service)
	hs := &http.Server{
		Handler:           handler,
		ReadTimeout:       5 * time.Minute,
		ReadHeaderTimeout: 5 * time.Second,
	}
	err := hs.Serve(l)
	log.Info("http server stop serving")

	ctx, cancel := context.WithTimeout(context.Background(), utils.DefaultHTTPGracefulShutdownTimeout)
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
	defer logutil.LogPanic()
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

// GetLeaderListenUrls gets service endpoints from the leader in election group.
func (s *Server) GetLeaderListenUrls() []string {
	return s.participant.GetLeaderListenUrls()
}

func (s *Server) startServer() (err error) {
	if s.clusterID, err = utils.InitClusterID(s.ctx, s.etcdClient); err != nil {
		return err
	}
	log.Info("init cluster id", zap.Uint64("cluster-id", s.clusterID))
	// The independent Resource Manager service still reuses PD version info since PD and Resource Manager are just
	// different service modes provided by the same pd-server binary
	serverInfo.WithLabelValues(versioninfo.PDReleaseVersion, versioninfo.PDGitHash).Set(float64(time.Now().Unix()))

	uniqueName := s.cfg.ListenAddr
	uniqueID := memberutil.GenerateUniqueID(uniqueName)
	log.Info("joining primary election", zap.String("participant-name", uniqueName), zap.Uint64("participant-id", uniqueID))
	resourceManagerPrimaryPrefix := fmt.Sprintf("/ms/%d/resource_manager", s.clusterID)
	s.participant = member.NewParticipant(s.etcdClient)
	s.participant.InitInfo(uniqueName, uniqueID, path.Join(resourceManagerPrimaryPrefix, fmt.Sprintf("%05d", 0)),
		"primary", "keyspace group primary election", s.cfg.AdvertiseListenAddr)

	s.service = &Service{
		ctx:     s.ctx,
		manager: NewManager[*Server](s),
	}

	tlsConfig, err := s.cfg.Security.ToTLSConfig()
	if err != nil {
		return err
	}
	s.listenURL, err = url.Parse(s.cfg.ListenAddr)
	if err != nil {
		return err
	}
	if tlsConfig != nil {
		s.muxListener, err = tls.Listen(utils.TCPNetworkStr, s.listenURL.Host, tlsConfig)
	} else {
		s.muxListener, err = net.Listen(utils.TCPNetworkStr, s.listenURL.Host)
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

	// Server has started.
	entry := &discovery.ServiceRegistryEntry{ServiceAddr: s.cfg.AdvertiseListenAddr}
	serializedEntry, err := entry.Serialize()
	if err != nil {
		return err
	}
	s.serviceRegister = discovery.NewServiceRegister(s.ctx, s.etcdClient, strconv.FormatUint(s.clusterID, 10),
		utils.ResourceManagerServiceName, s.cfg.AdvertiseListenAddr, serializedEntry, discovery.DefaultLeaseInSeconds)
	if err := s.serviceRegister.Register(); err != nil {
		log.Error("failed to register the service", zap.String("service-name", utils.ResourceManagerServiceName), errs.ZapError(err))
		return err
	}
	atomic.StoreInt64(&s.isRunning, 1)
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
	log.Info("got signal to exit", zap.String("signal", sig.String()))

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
