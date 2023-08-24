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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/log"
	"github.com/pingcap/sysutil"
	"github.com/soheilhy/cmux"
	"github.com/spf13/cobra"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	"github.com/tikv/pd/pkg/mcs/scheduling/server/rule"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/schedule"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/memberutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Server is the scheduling server, and it implements bs.Server.
type Server struct {
	diagnosticspb.DiagnosticsServer

	// Server state. 0 is not running, 1 is running.
	isRunning int64
	// Server start timestamp
	startTimestamp int64

	ctx              context.Context
	serverLoopCtx    context.Context
	serverLoopCancel func()
	serverLoopWg     sync.WaitGroup

	cfg           *config.Config
	name          string
	clusterID     uint64
	listenURL     *url.URL
	backendUrls   []url.URL
	persistConfig *config.PersistConfig

	// etcd client
	etcdClient *clientv3.Client
	// http client
	httpClient *http.Client

	// Store as map[string]*grpc.ClientConn
	clientConns sync.Map

	// for the primary election of scheduling
	participant *member.Participant

	secure       bool
	muxListener  net.Listener
	httpListener net.Listener
	grpcServer   *grpc.Server
	httpServer   *http.Server
	service      *Service

	// Callback functions for different stages
	// startCallbacks will be called after the server is started.
	startCallbacks []func()
	// primaryCallbacks will be called after the server becomes leader.
	primaryCallbacks []func(context.Context)

	// for service registry
	serviceID       *discovery.ServiceRegistryEntry
	serviceRegister *discovery.ServiceRegister

	cluster   *Cluster
	hbStreams *hbstream.HeartbeatStreams
	storage   *endpoint.StorageEndpoint

	// for watching the PD API server meta info updates that are related to the scheduling.
	configWatcher *config.Watcher
	ruleWatcher   *rule.Watcher
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

// GetBackendEndpoints returns the backend endpoints.
func (s *Server) GetBackendEndpoints() string {
	return s.cfg.BackendEndpoints
}

// GetClientConns returns the client connections.
func (s *Server) GetClientConns() *sync.Map {
	return &s.clientConns
}

// Run runs the scheduling server.
func (s *Server) Run() error {
	skipWaitAPIServiceReady := false
	failpoint.Inject("skipWaitAPIServiceReady", func() {
		skipWaitAPIServiceReady = true
	})
	if !skipWaitAPIServiceReady {
		if err := utils.WaitAPIServiceReady(s); err != nil {
			return err
		}
	}

	if err := s.initClient(); err != nil {
		return err
	}
	return s.startServer()
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
		select {
		case <-s.serverLoopCtx.Done():
			log.Info("server is closed, exit resource manager primary election loop")
			return
		default:
		}

		primary, checkAgain := s.participant.CheckLeader()
		if checkAgain {
			continue
		}
		if primary != nil {
			log.Info("start to watch the primary", zap.Stringer("scheduling-primary", primary))
			// Watch will keep looping and never return unless the primary/leader has changed.
			primary.Watch(s.serverLoopCtx)
			log.Info("the scheduling primary has changed, try to re-campaign a primary")
		}

		s.campaignLeader()
	}
}

func (s *Server) campaignLeader() {
	log.Info("start to campaign the primary/leader", zap.String("campaign-scheduling-primary-name", s.participant.Name()))
	if err := s.participant.CampaignLeader(s.cfg.LeaderLease); err != nil {
		if err.Error() == errs.ErrEtcdTxnConflict.Error() {
			log.Info("campaign scheduling primary meets error due to txn conflict, another server may campaign successfully",
				zap.String("campaign-scheduling-primary-name", s.participant.Name()))
		} else {
			log.Error("campaign scheduling primary meets error due to etcd error",
				zap.String("campaign-scheduling-primary-name", s.participant.Name()),
				errs.ZapError(err))
		}
		return
	}

	// Start keepalive the leadership and enable Scheduling service.
	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	var resetLeaderOnce sync.Once
	defer resetLeaderOnce.Do(func() {
		cancel()
		s.participant.ResetLeader()
	})

	// maintain the leadership, after this, Scheduling could be ready to provide service.
	s.participant.KeepLeader(ctx)
	log.Info("campaign scheduling primary ok", zap.String("campaign-scheduling-primary-name", s.participant.Name()))

	log.Info("triggering the primary callback functions")
	for _, cb := range s.primaryCallbacks {
		cb(ctx)
	}

	s.participant.EnableLeader()
	log.Info("scheduling primary is ready to serve", zap.String("scheduling-primary-name", s.participant.Name()))

	leaderTicker := time.NewTicker(utils.LeaderTickInterval)
	defer leaderTicker.Stop()

	for {
		select {
		case <-leaderTicker.C:
			if !s.participant.IsLeader() {
				log.Info("no longer a primary/leader because lease has expired, the scheduling primary/leader will step down")
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

	log.Info("closing scheduling server ...")
	s.serviceRegister.Deregister()
	s.stopHTTPServer()
	s.stopGRPCServer()
	s.muxListener.Close()
	s.GetCoordinator().Stop()
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
	log.Info("scheduling server is closed")
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

// GetTLSConfig gets the security config.
func (s *Server) GetTLSConfig() *grpcutil.TLSConfig {
	return &s.cfg.Security.TLSConfig
}

// GetCluster returns the cluster.
func (s *Server) GetCluster() *Cluster {
	return s.cluster
}

// GetCoordinator returns the coordinator.
func (s *Server) GetCoordinator() *schedule.Coordinator {
	return s.GetCluster().GetCoordinator()
}

func (s *Server) initClient() error {
	tlsConfig, err := s.cfg.Security.ToTLSConfig()
	if err != nil {
		return err
	}
	s.backendUrls, err = types.NewURLs(strings.Split(s.cfg.BackendEndpoints, ","))
	if err != nil {
		return err
	}
	s.etcdClient, s.httpClient, err = etcdutil.CreateClients(tlsConfig, s.backendUrls)
	return err
}

func (s *Server) startGRPCServer(l net.Listener) {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	log.Info("grpc server starts serving", zap.String("address", l.Addr().String()))
	err := s.grpcServer.Serve(l)
	if s.IsClosed() {
		log.Info("grpc server stopped")
	} else {
		log.Fatal("grpc server stopped unexpectedly", errs.ZapError(err))
	}
}

func (s *Server) startHTTPServer(l net.Listener) {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	log.Info("http server starts serving", zap.String("address", l.Addr().String()))
	err := s.httpServer.Serve(l)
	if s.IsClosed() {
		log.Info("http server stopped")
	} else {
		log.Fatal("http server stopped unexpectedly", errs.ZapError(err))
	}
}

func (s *Server) startGRPCAndHTTPServers(serverReadyChan chan<- struct{}, l net.Listener) {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	mux := cmux.New(l)
	// Don't hang on matcher after closing listener
	mux.SetReadTimeout(3 * time.Second)
	grpcL := mux.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	if s.secure {
		s.httpListener = mux.Match(cmux.Any())
	} else {
		s.httpListener = mux.Match(cmux.HTTP1())
	}

	s.grpcServer = grpc.NewServer()
	s.service.RegisterGRPCService(s.grpcServer)
	diagnosticspb.RegisterDiagnosticsServer(s.grpcServer, s)
	s.serverLoopWg.Add(1)
	go s.startGRPCServer(grpcL)

	handler, _ := SetUpRestHandler(s.service)
	s.httpServer = &http.Server{
		Handler:     handler,
		ReadTimeout: 3 * time.Second,
	}
	s.serverLoopWg.Add(1)
	go s.startHTTPServer(s.httpListener)

	serverReadyChan <- struct{}{}
	if err := mux.Serve(); err != nil {
		if s.IsClosed() {
			log.Info("mux stopped serving", errs.ZapError(err))
		} else {
			log.Fatal("mux stopped serving unexpectedly", errs.ZapError(err))
		}
	}
}

// GetLeaderListenUrls gets service endpoints from the leader in election group.
func (s *Server) GetLeaderListenUrls() []string {
	return s.participant.GetLeaderListenUrls()
}

func (s *Server) stopHTTPServer() {
	log.Info("stopping http server")
	defer log.Info("http server stopped")

	ctx, cancel := context.WithTimeout(context.Background(), utils.DefaultHTTPGracefulShutdownTimeout)
	defer cancel()

	// First, try to gracefully shutdown the http server
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		s.httpServer.Shutdown(ctx)
	}()

	select {
	case <-ch:
	case <-ctx.Done():
		// Took too long, manually close open transports
		log.Warn("http server graceful shutdown timeout, forcing close")
		s.httpServer.Close()
		// concurrent Graceful Shutdown should be interrupted
		<-ch
	}
}

func (s *Server) stopGRPCServer() {
	log.Info("stopping grpc server")
	defer log.Info("grpc server stopped")

	// Do not grpc.Server.GracefulStop with TLS enabled etcd server
	// See https://github.com/grpc/grpc-go/issues/1384#issuecomment-317124531
	// and https://github.com/etcd-io/etcd/issues/8916
	if s.secure {
		s.grpcServer.Stop()
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), utils.DefaultGRPCGracefulStopTimeout)
	defer cancel()

	// First, try to gracefully shutdown the grpc server
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		// Close listeners to stop accepting new connections,
		// will block on any existing transports
		s.grpcServer.GracefulStop()
	}()

	// Wait until all pending RPCs are finished
	select {
	case <-ch:
	case <-ctx.Done():
		// Took too long, manually close open transports
		// e.g. watch streams
		log.Warn("grpc server graceful shutdown timeout, forcing close")
		s.grpcServer.Stop()
		// concurrent GracefulStop should be interrupted
		<-ch
	}
}

func (s *Server) startServer() (err error) {
	if s.clusterID, err = utils.InitClusterID(s.ctx, s.etcdClient); err != nil {
		return err
	}
	log.Info("init cluster id", zap.Uint64("cluster-id", s.clusterID))
	// The independent Scheduling service still reuses PD version info since PD and Scheduling are just
	// different service modes provided by the same pd-server binary
	serverInfo.WithLabelValues(versioninfo.PDReleaseVersion, versioninfo.PDGitHash).Set(float64(time.Now().Unix()))

	uniqueName := s.cfg.ListenAddr
	uniqueID := memberutil.GenerateUniqueID(uniqueName)
	log.Info("joining primary election", zap.String("participant-name", uniqueName), zap.Uint64("participant-id", uniqueID))
	schedulingPrimaryPrefix := endpoint.SchedulingSvcRootPath(s.clusterID)
	s.participant = member.NewParticipant(s.etcdClient)
	s.participant.InitInfo(uniqueName, uniqueID, path.Join(schedulingPrimaryPrefix, fmt.Sprintf("%05d", 0)),
		utils.PrimaryKey, "primary election", s.cfg.AdvertiseListenAddr)
	s.storage = endpoint.NewStorageEndpoint(
		kv.NewEtcdKVBase(s.etcdClient, endpoint.PDRootPath(s.clusterID)), nil)
	basicCluster := core.NewBasicCluster()
	s.hbStreams = hbstream.NewHeartbeatStreams(s.ctx, s.clusterID, basicCluster)
	s.cluster, err = NewCluster(s.ctx, s.cfg, s.storage, basicCluster, s.hbStreams)
	if err != nil {
		return err
	}

	s.listenURL, err = url.Parse(s.cfg.ListenAddr)
	if err != nil {
		return err
	}
	s.service = &Service{Server: s}
	tlsConfig, err := s.cfg.Security.ToTLSConfig()
	if err != nil {
		return err
	}
	if tlsConfig != nil {
		s.secure = true
		s.muxListener, err = tls.Listen(utils.TCPNetworkStr, s.listenURL.Host, tlsConfig)
	} else {
		s.muxListener, err = net.Listen(utils.TCPNetworkStr, s.listenURL.Host)
	}
	if err != nil {
		return err
	}
	err = s.startWatcher()
	if err != nil {
		return err
	}

	go s.GetCoordinator().RunUntilStop()
	serverReadyChan := make(chan struct{})
	defer close(serverReadyChan)
	s.serverLoopWg.Add(1)
	go s.startGRPCAndHTTPServers(serverReadyChan, s.muxListener)
	<-serverReadyChan
	s.startServerLoop()

	// Run callbacks
	log.Info("triggering the start callback functions")
	for _, cb := range s.startCallbacks {
		cb()
	}

	// Server has started.
	serializedEntry, err := s.serviceID.Serialize()
	if err != nil {
		return err
	}
	s.serviceRegister = discovery.NewServiceRegister(s.ctx, s.etcdClient, strconv.FormatUint(s.clusterID, 10),
		utils.SchedulingServiceName, s.cfg.AdvertiseListenAddr, serializedEntry, discovery.DefaultLeaseInSeconds)
	if err := s.serviceRegister.Register(); err != nil {
		log.Error("failed to register the service", zap.String("service-name", utils.SchedulingServiceName), errs.ZapError(err))
		return err
	}
	atomic.StoreInt64(&s.isRunning, 1)
	return nil
}

func (s *Server) startWatcher() (err error) {
	s.configWatcher, err = config.NewWatcher(
		s.ctx, s.etcdClient, s.clusterID, s.persistConfig,
	)
	if err != nil {
		return err
	}
	s.ruleWatcher, err = rule.NewWatcher(
		s.ctx, s.etcdClient, s.clusterID,
	)
	return err
}

// CreateServer creates the Server
func CreateServer(ctx context.Context, cfg *config.Config) *Server {
	svr := &Server{
		DiagnosticsServer: sysutil.NewDiagnosticsServer(cfg.Log.File.Filename),
		startTimestamp:    time.Now().Unix(),
		cfg:               cfg,
		persistConfig:     config.NewPersistConfig(cfg),
		ctx:               ctx,
	}
	return svr
}

// CreateServerWrapper encapsulates the configuration/log/metrics initialization and create the server
func CreateServerWrapper(cmd *cobra.Command, args []string) {
	cmd.Flags().Parse(args)
	cfg := config.NewConfig()
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

	versioninfo.Log("Scheduling")
	log.Info("Scheduling config", zap.Reflect("config", cfg))

	grpcprometheus.EnableHandlingTimeHistogram()
	metricutil.Push(&cfg.Metric)

	ctx, cancel := context.WithCancel(context.Background())
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
		exit(0)
	default:
		exit(1)
	}
}

func exit(code int) {
	log.Sync()
	os.Exit(code)
}
