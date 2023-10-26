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
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/schedulingpb"
	"github.com/pingcap/log"
	"github.com/pingcap/sysutil"
	"github.com/spf13/cobra"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	"github.com/tikv/pd/pkg/mcs/scheduling/server/meta"
	"github.com/tikv/pd/pkg/mcs/scheduling/server/rule"
	"github.com/tikv/pd/pkg/mcs/server"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/schedule"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/memberutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var _ bs.Server = (*Server)(nil)

const (
	serviceName = "Scheduling Service"

	memberUpdateInterval = time.Minute
)

// Server is the scheduling server, and it implements bs.Server.
type Server struct {
	*server.BaseServer
	diagnosticspb.DiagnosticsServer

	// Server state. 0 is not running, 1 is running.
	isRunning int64

	serverLoopCtx    context.Context
	serverLoopCancel func()
	serverLoopWg     sync.WaitGroup

	cfg           *config.Config
	clusterID     uint64
	persistConfig *config.PersistConfig
	basicCluster  *core.BasicCluster

	// for the primary election of scheduling
	participant *member.Participant

	service           *Service
	checkMembershipCh chan struct{}

	// primaryCallbacks will be called after the server becomes leader.
	primaryCallbacks     []func(context.Context) error
	primaryExitCallbacks []func()

	// for service registry
	serviceID       *discovery.ServiceRegistryEntry
	serviceRegister *discovery.ServiceRegister

	cluster   *Cluster
	hbStreams *hbstream.HeartbeatStreams
	storage   *endpoint.StorageEndpoint

	// for watching the PD API server meta info updates that are related to the scheduling.
	configWatcher *config.Watcher
	ruleWatcher   *rule.Watcher
	metaWatcher   *meta.Watcher
}

// Name returns the unique name for this server in the scheduling cluster.
func (s *Server) Name() string {
	return s.cfg.Name
}

// GetAddr returns the server address.
func (s *Server) GetAddr() string {
	return s.cfg.ListenAddr
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

	if err := utils.InitClient(s); err != nil {
		return err
	}
	return s.startServer()
}

func (s *Server) startServerLoop() {
	s.serverLoopCtx, s.serverLoopCancel = context.WithCancel(s.Context())
	s.serverLoopWg.Add(2)
	go s.primaryElectionLoop()
	go s.updateAPIServerMemberLoop()
}

func (s *Server) updateAPIServerMemberLoop() {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()
	ticker := time.NewTicker(memberUpdateInterval)
	failpoint.Inject("fastUpdateMember", func() {
		ticker.Stop()
		ticker = time.NewTicker(100 * time.Millisecond)
	})
	defer ticker.Stop()
	var curLeader uint64
	for {
		select {
		case <-ctx.Done():
			log.Info("server is closed, exit update member loop")
			return
		case <-ticker.C:
		case <-s.checkMembershipCh:
		}
		if !s.IsServing() {
			continue
		}
		members, err := s.GetClient().MemberList(ctx)
		if err != nil {
			log.Warn("failed to list members", errs.ZapError(err))
			continue
		}
		for _, ep := range members.Members {
			status, err := s.GetClient().Status(ctx, ep.ClientURLs[0])
			if err != nil {
				log.Info("failed to get status of member", zap.String("member-id", fmt.Sprintf("%x", ep.ID)), zap.String("endpoint", ep.ClientURLs[0]), errs.ZapError(err))
				continue
			}
			if status.Leader == ep.ID {
				cc, err := s.GetDelegateClient(ctx, s.GetTLSConfig(), ep.ClientURLs[0])
				if err != nil {
					log.Info("failed to get delegate client", errs.ZapError(err))
				}
				if s.cluster.SwitchAPIServerLeader(pdpb.NewPDClient(cc)) {
					if status.Leader != curLeader {
						log.Info("switch leader", zap.String("leader-id", fmt.Sprintf("%x", ep.ID)), zap.String("endpoint", ep.ClientURLs[0]))
					}
					curLeader = ep.ID
					break
				}
			}
		}
	}
}

func (s *Server) primaryElectionLoop() {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	for {
		select {
		case <-s.serverLoopCtx.Done():
			log.Info("server is closed, exit primary election loop")
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
		member.ServiceMemberGauge.WithLabelValues(serviceName).Set(0)
	})

	// maintain the leadership, after this, Scheduling could be ready to provide service.
	s.participant.KeepLeader(ctx)
	log.Info("campaign scheduling primary ok", zap.String("campaign-scheduling-primary-name", s.participant.Name()))

	log.Info("triggering the primary callback functions")
	for _, cb := range s.primaryCallbacks {
		if err := cb(ctx); err != nil {
			log.Error("failed to trigger the primary callback functions", errs.ZapError(err))
			return
		}
	}
	defer func() {
		for _, cb := range s.primaryExitCallbacks {
			cb()
		}
	}()
	s.participant.EnableLeader()
	member.ServiceMemberGauge.WithLabelValues(serviceName).Set(1)
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
	utils.StopHTTPServer(s)
	utils.StopGRPCServer(s)
	s.GetListener().Close()
	s.serverLoopCancel()
	s.serverLoopWg.Wait()

	if s.GetClient() != nil {
		if err := s.GetClient().Close(); err != nil {
			log.Error("close etcd client meet error", errs.ZapError(errs.ErrCloseEtcdClient, err))
		}
	}

	if s.GetHTTPClient() != nil {
		s.GetHTTPClient().CloseIdleConnections()
	}
	log.Info("scheduling server is closed")
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
func (s *Server) AddServiceReadyCallback(callbacks ...func(context.Context) error) {
	s.primaryCallbacks = append(s.primaryCallbacks, callbacks...)
}

// AddServiceExitCallback adds callbacks when the server becomes the leader, if there is embedded etcd, or the primary otherwise.
func (s *Server) AddServiceExitCallback(callbacks ...func()) {
	s.primaryExitCallbacks = append(s.primaryExitCallbacks, callbacks...)
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

// GetCoordinator returns the coordinator.
func (s *Server) GetCoordinator() *schedule.Coordinator {
	c := s.GetCluster()
	if c == nil {
		return nil
	}
	return c.GetCoordinator()
}

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

// GetLeaderListenUrls gets service endpoints from the leader in election group.
func (s *Server) GetLeaderListenUrls() []string {
	return s.participant.GetLeaderListenUrls()
}

func (s *Server) startServer() (err error) {
	if s.clusterID, err = utils.InitClusterID(s.Context(), s.GetClient()); err != nil {
		return err
	}
	log.Info("init cluster id", zap.Uint64("cluster-id", s.clusterID))
	// The independent Scheduling service still reuses PD version info since PD and Scheduling are just
	// different service modes provided by the same pd-server binary
	serverInfo.WithLabelValues(versioninfo.PDReleaseVersion, versioninfo.PDGitHash).Set(float64(time.Now().Unix()))

	uniqueName := s.cfg.ListenAddr
	uniqueID := memberutil.GenerateUniqueID(uniqueName)
	log.Info("joining primary election", zap.String("participant-name", uniqueName), zap.Uint64("participant-id", uniqueID))
	s.participant = member.NewParticipant(s.GetClient(), utils.SchedulingServiceName)
	p := &schedulingpb.Participant{
		Name:       uniqueName,
		Id:         uniqueID, // id is unique among all participants
		ListenUrls: []string{s.cfg.AdvertiseListenAddr},
	}
	s.participant.InitInfo(p, endpoint.SchedulingSvcRootPath(s.clusterID), utils.PrimaryKey, "primary election")

	s.service = &Service{Server: s}
	s.AddServiceReadyCallback(s.startCluster)
	s.AddServiceExitCallback(s.stopCluster)
	if err := s.InitListener(s.GetTLSConfig(), s.cfg.ListenAddr); err != nil {
		return err
	}

	serverReadyChan := make(chan struct{})
	defer close(serverReadyChan)
	s.startServerLoop()
	s.serverLoopWg.Add(1)
	go utils.StartGRPCAndHTTPServers(s, serverReadyChan, s.GetListener())
	s.checkMembershipCh <- struct{}{}
	<-serverReadyChan

	// Run callbacks
	log.Info("triggering the start callback functions")
	for _, cb := range s.GetStartCallbacks() {
		cb()
	}

	// Server has started.
	serializedEntry, err := s.serviceID.Serialize()
	if err != nil {
		return err
	}
	s.serviceRegister = discovery.NewServiceRegister(s.Context(), s.GetClient(), strconv.FormatUint(s.clusterID, 10),
		utils.SchedulingServiceName, s.cfg.AdvertiseListenAddr, serializedEntry, discovery.DefaultLeaseInSeconds)
	if err := s.serviceRegister.Register(); err != nil {
		log.Error("failed to register the service", zap.String("service-name", utils.SchedulingServiceName), errs.ZapError(err))
		return err
	}
	atomic.StoreInt64(&s.isRunning, 1)
	return nil
}

func (s *Server) startCluster(context.Context) error {
	s.basicCluster = core.NewBasicCluster()
	s.storage = endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	err := s.startMetaConfWatcher()
	if err != nil {
		return err
	}
	s.hbStreams = hbstream.NewHeartbeatStreams(s.Context(), s.clusterID, utils.SchedulingServiceName, s.basicCluster)
	s.cluster, err = NewCluster(s.Context(), s.persistConfig, s.storage, s.basicCluster, s.hbStreams, s.clusterID, s.checkMembershipCh)
	if err != nil {
		return err
	}
	// Inject the cluster components into the config watcher after the scheduler controller is created.
	s.configWatcher.SetSchedulersController(s.cluster.GetCoordinator().GetSchedulersController())
	// Start the rule watcher after the cluster is created.
	err = s.startRuleWatcher()
	if err != nil {
		return err
	}
	s.cluster.StartBackgroundJobs()
	return nil
}

func (s *Server) stopCluster() {
	s.cluster.StopBackgroundJobs()
	s.stopWatcher()
}

func (s *Server) startMetaConfWatcher() (err error) {
	s.metaWatcher, err = meta.NewWatcher(s.Context(), s.GetClient(), s.clusterID, s.basicCluster)
	if err != nil {
		return err
	}
	s.configWatcher, err = config.NewWatcher(s.Context(), s.GetClient(), s.clusterID, s.persistConfig, s.storage)
	if err != nil {
		return err
	}
	return err
}

func (s *Server) startRuleWatcher() (err error) {
	s.ruleWatcher, err = rule.NewWatcher(s.Context(), s.GetClient(), s.clusterID, s.storage,
		s.cluster.GetCoordinator().GetCheckerController(), s.cluster.GetRuleManager(), s.cluster.GetRegionLabeler())
	return err
}

func (s *Server) stopWatcher() {
	s.ruleWatcher.Close()
	s.configWatcher.Close()
	s.metaWatcher.Close()
}

// CreateServer creates the Server
func CreateServer(ctx context.Context, cfg *config.Config) *Server {
	svr := &Server{
		BaseServer:        server.NewBaseServer(ctx),
		DiagnosticsServer: sysutil.NewDiagnosticsServer(cfg.Log.File.Filename),
		cfg:               cfg,
		persistConfig:     config.NewPersistConfig(cfg),
		checkMembershipCh: make(chan struct{}, 1),
	}
	return svr
}

// CreateServerWrapper encapsulates the configuration/log/metrics initialization and create the server
func CreateServerWrapper(cmd *cobra.Command, args []string) {
	schedulers.Register()
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
		utils.Exit(0)
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

	versioninfo.Log(serviceName)
	log.Info("scheduling service config", zap.Reflect("config", cfg))

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
		utils.Exit(0)
	default:
		utils.Exit(1)
	}
}
