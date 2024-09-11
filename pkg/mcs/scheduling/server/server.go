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
	"runtime"
	"strings"
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
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	"github.com/tikv/pd/pkg/mcs/scheduling/server/meta"
	"github.com/tikv/pd/pkg/mcs/scheduling/server/rule"
	"github.com/tikv/pd/pkg/mcs/server"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/schedule"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/keypath"
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

// GetAdvertiseListenAddr returns the advertise address of the server.
func (s *Server) GetAdvertiseListenAddr() string {
	return s.cfg.AdvertiseListenAddr
}

// GetBackendEndpoints returns the backend endpoints.
func (s *Server) GetBackendEndpoints() string {
	return s.cfg.BackendEndpoints
}

// GetParticipant returns the participant.
func (s *Server) GetParticipant() *member.Participant {
	return s.participant
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
func (s *Server) Run() (err error) {
	if err = utils.InitClient(s); err != nil {
		return err
	}

	if s.serviceID, s.serviceRegister, err = utils.Register(s, constant.SchedulingServiceName); err != nil {
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
		ticker.Reset(100 * time.Millisecond)
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
		members, err := etcdutil.ListEtcdMembers(ctx, s.GetClient())
		if err != nil {
			log.Warn("failed to list members", errs.ZapError(err))
			continue
		}
		for _, ep := range members.Members {
			if len(ep.GetClientURLs()) == 0 { // This member is not started yet.
				log.Info("member is not started yet", zap.String("member-id", fmt.Sprintf("%x", ep.GetID())), errs.ZapError(err))
				continue
			}
			status, err := s.GetClient().Status(ctx, ep.ClientURLs[0])
			if err != nil {
				log.Info("failed to get status of member", zap.String("member-id", fmt.Sprintf("%x", ep.ID)), zap.String("endpoint", ep.ClientURLs[0]), errs.ZapError(err))
				continue
			}
			if status.Leader == ep.ID {
				cc, err := s.GetDelegateClient(ctx, s.GetTLSConfig(), ep.ClientURLs[0])
				if err != nil {
					log.Info("failed to get delegate client", errs.ZapError(err))
					continue
				}
				if !s.IsServing() {
					// double check
					break
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

		// To make sure the expected primary(if existed) and new primary are on the same server.
		expectedPrimary := utils.GetExpectedPrimaryFlag(s.GetClient(), s.participant.GetLeaderPath())
		// skip campaign the primary if the expected primary is not empty and not equal to the current memberValue.
		// expected primary ONLY SET BY `{service}/primary/transfer` API.
		if len(expectedPrimary) > 0 && !strings.Contains(s.participant.MemberValue(), expectedPrimary) {
			log.Info("skip campaigning of scheduling primary and check later",
				zap.String("server-name", s.Name()),
				zap.String("expected-primary-id", expectedPrimary),
				zap.Uint64("member-id", s.participant.ID()),
				zap.String("cur-member-value", s.participant.MemberValue()))
			time.Sleep(200 * time.Millisecond)
			continue
		}

		s.campaignLeader()
	}
}

func (s *Server) campaignLeader() {
	log.Info("start to campaign the primary/leader", zap.String("campaign-scheduling-primary-name", s.participant.Name()))
	if err := s.participant.CampaignLeader(s.Context(), s.cfg.LeaderLease); err != nil {
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
	// check expected primary and watch the primary.
	exitPrimary := make(chan struct{})
	lease, err := utils.KeepExpectedPrimaryAlive(ctx, s.GetClient(), exitPrimary,
		s.cfg.LeaderLease, s.participant.GetLeaderPath(), s.participant.MemberValue(), constant.SchedulingServiceName)
	if err != nil {
		log.Error("prepare scheduling primary watch error", errs.ZapError(err))
		return
	}
	s.participant.SetExpectedPrimaryLease(lease)
	s.participant.EnableLeader()

	member.ServiceMemberGauge.WithLabelValues(serviceName).Set(1)
	log.Info("scheduling primary is ready to serve", zap.String("scheduling-primary-name", s.participant.Name()))

	leaderTicker := time.NewTicker(constant.LeaderTickInterval)
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
		case <-exitPrimary:
			log.Info("no longer be primary because primary have been updated, the scheduling primary will step down")
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
	if err := s.serviceRegister.Deregister(); err != nil {
		log.Error("failed to deregister the service", errs.ZapError(err))
	}
	utils.StopHTTPServer(s)
	utils.StopGRPCServer(s)
	s.GetListener().Close()
	s.CloseClientConns()
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
	// The independent Scheduling service still reuses PD version info since PD and Scheduling are just
	// different service modes provided by the same pd-server binary
	bs.ServerInfoGauge.WithLabelValues(versioninfo.PDReleaseVersion, versioninfo.PDGitHash).Set(float64(time.Now().Unix()))
	bs.ServerMaxProcsGauge.Set(float64(runtime.GOMAXPROCS(0)))
	uniqueName := s.cfg.GetAdvertiseListenAddr()
	uniqueID := memberutil.GenerateUniqueID(uniqueName)
	log.Info("joining primary election", zap.String("participant-name", uniqueName), zap.Uint64("participant-id", uniqueID))
	s.participant = member.NewParticipant(s.GetClient(), constant.SchedulingServiceName)
	p := &schedulingpb.Participant{
		Name:       uniqueName,
		Id:         uniqueID, // id is unique among all participants
		ListenUrls: []string{s.cfg.GetAdvertiseListenAddr()},
	}
	s.participant.InitInfo(p, keypath.SchedulingSvcRootPath(), constant.PrimaryKey, "primary election")

	s.service = &Service{Server: s}
	s.AddServiceReadyCallback(s.startCluster)
	s.AddServiceExitCallback(s.stopCluster)
	if err := s.InitListener(s.GetTLSConfig(), s.cfg.GetListenAddr()); err != nil {
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
	s.hbStreams = hbstream.NewHeartbeatStreams(s.Context(), constant.SchedulingServiceName, s.basicCluster)
	s.cluster, err = NewCluster(s.Context(), s.persistConfig, s.storage, s.basicCluster, s.hbStreams, s.checkMembershipCh)
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
	s.metaWatcher, err = meta.NewWatcher(s.Context(), s.GetClient(), s.basicCluster)
	if err != nil {
		return err
	}
	s.configWatcher, err = config.NewWatcher(s.Context(), s.GetClient(), s.persistConfig, s.storage)
	if err != nil {
		return err
	}
	return err
}

func (s *Server) startRuleWatcher() (err error) {
	s.ruleWatcher, err = rule.NewWatcher(s.Context(), s.GetClient(), s.storage,
		s.cluster.GetCoordinator().GetCheckerController(), s.cluster.GetRuleManager(), s.cluster.GetRegionLabeler())
	return err
}

func (s *Server) stopWatcher() {
	s.ruleWatcher.Close()
	s.configWatcher.Close()
	s.metaWatcher.Close()
}

// GetPersistConfig returns the persist config.
// It's used to test.
func (s *Server) GetPersistConfig() *config.PersistConfig {
	return s.persistConfig
}

// GetConfig gets the config.
func (s *Server) GetConfig() *config.Config {
	cfg := s.cfg.Clone()
	cfg.Schedule = *s.persistConfig.GetScheduleConfig().Clone()
	cfg.Replication = *s.persistConfig.GetReplicationConfig().Clone()
	cfg.ClusterVersion = *s.persistConfig.GetClusterVersion()
	return cfg
}

// CreateServer creates the Server
func CreateServer(ctx context.Context, cfg *config.Config) *Server {
	svr := &Server{
		BaseServer:        server.NewBaseServer(ctx),
		DiagnosticsServer: sysutil.NewDiagnosticsServer(cfg.Log.File.Filename),
		cfg:               cfg,
		persistConfig:     config.NewPersistConfig(cfg, cache.NewStringTTL(ctx, sc.DefaultGCInterval, sc.DefaultTTL)),
		checkMembershipCh: make(chan struct{}, 1),
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
	err = logutil.SetupLogger(cfg.Log, &cfg.Logger, &cfg.LogProps, cfg.Security.RedactInfoLog)
	if err == nil {
		log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
	} else {
		log.Fatal("initialize logger error", errs.ZapError(err))
	}
	// Flushing any buffered log entries
	log.Sync()

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
