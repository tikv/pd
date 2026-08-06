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
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/schedulingpb"
	"github.com/pingcap/log"
	"github.com/pingcap/sysutil"

	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/cgroup"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/mcs/scheduling/server/affinity"
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

	// for the primary election of scheduling
	participant *member.Participant

	service           *Service
	checkMembershipCh chan struct{}

	// primaryCallbacks will be called after the server becomes primary.
	primaryCallbacks     []func(context.Context) error
	primaryExitCallbacks []func()

	// for service registry
	serviceID       *discovery.ServiceRegistryEntry
	serviceRegister *discovery.ServiceRegister

	cluster atomic.Value // *Cluster

	// Cgroup Monitor
	cgMonitor cgroup.Monitor
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
	if err = s.initListenerAndUpdateConfig(); err != nil {
		return err
	}
	defer func() {
		if err != nil && s.serviceRegister != nil {
			if deregisterErr := s.serviceRegister.Deregister(); deregisterErr != nil {
				log.Warn("failed to deregister the service", errs.ZapError(deregisterErr))
			}
		}
		if err != nil && s.GetListener() != nil {
			if closeErr := s.GetListener().Close(); closeErr != nil {
				log.Warn("failed to close listener", errs.ZapError(closeErr))
			}
		}
	}()

	if s.serviceID, s.serviceRegister, err = utils.Register(s, constant.SchedulingServiceName); err != nil {
		return err
	}

	s.cgMonitor.StartMonitor(s.Context())
	return s.startServer()
}

func (s *Server) startServerLoop() {
	s.serverLoopCtx, s.serverLoopCancel = context.WithCancel(s.Context())
	s.serverLoopWg.Add(2)
	go s.primaryElectionLoop()
	go s.updatePDMemberLoop()
}

func (s *Server) updatePDMemberLoop() {
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
				log.Info("member is not started yet", zap.String("member-id", strconv.FormatUint(ep.GetID(), 16)), errs.ZapError(err))
				continue
			}
			status, err := s.GetClient().Status(ctx, ep.ClientURLs[0])
			if err != nil {
				log.Info("failed to get status of member", zap.String("member-id", strconv.FormatUint(ep.ID, 16)), zap.String("endpoint", ep.ClientURLs[0]), errs.ZapError(err))
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
				cluster := s.GetCluster()
				if cluster != nil {
					if cluster.SwitchPDLeader(pdpb.NewPDClient(cc)) {
						if status.Leader != curLeader {
							log.Info("switch PD leader", zap.String("leader-id", strconv.FormatUint(ep.ID, 16)), zap.String("endpoint", ep.ClientURLs[0]))
						}
						curLeader = ep.ID
						break
					}
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

		primary, checkAgain := s.participant.CheckPrimary()
		if checkAgain {
			continue
		}
		if primary != nil {
			log.Info("start to watch the primary", zap.Stringer("scheduling-primary", primary))
			// Watch will keep looping and never return unless the primary has changed.
			primary.Watch(s.serverLoopCtx)
			log.Info("the scheduling primary has changed, try to re-campaign a primary")
		}

		// To make sure the expected primary(if existed) and new primary are on the same server.
		expectedPrimary, err := utils.GetExpectedPrimaryFlag(s.GetClient(), &s.participant.MsParam)
		if err != nil {
			// Do not campaign on a read failure: an empty flag would be treated as
			// "no transfer" and skip the affinity guard, so retry instead.
			log.Warn("failed to get expected primary flag, retry later", errs.ZapError(err))
			time.Sleep(200 * time.Millisecond)
			continue
		}
		// skip campaign the primary if the expected primary is not empty and not this member.
		// expected primary ONLY SET BY `{service}/primary/transfer` API.
		if len(expectedPrimary) > 0 && !s.participant.IsExpectedPrimary(expectedPrimary) {
			log.Info("skip campaigning of scheduling primary and check later",
				zap.String("server-name", s.Name()),
				zap.String("expected-primary-id", expectedPrimary),
				zap.Uint64("participant-id", s.participant.ID()),
				zap.String("cur-participant-value", s.participant.ParticipantString()))
			time.Sleep(200 * time.Millisecond)
			continue
		}

		s.campaignPrimary(expectedPrimary)
	}
}

// campaignPrimary campaigns the scheduling primary. expectedPrimary is the value of
// the expected primary flag observed before campaigning (empty when no transfer is
// in progress); it is used to bind the campaign to the transfer target atomically
// and to clean the flag up once this server wins.
func (s *Server) campaignPrimary(expectedPrimary string) {
	log.Info("start to campaign the primary", zap.String("campaign-scheduling-primary-name", s.participant.Name()))
	var cmps []clientv3.Cmp
	if cmp := utils.ExpectedPrimaryCmp(&s.participant.MsParam, expectedPrimary); cmp != nil {
		cmps = append(cmps, *cmp)
	}
	if err := s.participant.CampaignWithCmps(s.Context(), s.cfg.LeaderLease, cmps...); err != nil {
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
	var resetPrimaryOnce sync.Once
	defer resetPrimaryOnce.Do(func() {
		cancel()
		s.participant.Resign()
		member.ServiceMemberGauge.WithLabelValues(serviceName).Set(0)
	})

	// maintain the leadership, after this, Scheduling could be ready to provide service.
	s.participant.GetLeadership().Keep(ctx)
	log.Info("campaign scheduling primary ok", zap.String("campaign-scheduling-primary-name", s.participant.Name()))

	// We have won the campaign, so the expected primary flag (if any) has served its
	// purpose as the affinity guard. Delete it so steady state is clean and a later
	// failure re-elects immediately instead of waiting for the flag's TTL.
	utils.DeleteExpectedPrimaryFlag(s.GetClient(), &s.participant.MsParam, expectedPrimary)

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
	s.participant.PromoteSelf()

	member.ServiceMemberGauge.WithLabelValues(serviceName).Set(1)
	log.Info("scheduling primary is ready to serve", zap.String("scheduling-primary-name", s.participant.Name()))

	primaryTicker := time.NewTicker(constant.PrimaryTickInterval)
	defer primaryTicker.Stop()

	for {
		select {
		case <-primaryTicker.C:
			// Step down once the leader lease is gone. This covers both lease
			// expiration and a `{service}/primary/transfer` API call, which resigns
			// this primary by revoking its leader lease.
			if !s.participant.IsServing() {
				log.Info("no longer a primary because lease has expired or transferred, the scheduling primary will step down")
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
	s.cgMonitor.StopMonitor()
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

// IsServing returns whether the server is the primary.
func (s *Server) IsServing() bool {
	return !s.IsClosed() && s.participant.IsServing()
}

// IsClosed checks if the server loop is closed
func (s *Server) IsClosed() bool {
	return s != nil && atomic.LoadInt64(&s.isRunning) == 0
}

// AddServiceReadyCallback adds callbacks when the server becomes the primary.
func (s *Server) AddServiceReadyCallback(callbacks ...func(context.Context) error) {
	s.primaryCallbacks = append(s.primaryCallbacks, callbacks...)
}

// AddServiceExitCallback adds callbacks when the server becomes the primary.
func (s *Server) AddServiceExitCallback(callbacks ...func()) {
	s.primaryExitCallbacks = append(s.primaryExitCallbacks, callbacks...)
}

// GetTLSConfig gets the security config.
func (s *Server) GetTLSConfig() *grpcutil.TLSConfig {
	return &s.cfg.Security.TLSConfig
}

func (s *Server) initListenerAndUpdateConfig() error {
	oldListenAddr := s.cfg.ListenAddr
	if err := s.InitListener(s.GetTLSConfig(), s.cfg.GetListenAddr()); err != nil {
		return err
	}
	actualListenAddr := s.GetActualListenAddr()
	if actualListenAddr == "" {
		return nil
	}
	s.cfg.ListenAddr = server.ResolveListenAddr(s.cfg.ListenAddr, actualListenAddr)
	s.cfg.AdvertiseListenAddr = server.ResolveAdvertiseListenAddr(s.cfg.AdvertiseListenAddr, actualListenAddr)
	if s.cfg.Name == "" || s.cfg.Name == oldListenAddr {
		s.cfg.Name = s.cfg.AdvertiseListenAddr
	}
	return nil
}

// GetCluster returns the cluster.
func (s *Server) GetCluster() *Cluster {
	cluster := s.cluster.Load()
	if cluster == nil {
		return nil
	}
	return cluster.(*Cluster)
}

// GetBasicCluster returns the basic cluster.
func (s *Server) GetBasicCluster() *core.BasicCluster {
	if cluster := s.GetCluster(); cluster != nil {
		return cluster.GetBasicCluster()
	}
	return nil
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

// GetServingUrls gets service endpoints.
func (s *Server) GetServingUrls() []string {
	return s.participant.GetServingUrls()
}

func (s *Server) startServer() (err error) {
	// The independent Scheduling service still reuses PD version info since PD and Scheduling are just
	// different service modes provided by the same pd-server binary
	bs.ServerInfoGauge.WithLabelValues(versioninfo.PDReleaseVersion, versioninfo.PDGitHash).Set(float64(time.Now().Unix()))
	bs.ServerMaxProcsGauge.Set(float64(runtime.GOMAXPROCS(0)))
	uniqueName := s.cfg.GetAdvertiseListenAddr()
	uniqueID := memberutil.GenerateUniqueID(uniqueName)
	log.Info("joining primary election", zap.String("participant-name", uniqueName), zap.Uint64("participant-id", uniqueID))
	s.participant = member.NewParticipant(s.GetClient(), keypath.MsParam{
		ServiceName: constant.SchedulingServiceName,
	})
	p := &schedulingpb.Participant{
		Name:       uniqueName,
		Id:         uniqueID, // id is unique among all participants
		ListenUrls: []string{s.cfg.GetAdvertiseListenAddr()},
	}
	s.participant.InitInfo(p, constant.SchedulingServiceName+" primary election")

	s.service = &Service{Server: s}
	s.AddServiceReadyCallback(s.startCluster)
	s.AddServiceExitCallback(s.stopCluster)

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

func (s *Server) startCluster(ctx context.Context) error {
	basicCluster := core.NewBasicCluster()
	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)

	var (
		hbStreams       *hbstream.HeartbeatStreams
		configWatcher   *config.Watcher
		metaWatcher     *meta.Watcher
		ruleWatcher     *rule.Watcher
		affinityWatcher *affinity.Watcher
		cluster         *Cluster
		err             error
	)

	var initSucceeded bool
	defer func() {
		if initSucceeded {
			return
		}
		// make sure cancel context done when some initialization step failed, to avoid goroutine leak in cluster.
		if cluster != nil {
			cluster.stopCluster()
		}
		// clean new sources
		if hbStreams != nil {
			hbStreams.Close()
			hbStreams = nil
		}
		if configWatcher != nil {
			configWatcher.Close()
			configWatcher = nil
		}
		if metaWatcher != nil {
			metaWatcher.Close()
			metaWatcher = nil
		}
		if ruleWatcher != nil {
			ruleWatcher.Close()
			ruleWatcher = nil
		}
		if affinityWatcher != nil {
			affinityWatcher.Close()
			affinityWatcher = nil
		}
		if storage != nil {
			storage.Close()
		}
	}()
	metaWatcher, configWatcher, err = s.startMetaConfWatcher(ctx, basicCluster, storage)
	if err != nil {
		return err
	}
	hbStreams = hbstream.NewHeartbeatStreams(ctx, constant.SchedulingServiceName, basicCluster)
	cluster, err = NewCluster(ctx, s.persistConfig, storage, basicCluster, hbStreams, s.checkMembershipCh, s.GetHTTPClient(), s.GetBackendEndpoints())
	storage = nil
	hbStreams = nil
	if err != nil {
		return err
	}

	am := cluster.GetAffinityManager()
	configWatcher.SetSchedulersController(cluster.GetCoordinator().GetSchedulersController())
	ruleWatcher, err = rule.NewWatcher(ctx, s.GetClient(), cluster.GetStorage(),
		cluster.GetCoordinator().GetCheckerController(), cluster.GetRuleManager(), cluster.GetRegionLabeler())
	if err != nil {
		return err
	}
	affinityWatcher, err = affinity.NewWatcher(ctx, s.GetClient(), am)
	if err != nil {
		return err
	}

	cluster.SetRuntimeResources(metaWatcher, configWatcher, ruleWatcher, affinityWatcher)
	// Set watchers to nil to avoid being closed in defer when cluster initialization is successful,
	// since cluster will take over the ownership of these watchers and close them when stopping cluster.
	metaWatcher = nil
	configWatcher = nil
	ruleWatcher = nil
	affinityWatcher = nil
	cluster.StartBackgroundJobs()
	s.cluster.Store(cluster)
	initSucceeded = true
	return nil
}

func (s *Server) stopCluster() {
	if cluster := s.GetCluster(); cluster != nil {
		s.cluster.Store((*Cluster)(nil))
		cluster.stopCluster()
	}
}

func (s *Server) startMetaConfWatcher(
	ctx context.Context,
	basicCluster *core.BasicCluster,
	storage *endpoint.StorageEndpoint,
) (metaWatcher *meta.Watcher, configWatcher *config.Watcher, err error) {
	metaWatcher, err = meta.NewWatcher(ctx, s.GetClient(), basicCluster)
	if err != nil {
		return nil, nil, err
	}
	configWatcher, err = config.NewWatcher(ctx, s.GetClient(), s.persistConfig, storage)
	if err != nil {
		metaWatcher.Close()
		return nil, nil, err
	}
	return metaWatcher, configWatcher, nil
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
	cfg.Schedule.MaxMergeRegionKeys = cfg.Schedule.GetMaxMergeRegionKeys()
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
	err = logutil.SetupLogger(&cfg.Log, &cfg.Logger, &cfg.LogProps, cfg.Security.RedactInfoLog)
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
