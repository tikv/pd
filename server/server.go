// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/coreos/go-semver/semver"
	"github.com/golang/protobuf/proto"
	"github.com/gorilla/mux"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/configpb"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/pd/v4/pkg/etcdutil"
	"github.com/pingcap/pd/v4/pkg/grpcutil"
	"github.com/pingcap/pd/v4/pkg/logutil"
	"github.com/pingcap/pd/v4/pkg/typeutil"
	"github.com/pingcap/pd/v4/server/cluster"
	"github.com/pingcap/pd/v4/server/config"
	configmanager "github.com/pingcap/pd/v4/server/config_manager"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/id"
	"github.com/pingcap/pd/v4/server/kv"
	"github.com/pingcap/pd/v4/server/member"
	syncer "github.com/pingcap/pd/v4/server/region_syncer"
	"github.com/pingcap/pd/v4/server/schedule/opt"
	"github.com/pingcap/pd/v4/server/tso"
	"github.com/pingcap/sysutil"
	"github.com/pkg/errors"
	"github.com/urfave/negroni"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/pkg/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	etcdTimeout           = time.Second * 3
	serverMetricsInterval = time.Minute
	leaderTickInterval    = 50 * time.Millisecond
	// pdRootPath for all pd servers.
	pdRootPath      = "/pd"
	pdAPIPrefix     = "/pd/"
	pdClusterIDPath = "/pd/cluster_id"
)

var (
	// EnableZap enable the zap logger in embed etcd.
	EnableZap = false
	// EtcdStartTimeout the timeout of the startup etcd.
	EtcdStartTimeout = time.Minute * 5
)

// Component is used to represent the kind of component in config manager.
const Component = "pd"

// ConfigCheckInterval represents the time interval of running config check.
var ConfigCheckInterval = 1 * time.Second

// Server is the pd server.
type Server struct {
	diagnosticspb.DiagnosticsServer

	// Server state.
	isServing int64

	// Server start timestamp
	startTimestamp int64

	// Configs and initial fields.
	cfg         *config.Config
	etcdCfg     *embed.Config
	scheduleOpt *config.ScheduleOption
	handler     *Handler

	ctx              context.Context
	serverLoopCtx    context.Context
	serverLoopCancel func()
	serverLoopWg     sync.WaitGroup

	member    *member.Member
	client    *clientv3.Client
	clusterID uint64 // pd cluster id.
	rootPath  string

	// Server services.
	// for id allocator, we can use one allocator for
	// store, region and peer, because we just need
	// a unique ID.
	idAllocator *id.AllocatorImpl
	// for storage operation.
	storage *core.Storage
	// for baiscCluster operation.
	basicCluster *core.BasicCluster
	// for tso.
	tso *tso.TimestampOracle
	// for raft cluster
	cluster *cluster.RaftCluster
	// For async region heartbeat.
	hbStreams *heartbeatStreams
	// Zap logger
	lg       *zap.Logger
	logProps *log.ZapProperties

	// components' configuration management
	cfgManager *configmanager.ConfigManager
	// component config
	configVersion *configpb.Version
	configClient  pd.ConfigClient

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()
}

// HandlerBuilder builds a server HTTP handler.
type HandlerBuilder func(context.Context, *Server) (http.Handler, ServiceGroup, error)

// ServiceGroup used to register the service.
type ServiceGroup struct {
	Name       string
	Version    string
	IsCore     bool
	PathPrefix string
}

const (
	// CorePath the core group, is at REST path `/pd/api/v1`.
	CorePath = "/pd/api/v1"
	// ExtensionsPath the named groups are REST at `/pd/apis/{GROUP_NAME}/{Version}`.
	ExtensionsPath = "/pd/apis"
)

func combineBuilderServerHTTPService(ctx context.Context, svr *Server, serviceBuilders ...HandlerBuilder) (map[string]http.Handler, error) {
	userHandlers := make(map[string]http.Handler)
	registerMap := make(map[string]struct{})

	apiService := negroni.New()
	recovery := negroni.NewRecovery()
	apiService.Use(recovery)
	router := mux.NewRouter()

	for _, build := range serviceBuilders {
		handler, info, err := build(ctx, svr)
		if err != nil {
			return nil, err
		}
		if !info.IsCore && len(info.PathPrefix) == 0 && (len(info.Name) == 0 || len(info.Version) == 0) {
			return nil, errors.Errorf("invalid API information, group %s version %s", info.Name, info.Version)
		}
		var pathPrefix string
		if len(info.PathPrefix) != 0 {
			pathPrefix = info.PathPrefix
		} else if info.IsCore {
			pathPrefix = CorePath
		} else {
			pathPrefix = path.Join(ExtensionsPath, info.Name, info.Version)
		}
		if _, ok := registerMap[pathPrefix]; ok {
			return nil, errors.Errorf("service with path [%s] already registered", pathPrefix)
		}

		log.Info("register REST path", zap.String("path", pathPrefix))
		registerMap[pathPrefix] = struct{}{}
		if len(info.PathPrefix) != 0 {
			// If PathPrefix is specified, register directly into userHandlers
			userHandlers[pathPrefix] = handler
		} else {
			// If PathPrefix is not specified, register into apiService,
			// and finally apiService is registered in userHandlers.
			router.PathPrefix(pathPrefix).Handler(handler)
			if info.IsCore {
				// Deprecated
				router.Path("/pd/health").Handler(handler)
				// Deprecated
				router.Path("/pd/diagnose").Handler(handler)
				// Deprecated
				router.Path("/pd/ping").Handler(handler)
			}
		}
	}
	apiService.UseHandler(router)
	userHandlers[pdAPIPrefix] = apiService

	return userHandlers, nil
}

// CreateServer creates the UNINITIALIZED pd server with given configuration.
func CreateServer(ctx context.Context, cfg *config.Config, serviceBuilders ...HandlerBuilder) (*Server, error) {
	log.Info("PD Config", zap.Reflect("config", cfg))
	rand.Seed(time.Now().UnixNano())

	s := &Server{
		cfg:               cfg,
		scheduleOpt:       config.NewScheduleOption(cfg),
		member:            &member.Member{},
		ctx:               ctx,
		startTimestamp:    time.Now().Unix(),
		DiagnosticsServer: sysutil.NewDiagnosticsServer(cfg.Log.File.Filename),
	}

	s.cfgManager = configmanager.NewConfigManager(s)
	s.handler = newHandler(s)

	// Adjust etcd config.
	etcdCfg, err := s.cfg.GenEmbedEtcdConfig()
	if err != nil {
		return nil, err
	}
	if len(serviceBuilders) != 0 {
		userHandlers, err := combineBuilderServerHTTPService(ctx, s, serviceBuilders...)
		if err != nil {
			return nil, err
		}
		etcdCfg.UserHandlers = userHandlers
	}
	etcdCfg.ServiceRegister = func(gs *grpc.Server) {
		pdpb.RegisterPDServer(gs, s)
		diagnosticspb.RegisterDiagnosticsServer(gs, s)
		configpb.RegisterConfigServer(gs, s.cfgManager)
	}
	s.etcdCfg = etcdCfg
	if EnableZap {
		// The etcd master version has removed embed.Config.SetupLogging.
		// Now logger is set up automatically based on embed.Config.Logger,
		// Use zap logger in the test, otherwise will panic.
		// Reference: https://go.etcd.io/etcd/blob/master/embed/config_logging.go#L45
		s.etcdCfg.Logger = "zap"
		s.etcdCfg.LogOutputs = []string{"stdout"}
	}
	s.lg = cfg.GetZapLogger()
	s.logProps = cfg.GetZapLogProperties()
	return s, nil
}

func (s *Server) startEtcd(ctx context.Context) error {
	newCtx, cancel := context.WithTimeout(ctx, EtcdStartTimeout)
	defer cancel()

	etcd, err := embed.StartEtcd(s.etcdCfg)
	if err != nil {
		return errors.WithStack(err)
	}

	// Check cluster ID
	urlmap, err := types.NewURLsMap(s.cfg.InitialCluster)
	if err != nil {
		return errors.WithStack(err)
	}
	tlsConfig, err := s.cfg.Security.ToTLSConfig()
	if err != nil {
		return err
	}

	if err = etcdutil.CheckClusterID(etcd.Server.Cluster().ID(), urlmap, tlsConfig); err != nil {
		return err
	}

	select {
	// Wait etcd until it is ready to use
	case <-etcd.Server.ReadyNotify():
	case <-newCtx.Done():
		return errors.Errorf("canceled when waiting embed etcd to be ready")
	}

	endpoints := []string{s.etcdCfg.ACUrls[0].String()}
	log.Info("create etcd v3 client", zap.Strings("endpoints", endpoints))

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: etcdTimeout,
		TLS:         tlsConfig,
	})
	if err != nil {
		return errors.WithStack(err)
	}

	etcdServerID := uint64(etcd.Server.ID())

	// update advertise peer urls.
	etcdMembers, err := etcdutil.ListEtcdMembers(client)
	if err != nil {
		return err
	}
	for _, m := range etcdMembers.Members {
		if etcdServerID == m.ID {
			etcdPeerURLs := strings.Join(m.PeerURLs, ",")
			if s.cfg.AdvertisePeerUrls != etcdPeerURLs {
				log.Info("update advertise peer urls", zap.String("from", s.cfg.AdvertisePeerUrls), zap.String("to", etcdPeerURLs))
				s.cfg.AdvertisePeerUrls = etcdPeerURLs
			}
		}
	}
	s.client = client
	failpoint.Inject("memberNil", func() {
		time.Sleep(1500 * time.Millisecond)
	})
	s.member = member.NewMember(etcd, client, etcdServerID)
	return nil
}

// AddStartCallback adds a callback in the startServer phase.
func (s *Server) AddStartCallback(callbacks ...func()) {
	s.startCallbacks = append(s.startCallbacks, callbacks...)
}

func (s *Server) startServer(ctx context.Context) error {
	var err error
	if err = s.initClusterID(); err != nil {
		return err
	}
	log.Info("init cluster id", zap.Uint64("cluster-id", s.clusterID))
	// It may lose accuracy if use float64 to store uint64. So we store the
	// cluster id in label.
	metadataGauge.WithLabelValues(fmt.Sprintf("cluster%d", s.clusterID)).Set(0)

	s.rootPath = path.Join(pdRootPath, strconv.FormatUint(s.clusterID, 10))
	s.member.MemberInfo(s.cfg, s.Name(), s.rootPath)
	s.member.SetMemberDeployPath(s.member.ID())
	s.member.SetMemberBinaryVersion(s.member.ID(), PDReleaseVersion)
	s.idAllocator = id.NewAllocatorImpl(s.client, s.rootPath, s.member.MemberValue())
	s.tso = tso.NewTimestampOracle(
		s.client,
		s.rootPath,
		s.member.MemberValue(),
		s.cfg.TsoSaveInterval.Duration,
		func() time.Duration { return s.scheduleOpt.LoadPDServerConfig().MaxResetTSGap },
	)
	kvBase := kv.NewEtcdKVBase(s.client, s.rootPath)
	path := filepath.Join(s.cfg.DataDir, "region-meta")
	regionStorage, err := core.NewRegionStorage(ctx, path)
	if err != nil {
		return err
	}
	s.storage = core.NewStorage(kvBase).SetRegionStorage(regionStorage)
	s.basicCluster = core.NewBasicCluster()
	s.cluster = cluster.NewRaftCluster(ctx, s.GetClusterRootPath(), s.clusterID, syncer.NewRegionSyncer(s), s.client)
	s.hbStreams = newHeartbeatStreams(ctx, s.clusterID, s.cluster)

	// Run callbacks
	for _, cb := range s.startCallbacks {
		cb()
	}

	// Server has started.
	atomic.StoreInt64(&s.isServing, 1)
	return nil
}

func (s *Server) initClusterID() error {
	// Get any cluster key to parse the cluster ID.
	resp, err := etcdutil.EtcdKVGet(s.client, pdClusterIDPath)
	if err != nil {
		return err
	}

	// If no key exist, generate a random cluster ID.
	if len(resp.Kvs) == 0 {
		s.clusterID, err = initOrGetClusterID(s.client, pdClusterIDPath)
		return err
	}
	s.clusterID, err = typeutil.BytesToUint64(resp.Kvs[0].Value)
	return err
}

// AddCloseCallback adds a callback in the Close phase.
func (s *Server) AddCloseCallback(callbacks ...func()) {
	s.closeCallbacks = append(s.closeCallbacks, callbacks...)
}

// Close closes the server.
func (s *Server) Close() {
	if !atomic.CompareAndSwapInt64(&s.isServing, 1, 0) {
		// server is already closed
		return
	}

	log.Info("closing server")

	s.stopServerLoop()

	if s.client != nil {
		s.client.Close()
	}

	if s.member.Etcd() != nil {
		s.member.Close()
	}

	if s.hbStreams != nil {
		s.hbStreams.Close()
	}
	if err := s.storage.Close(); err != nil {
		log.Error("close storage meet error", zap.Error(err))
	}

	// Run callbacks
	for _, cb := range s.closeCallbacks {
		cb()
	}

	log.Info("close server")
}

// IsClosed checks whether server is closed or not.
func (s *Server) IsClosed() bool {
	return atomic.LoadInt64(&s.isServing) == 0
}

// Run runs the pd server.
func (s *Server) Run() error {
	go StartMonitor(s.ctx, time.Now, func() {
		log.Error("system time jumps backward")
		timeJumpBackCounter.Inc()
	})
	if err := s.startEtcd(s.ctx); err != nil {
		return err
	}

	if err := s.startServer(s.ctx); err != nil {
		return err
	}

	s.startServerLoop(s.ctx)

	return nil
}

// Context returns the context of server.
func (s *Server) Context() context.Context {
	return s.ctx
}

// LoopContext returns the loop context of server.
func (s *Server) LoopContext() context.Context {
	return s.serverLoopCtx
}

func (s *Server) startServerLoop(ctx context.Context) {
	s.serverLoopCtx, s.serverLoopCancel = context.WithCancel(ctx)
	s.serverLoopWg.Add(3)
	go s.leaderLoop()
	go s.etcdLeaderLoop()
	go s.serverMetricsLoop()
	if s.cfg.EnableDynamicConfig {
		s.serverLoopWg.Add(1)
		go s.configCheckLoop()
	}
}

func (s *Server) stopServerLoop() {
	s.serverLoopCancel()
	s.serverLoopWg.Wait()
}

func (s *Server) serverMetricsLoop() {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()
	for {
		select {
		case <-time.After(serverMetricsInterval):
			s.collectEtcdStateMetrics()
		case <-ctx.Done():
			log.Info("server is closed, exit metrics loop")
			return
		}
	}
}

func (s *Server) collectEtcdStateMetrics() {
	etcdStateGauge.WithLabelValues("term").Set(float64(s.member.Etcd().Server.Term()))
	etcdStateGauge.WithLabelValues("appliedIndex").Set(float64(s.member.Etcd().Server.AppliedIndex()))
	etcdStateGauge.WithLabelValues("committedIndex").Set(float64(s.member.Etcd().Server.CommittedIndex()))
}

func (s *Server) bootstrapCluster(req *pdpb.BootstrapRequest) (*pdpb.BootstrapResponse, error) {
	clusterID := s.clusterID

	log.Info("try to bootstrap raft cluster",
		zap.Uint64("cluster-id", clusterID),
		zap.String("request", fmt.Sprintf("%v", req)))

	if err := checkBootstrapRequest(clusterID, req); err != nil {
		return nil, err
	}

	clusterMeta := metapb.Cluster{
		Id:           clusterID,
		MaxPeerCount: uint32(s.scheduleOpt.GetReplication().GetMaxReplicas()),
	}

	// Set cluster meta
	clusterValue, err := clusterMeta.Marshal()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	clusterRootPath := s.GetClusterRootPath()

	var ops []clientv3.Op
	ops = append(ops, clientv3.OpPut(clusterRootPath, string(clusterValue)))

	// Set bootstrap time
	bootstrapKey := makeBootstrapTimeKey(clusterRootPath)
	nano := time.Now().UnixNano()

	timeData := typeutil.Uint64ToBytes(uint64(nano))
	ops = append(ops, clientv3.OpPut(bootstrapKey, string(timeData)))

	// Set store meta
	storeMeta := req.GetStore()
	storePath := makeStoreKey(clusterRootPath, storeMeta.GetId())
	storeValue, err := storeMeta.Marshal()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ops = append(ops, clientv3.OpPut(storePath, string(storeValue)))

	regionValue, err := req.GetRegion().Marshal()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Set region meta with region id.
	regionPath := makeRegionKey(clusterRootPath, req.GetRegion().GetId())
	ops = append(ops, clientv3.OpPut(regionPath, string(regionValue)))

	// TODO: we must figure out a better way to handle bootstrap failed, maybe intervene manually.
	bootstrapCmp := clientv3.Compare(clientv3.CreateRevision(clusterRootPath), "=", 0)
	resp, err := kv.NewSlowLogTxn(s.client).If(bootstrapCmp).Then(ops...).Commit()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !resp.Succeeded {
		log.Warn("cluster already bootstrapped", zap.Uint64("cluster-id", clusterID))
		return nil, errors.Errorf("cluster %d already bootstrapped", clusterID)
	}

	log.Info("bootstrap cluster ok", zap.Uint64("cluster-id", clusterID))
	err = s.storage.SaveRegion(req.GetRegion())
	if err != nil {
		log.Warn("save the bootstrap region failed", zap.Error(err))
	}
	err = s.storage.Flush()
	if err != nil {
		log.Warn("flush the bootstrap region failed", zap.Error(err))
	}

	if err := s.cluster.Start(s); err != nil {
		return nil, err
	}

	return &pdpb.BootstrapResponse{}, nil
}

func (s *Server) createRaftCluster() error {
	if s.cluster.IsRunning() {
		return nil
	}

	return s.cluster.Start(s)
}

func (s *Server) stopRaftCluster() {
	s.cluster.Stop()
}

// GetAddr returns the server urls for clients.
func (s *Server) GetAddr() string {
	return s.cfg.AdvertiseClientUrls
}

// GetMemberInfo returns the server member information.
func (s *Server) GetMemberInfo() *pdpb.Member {
	return proto.Clone(s.member.Member()).(*pdpb.Member)
}

// GetHandler returns the handler for API.
func (s *Server) GetHandler() *Handler {
	return s.handler
}

// GetEndpoints returns the etcd endpoints for outer use.
func (s *Server) GetEndpoints() []string {
	return s.client.Endpoints()
}

// GetClient returns builtin etcd client.
func (s *Server) GetClient() *clientv3.Client {
	return s.client
}

// GetConfigManager returns the config manager of server.
func (s *Server) GetConfigManager() *configmanager.ConfigManager {
	return s.cfgManager
}

// GetConfigClient returns the config client of server.
func (s *Server) GetConfigClient() pd.ConfigClient {
	return s.configClient
}

// GetLeader returns leader of etcd.
func (s *Server) GetLeader() *pdpb.Member {
	return s.member.GetLeader()
}

// GetMember returns the member of server.
func (s *Server) GetMember() *member.Member {
	return s.member
}

// GetStorage returns the backend storage of server.
func (s *Server) GetStorage() *core.Storage {
	return s.storage
}

// SetStorage changes the storage for test purpose.
func (s *Server) SetStorage(storage *core.Storage) {
	s.storage = storage
}

// GetBasicCluster returns the basic cluster of server.
func (s *Server) GetBasicCluster() *core.BasicCluster {
	return s.basicCluster
}

// GetScheduleOption returns the schedule option.
func (s *Server) GetScheduleOption() *config.ScheduleOption {
	return s.scheduleOpt
}

// GetHBStreams returns the heartbeat streams.
func (s *Server) GetHBStreams() opt.HeartbeatStreams {
	return s.hbStreams
}

// GetAllocator returns the ID allocator of server.
func (s *Server) GetAllocator() *id.AllocatorImpl {
	return s.idAllocator
}

// GetSchedulersCallback returns a callback function to update config manager.
func (s *Server) GetSchedulersCallback() func() {
	return func() {
		if s.GetConfig().EnableDynamicConfig {
			value := s.GetScheduleConfig().Schedulers
			tmp := map[string]interface{}{
				"schedulers": value,
			}
			var buf bytes.Buffer
			if err := toml.NewEncoder(&buf).Encode(tmp); err != nil {
				log.Error("failed to encode config", zap.Error(err))
			}

			if err := s.updateConfigManager("schedule.schedulers", buf.String()); err != nil {
				log.Error("failed to update the schedulers", zap.Error(err))
			}
		}
	}
}

// Name returns the unique etcd Name for this server in etcd cluster.
func (s *Server) Name() string {
	return s.cfg.Name
}

// ClusterID returns the cluster ID of this server.
func (s *Server) ClusterID() uint64 {
	return s.clusterID
}

// StartTimestamp returns the start timestamp of this server
func (s *Server) StartTimestamp() int64 {
	return s.startTimestamp
}

// GetConfig gets the config information.
func (s *Server) GetConfig() *config.Config {
	cfg := s.cfg.Clone()
	cfg.Schedule = *s.scheduleOpt.Load()
	cfg.Replication = *s.scheduleOpt.GetReplication().Load()
	cfg.LabelProperty = s.scheduleOpt.LoadLabelPropertyConfig().Clone()
	cfg.ClusterVersion = *s.scheduleOpt.LoadClusterVersion()
	cfg.PDServerCfg = *s.scheduleOpt.LoadPDServerConfig()
	cfg.Log = *s.scheduleOpt.LoadLogConfig()
	storage := s.GetStorage()
	if storage == nil {
		return cfg
	}
	sches, configs, err := storage.LoadAllScheduleConfig()
	if err != nil {
		return cfg
	}
	payload := make(map[string]string)
	for i, sche := range sches {
		payload[sche] = configs[i]
	}
	cfg.Schedule.SchedulersPayload = payload
	return cfg
}

// GetScheduleConfig gets the balance config information.
func (s *Server) GetScheduleConfig() *config.ScheduleConfig {
	cfg := &config.ScheduleConfig{}
	*cfg = *s.scheduleOpt.Load()
	return cfg
}

// SetScheduleConfig sets the balance config information.
func (s *Server) SetScheduleConfig(cfg config.ScheduleConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	if err := cfg.Deprecated(); err != nil {
		return err
	}
	old := s.scheduleOpt.Load()
	s.scheduleOpt.Store(&cfg)
	if err := s.scheduleOpt.Persist(s.storage); err != nil {
		s.scheduleOpt.Store(old)
		log.Error("failed to update schedule config",
			zap.Reflect("new", cfg),
			zap.Reflect("old", old),
			zap.Error(err))
		return err
	}
	log.Info("schedule config is updated", zap.Reflect("new", cfg), zap.Reflect("old", old))
	return nil
}

// GetReplicationConfig get the replication config.
func (s *Server) GetReplicationConfig() *config.ReplicationConfig {
	cfg := &config.ReplicationConfig{}
	*cfg = *s.scheduleOpt.GetReplication().Load()
	return cfg
}

// SetReplicationConfig sets the replication config.
func (s *Server) SetReplicationConfig(cfg config.ReplicationConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	old := s.scheduleOpt.GetReplication().Load()
	if cfg.EnablePlacementRules != old.EnablePlacementRules {
		raftCluster := s.GetRaftCluster()
		if raftCluster == nil {
			return errors.WithStack(cluster.ErrNotBootstrapped)
		}
		if cfg.EnablePlacementRules {
			// initialize rule manager.
			if err := raftCluster.GetRuleManager().Initialize(int(cfg.MaxReplicas), cfg.LocationLabels); err != nil {
				return err
			}
		} else {
			// NOTE: can be removed after placement rules feature is enabled by default.
			for _, s := range raftCluster.GetStores() {
				if !s.IsTombstone() && isTiFlashStore(s.GetMeta()) {
					return errors.New("cannot disable placement rules with TiFlash nodes")
				}
			}
		}
	}

	s.scheduleOpt.GetReplication().Store(&cfg)
	if err := s.scheduleOpt.Persist(s.storage); err != nil {
		s.scheduleOpt.GetReplication().Store(old)
		log.Error("failed to update replication config",
			zap.Reflect("new", cfg),
			zap.Reflect("old", old),
			zap.Error(err))
		return err
	}
	log.Info("replication config is updated", zap.Reflect("new", cfg), zap.Reflect("old", old))
	return nil
}

// GetPDServerConfig gets the balance config information.
func (s *Server) GetPDServerConfig() *config.PDServerConfig {
	cfg := &config.PDServerConfig{}
	*cfg = *s.scheduleOpt.GetPDServerConfig()
	return cfg
}

// SetPDServerConfig sets the server config.
func (s *Server) SetPDServerConfig(cfg config.PDServerConfig) error {
	old := s.scheduleOpt.LoadPDServerConfig()
	s.scheduleOpt.SetPDServerConfig(&cfg)
	if err := s.scheduleOpt.Persist(s.storage); err != nil {
		s.scheduleOpt.SetPDServerConfig(old)
		log.Error("failed to update PDServer config",
			zap.Reflect("new", cfg),
			zap.Reflect("old", old),
			zap.Error(err))
		return err
	}
	log.Info("PD server config is updated", zap.Reflect("new", cfg), zap.Reflect("old", old))
	return nil
}

// SetLabelPropertyConfig sets the label property config.
func (s *Server) SetLabelPropertyConfig(cfg config.LabelPropertyConfig) error {
	old := s.scheduleOpt.LoadLabelPropertyConfig()
	s.scheduleOpt.SetLabelPropertyConfig(cfg)
	if err := s.scheduleOpt.Persist(s.storage); err != nil {
		s.scheduleOpt.SetLabelPropertyConfig(old)
		log.Error("failed to update label property config",
			zap.Reflect("new", cfg),
			zap.Reflect("old", &old),
			zap.Error(err))
		return err
	}
	log.Info("label property config is updated", zap.Reflect("new", cfg), zap.Reflect("old", old))
	return nil
}

// GetLogConfig gets the log config.
func (s *Server) GetLogConfig() *log.Config {
	cfg := &log.Config{}
	*cfg = *s.scheduleOpt.GetLogConfig()
	return cfg
}

// SetLogConfig sets the log config.
func (s *Server) SetLogConfig(cfg log.Config) error {
	old := s.scheduleOpt.LoadLogConfig()
	s.scheduleOpt.SetLogConfig(&cfg)
	log.SetLevel(logutil.StringToZapLogLevel(cfg.Level))
	if err := s.scheduleOpt.Persist(s.storage); err != nil {
		s.scheduleOpt.SetLogConfig(old)
		log.SetLevel(logutil.StringToZapLogLevel(old.Level))
		log.Error("failed to update log config",
			zap.Reflect("new", cfg),
			zap.Reflect("old", old),
			zap.Error(err))
		return err
	}
	log.Info("log config is updated", zap.Reflect("new", cfg), zap.Reflect("old", old))
	return nil
}

// SetLabelProperty inserts a label property config.
func (s *Server) SetLabelProperty(typ, labelKey, labelValue string) error {
	s.scheduleOpt.SetLabelProperty(typ, labelKey, labelValue)
	err := s.scheduleOpt.Persist(s.storage)
	if err != nil {
		s.scheduleOpt.DeleteLabelProperty(typ, labelKey, labelValue)
		log.Error("failed to update label property config",
			zap.String("typ", typ),
			zap.String("labelKey", labelKey),
			zap.String("labelValue", labelValue),
			zap.Reflect("config", s.scheduleOpt.LoadLabelPropertyConfig()),
			zap.Error(err))
		return err
	}

	log.Info("label property config is updated", zap.Reflect("config", s.scheduleOpt.LoadLabelPropertyConfig()))
	return nil
}

// DeleteLabelProperty deletes a label property config.
func (s *Server) DeleteLabelProperty(typ, labelKey, labelValue string) error {
	s.scheduleOpt.DeleteLabelProperty(typ, labelKey, labelValue)
	err := s.scheduleOpt.Persist(s.storage)
	if err != nil {
		s.scheduleOpt.SetLabelProperty(typ, labelKey, labelValue)
		log.Error("failed to delete label property config",
			zap.String("typ", typ),
			zap.String("labelKey", labelKey),
			zap.String("labelValue", labelValue),
			zap.Reflect("config", s.scheduleOpt.LoadLabelPropertyConfig()),
			zap.Error(err))
		return err
	}

	log.Info("label property config is deleted", zap.Reflect("config", s.scheduleOpt.LoadLabelPropertyConfig()))
	return nil
}

func (s *Server) updateConfigManager(name, value string) error {
	configManager := s.GetConfigManager()
	globalVersion := configManager.GetGlobalConfigs(Component).GetVersion()
	version := &configpb.Version{Global: globalVersion}
	entries := []*configpb.ConfigEntry{{Name: name, Value: value}}
	configManager.Lock()
	_, status := configManager.UpdateGlobal(Component, version, entries)
	configManager.Unlock()
	if status.GetCode() != configpb.StatusCode_OK {
		return errors.New(status.GetMessage())
	}

	return configManager.Persist(s.GetStorage())
}

// GetLabelProperty returns the whole label property config.
func (s *Server) GetLabelProperty() config.LabelPropertyConfig {
	return s.scheduleOpt.LoadLabelPropertyConfig().Clone()
}

// SetClusterVersion sets the version of cluster.
func (s *Server) SetClusterVersion(v string) error {
	version, err := cluster.ParseVersion(v)
	if err != nil {
		return err
	}
	old := s.scheduleOpt.LoadClusterVersion()
	s.scheduleOpt.SetClusterVersion(version)
	err = s.scheduleOpt.Persist(s.storage)
	if err != nil {
		s.scheduleOpt.SetClusterVersion(old)
		log.Error("failed to update cluster version",
			zap.String("old-version", old.String()),
			zap.String("new-version", v),
			zap.Error(err))
		return err
	}
	log.Info("cluster version is updated", zap.String("new-version", v))
	return nil
}

// GetClusterVersion returns the version of cluster.
func (s *Server) GetClusterVersion() semver.Version {
	return *s.scheduleOpt.LoadClusterVersion()
}

// GetSecurityConfig get the security config.
func (s *Server) GetSecurityConfig() *grpcutil.SecurityConfig {
	return &s.cfg.Security
}

// GetClusterRootPath returns the cluster root path.
func (s *Server) GetClusterRootPath() string {
	return path.Join(s.rootPath, "raft")
}

// GetRaftCluster gets Raft cluster.
// If cluster has not been bootstrapped, return nil.
func (s *Server) GetRaftCluster() *cluster.RaftCluster {
	if s.IsClosed() || !s.cluster.IsRunning() {
		return nil
	}
	return s.cluster
}

// GetCluster gets cluster.
func (s *Server) GetCluster() *metapb.Cluster {
	return &metapb.Cluster{
		Id:           s.clusterID,
		MaxPeerCount: uint32(s.scheduleOpt.GetReplication().GetMaxReplicas()),
	}
}

// GetServerOption gets the option of the server.
func (s *Server) GetServerOption() *config.ScheduleOption {
	return s.scheduleOpt
}

// GetMetaRegions gets meta regions from cluster.
func (s *Server) GetMetaRegions() []*metapb.Region {
	cluster := s.GetRaftCluster()
	if cluster != nil {
		return cluster.GetMetaRegions()
	}
	return nil
}

// GetRegions gets regions from cluster.
func (s *Server) GetRegions() []*core.RegionInfo {
	cluster := s.GetRaftCluster()
	if cluster != nil {
		return cluster.GetRegions()
	}
	return nil
}

// GetClusterStatus gets cluster status.
func (s *Server) GetClusterStatus() (*cluster.Status, error) {
	s.cluster.Lock()
	defer s.cluster.Unlock()
	return s.cluster.LoadClusterStatus()
}

// SetLogLevel sets log level.
func (s *Server) SetLogLevel(level string) {
	s.cfg.Log.Level = level
	log.SetLevel(logutil.StringToZapLogLevel(level))
	log.Warn("log level changed", zap.String("level", log.GetLevel().String()))
}

// GetConfigVersion returns the config version.
func (s *Server) GetConfigVersion() *configpb.Version {
	if s.configVersion == nil {
		return &configpb.Version{Local: 0, Global: 0}
	}
	return s.configVersion
}

func (s *Server) setConfigVersion(version *configpb.Version) {
	s.configVersion = version
}

func (s *Server) leaderLoop() {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	for {
		if s.IsClosed() {
			log.Info("server is closed, return leader loop")
			return
		}

		leader, rev, checkAgain := s.member.CheckLeader(s.Name())
		if checkAgain {
			continue
		}
		if leader != nil {
			err := s.reloadConfigFromKV()
			if err != nil {
				log.Error("reload config failed", zap.Error(err))
				continue
			}
			syncer := s.cluster.GetRegionSyncer()
			if s.scheduleOpt.LoadPDServerConfig().UseRegionStorage {
				syncer.StartSyncWithLeader(leader.GetClientUrls()[0])
			}
			log.Info("start watch leader", zap.Stringer("leader", leader))
			s.member.WatchLeader(s.serverLoopCtx, leader, rev)
			syncer.StopSyncWithLeader()
			log.Info("leader changed, try to campaign leader")
		}

		etcdLeader := s.member.GetEtcdLeader()
		if etcdLeader != s.member.ID() {
			log.Info("skip campaign leader and check later",
				zap.String("server-name", s.Name()),
				zap.Uint64("etcd-leader-id", etcdLeader))
			time.Sleep(200 * time.Millisecond)
			continue
		}
		s.campaignLeader()
	}
}

func (s *Server) campaignLeader() {
	log.Info("start to campaign leader", zap.String("campaign-leader-name", s.Name()))

	lease := member.NewLeaderLease(s.client)
	defer lease.Close()
	if err := s.member.CampaignLeader(lease, s.cfg.LeaderLease); err != nil {
		log.Error("campaign leader meet error", zap.Error(err))
		return
	}

	// Start keepalive and enable TSO service.
	// TSO service is strictly enabled/disabled by leader lease for 2 reasons:
	//   1. lease based approach is not affected by thread pause, slow runtime schedule, etc.
	//   2. load region could be slow. Based on lease we can recover TSO service faster.

	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()
	go lease.KeepAlive(ctx)
	log.Info("campaign leader ok", zap.String("campaign-leader-name", s.Name()))

	log.Debug("sync timestamp for tso")
	if err := s.tso.SyncTimestamp(lease); err != nil {
		log.Error("failed to sync timestamp", zap.Error(err))
		return
	}
	defer s.tso.ResetTimestamp()

	err := s.reloadConfigFromKV()
	if err != nil {
		log.Error("failed to reload configuration", zap.Error(err))
		return
	}
	// Try to create raft cluster.
	err = s.createRaftCluster()
	if err != nil {
		log.Error("failed to create raft cluster", zap.Error(err))
		return
	}
	defer s.stopRaftCluster()

	s.member.EnableLeader()
	defer s.member.DisableLeader()

	CheckPDVersion(s.scheduleOpt)
	log.Info("PD cluster leader is ready to serve", zap.String("leader-name", s.Name()))

	tsTicker := time.NewTicker(tso.UpdateTimestampStep)
	defer tsTicker.Stop()
	leaderTicker := time.NewTicker(leaderTickInterval)
	defer leaderTicker.Stop()

	for {
		select {
		case <-leaderTicker.C:
			if lease.IsExpired() {
				log.Info("lease expired, leader step down")
				return
			}
			etcdLeader := s.member.GetEtcdLeader()
			if etcdLeader != s.member.ID() {
				log.Info("etcd leader changed, resigns leadership", zap.String("old-leader-name", s.Name()))
				return
			}
		case <-tsTicker.C:
			if err = s.tso.UpdateTimestamp(); err != nil {
				log.Error("failed to update timestamp", zap.Error(err))
				return
			}
		case <-ctx.Done():
			// Server is closed and it should return nil.
			log.Info("server is closed")
			return
		}
	}
}

func (s *Server) etcdLeaderLoop() {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()
	for {
		select {
		case <-time.After(s.cfg.LeaderPriorityCheckInterval.Duration):
			s.member.CheckPriority(ctx)
		case <-ctx.Done():
			log.Info("server is closed, exit etcd leader loop")
			return
		}
	}
}

func (s *Server) configCheckLoop() {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()

	ticker := time.NewTicker(ConfigCheckInterval)
	defer ticker.Stop()
	for {
		// wait leader
		leader := s.GetLeader()
		if leader != nil {
			var err error
			securityConfig := s.GetSecurityConfig()
			s.configClient, err = pd.NewConfigClientWithContext(ctx, s.GetEndpoints(), pd.SecurityOption{
				CAPath:   securityConfig.CAPath,
				CertPath: securityConfig.CertPath,
				KeyPath:  securityConfig.KeyPath,
			})
			if err != nil {
				log.Error("failed to create config client", zap.Error(err))
				return
			}
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			log.Info("config check stop running")
			return
		}
	}

	addr := s.GetAddr()
	u, err := url.Parse(addr)
	if err != nil {
		log.Error("failed to parse url", zap.Error(err))
		return
	}
	compoenntID := u.Host + u.Path
	version := s.GetConfigVersion()
	var config bytes.Buffer
	if err := toml.NewEncoder(&config).Encode(*s.GetConfig()); err != nil {
		log.Error("failed to encode config", zap.Error(err))
		return
	}
	if err := s.createComponentConfig(ctx, version, compoenntID, config.String()); err != nil {
		log.Error("failed to create config", zap.Error(err))
		return
	}

	for {
		select {
		case <-ctx.Done():
			log.Info("config check has been stopped")
			return
		case <-ticker.C:
			if err := s.updateConfig(ctx, compoenntID); err != nil {
				log.Error("failed to update config", zap.Error(err))
			}

			rc := s.GetRaftCluster()
			if s.GetMember().IsLeader() && rc != nil {
				if !rc.GetConfigCheck() {
					rc.SetConfigCheck()
				}
			}
		}
	}
}

func (s *Server) updateConfig(ctx context.Context, compoenntID string) error {
	version := s.GetConfigVersion()
	config, err := s.getComponentConfig(ctx, version, compoenntID)
	if err != nil {
		return err
	}
	if config == "" {
		return nil
	}
	return s.updateComponentConfig(config)
}

func (s *Server) createComponentConfig(ctx context.Context, version *configpb.Version, componentID, config string) error {
	status, v, config, err := s.configClient.Create(ctx, version, Component, componentID, config)
	if err != nil {
		return err
	}
	switch status.GetCode() {
	case configpb.StatusCode_OK, configpb.StatusCode_WRONG_VERSION:
		s.setConfigVersion(v)
		s.updateComponentConfig(config)
	case configpb.StatusCode_UNKNOWN:
		return errors.Errorf("unknown error: %v", status.GetMessage())
	}
	return nil
}

func (s *Server) getComponentConfig(ctx context.Context, version *configpb.Version, componentID string) (string, error) {
	status, v, cfg, err := s.configClient.Get(ctx, version, Component, componentID)
	if err != nil {
		return "", err
	}
	var config string
	switch status.GetCode() {
	case configpb.StatusCode_OK:
	case configpb.StatusCode_WRONG_VERSION:
		config = cfg
		s.setConfigVersion(v)
	case configpb.StatusCode_COMPONENT_ID_NOT_FOUND:
		return "", errors.Errorf("component ID not found: %v", componentID)
	case configpb.StatusCode_COMPONENT_NOT_FOUND:
		return "", errors.Errorf("component not found: %v", Component)
	case configpb.StatusCode_UNKNOWN:
		return "", errors.Errorf("unknown error: %v", status.GetMessage())
	}
	return config, nil
}

// TODO: support more config item
func (s *Server) updateComponentConfig(cfg string) error {
	new := &config.Config{}
	var saveFile bool
	if _, err := toml.Decode(cfg, &new); err != nil {
		return err
	}
	var err error
	if !reflect.DeepEqual(s.GetScheduleConfig(), &new.Schedule) {
		if err = new.Schedule.Validate(); err != nil {
			return err
		}
		err = s.SetScheduleConfig(new.Schedule)
		saveFile = true
	}

	if !reflect.DeepEqual(s.GetReplicationConfig(), &new.Replication) {
		if err = new.Replication.Validate(); err != nil {
			return err
		}
		err = s.SetReplicationConfig(new.Replication)
		saveFile = true
	}

	if !reflect.DeepEqual(s.GetPDServerConfig(), &new.PDServerCfg) {
		err = s.SetPDServerConfig(new.PDServerCfg)
		saveFile = true
	}

	if !reflect.DeepEqual(s.GetClusterVersion(), new.ClusterVersion) {
		err = s.SetClusterVersion(new.ClusterVersion.String())
		saveFile = true
	}

	if !reflect.DeepEqual(s.GetLabelProperty(), new.LabelProperty) {
		err = s.SetLabelPropertyConfig(new.LabelProperty)
		saveFile = true
	}

	if !reflect.DeepEqual(s.GetLogConfig(), &new.Log) {
		err = s.SetLogConfig(new.Log)
		saveFile = true
	}
	if err != nil {
		return err
	}

	if saveFile {
		return s.cfg.RewriteFile(new)
	}
	return nil
}

func (s *Server) reloadConfigFromKV() error {
	err := s.scheduleOpt.Reload(s.storage)
	if err != nil {
		return err
	}
	if s.scheduleOpt.LoadPDServerConfig().UseRegionStorage {
		s.storage.SwitchToRegionStorage()
		log.Info("server enable region storage")
	} else {
		s.storage.SwitchToDefaultStorage()
		log.Info("server disable region storage")
	}

	// The request only valid when there is a leader.
	// And before the a PD becomes a leader it will firstly reload the config.
	if s.cfg.EnableDynamicConfig {
		err = s.cfgManager.Reload(s.storage)
		return err
	}

	return nil
}
