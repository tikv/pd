// Copyright 2016 TiKV Project Authors.
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
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pingcap/sysutil"
	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/pkg/audit"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/pkg/grpcutil"
	"github.com/tikv/pd/pkg/jsonutil"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/pkg/ratelimit"
	"github.com/tikv/pd/pkg/systimemon"
	"github.com/tikv/pd/pkg/tsoutil"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/encryptionkm"
	"github.com/tikv/pd/server/gc"
	"github.com/tikv/pd/server/id"
	"github.com/tikv/pd/server/keyspace"
	"github.com/tikv/pd/server/member"
	syncer "github.com/tikv/pd/server/region_syncer"
	"github.com/tikv/pd/server/replication"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/hbstream"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/storage"
	"github.com/tikv/pd/server/storage/endpoint"
	"github.com/tikv/pd/server/storage/kv"
	"github.com/tikv/pd/server/tso"
	"github.com/tikv/pd/server/versioninfo"
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
	// idAllocPath for idAllocator to save persistent window's end.
	idAllocPath  = "alloc_id"
	idAllocLabel = "idalloc"

	recoveringMarkPath = "cluster/markers/snapshot-recovering"

	lostPDLeaderMaxTimeoutSecs   = 10
	lostPDLeaderReElectionFactor = 10
)

// EtcdStartTimeout the timeout of the startup etcd.
var EtcdStartTimeout = time.Minute * 5

// Server is the pd server.
// nolint
type Server struct {
	diagnosticspb.DiagnosticsServer

	// Server state.
	isServing int64

	// Server start timestamp
	startTimestamp int64

	// Configs and initial fields.
	cfg                             *config.Config
	serviceMiddlewareCfg            *config.ServiceMiddlewareConfig
	etcdCfg                         *embed.Config
	serviceMiddlewarePersistOptions *config.ServiceMiddlewarePersistOptions
	persistOptions                  *config.PersistOptions
	handler                         *Handler

	ctx              context.Context
	serverLoopCtx    context.Context
	serverLoopCancel func()
	serverLoopWg     sync.WaitGroup

	// for PD leader election.
	member *member.Member
	// etcd client
	client *clientv3.Client
	// electionClient is used for leader election.
	electionClient *clientv3.Client
	// http client
	httpClient *http.Client
	clusterID  uint64 // pd cluster id.
	rootPath   string

	// Server services.
	// for id allocator, we can use one allocator for
	// store, region and peer, because we just need
	// a unique ID.
	idAllocator id.Allocator
	// for encryption
	encryptionKeyManager *encryptionkm.KeyManager
	// for storage operation.
	storage storage.Storage
	// safepoint manager
	gcSafePointManager *gc.SafePointManager
	// keyspace manager
	keyspaceManager *keyspace.Manager
	// for basicCluster operation.
	basicCluster *core.BasicCluster
	// for tso.
	tsoAllocatorManager *tso.AllocatorManager
	// for raft cluster
	cluster *cluster.RaftCluster
	// For async region heartbeat.
	hbStreams *hbstream.HeartbeatStreams
	// Zap logger
	lg       *zap.Logger
	logProps *log.ZapProperties

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()

	// hot region history info storeage
	hotRegionStorage *storage.HotRegionStorage
	// Store as map[string]*grpc.ClientConn
	clientConns sync.Map
	// tsoDispatcher is used to dispatch different TSO requests to
	// the corresponding forwarding TSO channel.
	tsoDispatcher sync.Map /* Store as map[string]chan *tsoRequest */

	serviceRateLimiter *ratelimit.Limiter
	serviceLabels      map[string][]apiutil.AccessPath
	apiServiceLabelMap map[apiutil.AccessPath]string

	serviceAuditBackendLabels map[string]*audit.BackendLabels

	auditBackends []audit.Backend
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
			return nil, errs.ErrAPIInformationInvalid.FastGenByArgs(info.Name, info.Version)
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
			return nil, errs.ErrServiceRegistered.FastGenByArgs(pathPrefix)
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
	rand.New(rand.NewSource(time.Now().UnixNano()))
	serviceMiddlewareCfg := config.NewServiceMiddlewareConfig()

	s := &Server{
		cfg:                             cfg,
		persistOptions:                  config.NewPersistOptions(cfg),
		serviceMiddlewareCfg:            serviceMiddlewareCfg,
		serviceMiddlewarePersistOptions: config.NewServiceMiddlewarePersistOptions(serviceMiddlewareCfg),
		member:                          &member.Member{},
		ctx:                             ctx,
		startTimestamp:                  time.Now().Unix(),
		DiagnosticsServer:               sysutil.NewDiagnosticsServer(cfg.Log.File.Filename),
	}
	s.handler = newHandler(s)

	// create audit backend
	s.auditBackends = []audit.Backend{
		audit.NewLocalLogBackend(true),
		audit.NewPrometheusHistogramBackend(serviceAuditHistogram, false),
	}
	s.serviceRateLimiter = ratelimit.NewLimiter()
	s.serviceAuditBackendLabels = make(map[string]*audit.BackendLabels)
	s.serviceRateLimiter = ratelimit.NewLimiter()
	s.serviceLabels = make(map[string][]apiutil.AccessPath)
	s.apiServiceLabelMap = make(map[apiutil.AccessPath]string)

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
		grpcServer := &GrpcServer{Server: s}
		pdpb.RegisterPDServer(gs, grpcServer)
		keyspacepb.RegisterKeyspaceServer(gs, &KeyspaceServer{GrpcServer: grpcServer})
		diagnosticspb.RegisterDiagnosticsServer(gs, s)
	}
	s.etcdCfg = etcdCfg
	s.lg = cfg.GetZapLogger()
	s.logProps = cfg.GetZapLogProperties()
	return s, nil
}

func (s *Server) startEtcd(ctx context.Context) error {
	newCtx, cancel := context.WithTimeout(ctx, EtcdStartTimeout)
	defer cancel()

	etcd, err := embed.StartEtcd(s.etcdCfg)
	if err != nil {
		return errs.ErrStartEtcd.Wrap(err).GenWithStackByCause()
	}

	// Check cluster ID
	urlMap, err := types.NewURLsMap(s.cfg.InitialCluster)
	if err != nil {
		return errs.ErrEtcdURLMap.Wrap(err).GenWithStackByCause()
	}
	tlsConfig, err := s.cfg.Security.ToTLSConfig()
	if err != nil {
		return err
	}

	if err = etcdutil.CheckClusterID(etcd.Server.Cluster().ID(), urlMap, tlsConfig); err != nil {
		return err
	}

	select {
	// Wait etcd until it is ready to use
	case <-etcd.Server.ReadyNotify():
	case <-newCtx.Done():
		return errs.ErrCancelStartEtcd.FastGenByArgs()
	}

	endpoints := []string{s.etcdCfg.AdvertiseClientUrls[0].String()}
	log.Info("create etcd v3 client", zap.Strings("endpoints", endpoints), zap.Reflect("cert", s.cfg.Security))

	lgc := zap.NewProductionConfig()
	lgc.Encoding = log.ZapEncodingName
	clientConfig := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: etcdTimeout,
		TLS:         tlsConfig,
		LogConfig:   &lgc,
	}
	client, err := clientv3.New(clientConfig)
	if err != nil {
		return errs.ErrNewEtcdClient.Wrap(err).GenWithStackByCause()
	}

	s.electionClient, err = clientv3.New(clientConfig)
	if err != nil {
		return errs.ErrNewEtcdClient.Wrap(err).GenWithStackByCause()
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
	// FIXME: Currently, there is no timeout set for certain requests, such as GetRegions,
	// which may take a significant amount of time. However, it might be necessary to
	// define an appropriate timeout in the future.
	httpCli := &http.Client{}
	if tlsConfig != nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = tlsConfig
		httpCli.Transport = transport
	}
	s.httpClient = httpCli

	failpoint.Inject("memberNil", func() {
		time.Sleep(1500 * time.Millisecond)
	})
	s.member = member.NewMember(etcd, s.electionClient, etcdServerID)
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
	serverInfo.WithLabelValues(versioninfo.PDReleaseVersion, versioninfo.PDGitHash).Set(float64(time.Now().Unix()))

	s.rootPath = path.Join(pdRootPath, strconv.FormatUint(s.clusterID, 10))
	s.member.MemberInfo(s.cfg, s.Name(), s.rootPath)
	s.member.SetMemberDeployPath(s.member.ID())
	s.member.SetMemberBinaryVersion(s.member.ID(), versioninfo.PDReleaseVersion)
	s.member.SetMemberGitHash(s.member.ID(), versioninfo.PDGitHash)
	s.idAllocator = id.NewAllocator(&id.AllocatorParams{
		Client:    s.client,
		RootPath:  s.rootPath,
		AllocPath: idAllocPath,
		Label:     idAllocLabel,
		Member:    s.member.MemberValue(),
	})
	s.tsoAllocatorManager = tso.NewAllocatorManager(
		s.member, s.rootPath, s.cfg,
		func() time.Duration { return s.persistOptions.GetMaxResetTSGap() })
	// Set up the Global TSO Allocator here, it will be initialized once the PD campaigns leader successfully.
	s.tsoAllocatorManager.SetUpAllocator(ctx, tso.GlobalDCLocation, s.member.GetLeadership())
	// When disabled the Local TSO, we should clean up the Local TSO Allocator's meta info written in etcd if it exists.
	if !s.cfg.EnableLocalTSO {
		if err = s.tsoAllocatorManager.CleanUpDCLocation(); err != nil {
			return err
		}
	}
	if zone, exist := s.cfg.Labels[config.ZoneLabel]; exist && zone != "" && s.cfg.EnableLocalTSO {
		if err = s.tsoAllocatorManager.SetLocalTSOConfig(zone); err != nil {
			return err
		}
	}
	s.encryptionKeyManager, err = encryptionkm.NewKeyManager(s.client, &s.cfg.Security.Encryption)
	if err != nil {
		return err
	}
	regionStorage, err := storage.NewStorageWithLevelDBBackend(ctx, filepath.Join(s.cfg.DataDir, "region-meta"), s.encryptionKeyManager)
	if err != nil {
		return err
	}
	defaultStorage := storage.NewStorageWithEtcdBackend(s.client, s.rootPath)
	s.storage = storage.NewCoreStorage(defaultStorage, regionStorage)
	s.gcSafePointManager = gc.NewSafePointManager(s.storage)
	keyspaceIDAllocator := id.NewAllocator(&id.AllocatorParams{
		Client:    s.client,
		RootPath:  s.rootPath,
		AllocPath: endpoint.KeyspaceIDAlloc(),
		Label:     keyspace.AllocLabel,
		Member:    s.member.MemberValue(),
		Step:      keyspace.AllocStep,
	})
	s.keyspaceManager, err = keyspace.NewKeyspaceManager(s.storage, keyspaceIDAllocator)
	if err != nil {
		return err
	}
	s.basicCluster = core.NewBasicCluster()
	s.cluster = cluster.NewRaftCluster(ctx, s.clusterID, syncer.NewRegionSyncer(s), s.client, s.httpClient)
	s.hbStreams = hbstream.NewHeartbeatStreams(ctx, s.clusterID, s.cluster)
	// initial hot_region_storage in here.
	s.hotRegionStorage, err = storage.NewHotRegionsStorage(
		ctx, filepath.Join(s.cfg.DataDir, "hot-region"), s.encryptionKeyManager, s.handler)
	if err != nil {
		return err
	}
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
		if err := s.client.Close(); err != nil {
			log.Error("close etcd client meet error", errs.ZapError(errs.ErrCloseEtcdClient, err))
		}
	}
	if s.electionClient != nil {
		if err := s.electionClient.Close(); err != nil {
			log.Error("close election client meet error", errs.ZapError(errs.ErrCloseEtcdClient, err))
		}
	}

	if s.httpClient != nil {
		s.httpClient.CloseIdleConnections()
	}

	if s.member.Etcd() != nil {
		s.member.Close()
	}

	if s.hbStreams != nil {
		s.hbStreams.Close()
	}
	if err := s.storage.Close(); err != nil {
		log.Error("close storage meet error", errs.ZapError(err))
	}

	if err := s.hotRegionStorage.Close(); err != nil {
		log.Error("close hot region storage meet error", errs.ZapError(err))
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
	go systimemon.StartMonitor(s.ctx, time.Now, func() {
		log.Error("system time jumps backward", errs.ZapError(errs.ErrIncorrectSystemTime))
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

// SetServiceAuditBackendForHTTP is used to register service audit config for HTTP.
func (s *Server) SetServiceAuditBackendForHTTP(route *mux.Route, labels ...string) {
	if len(route.GetName()) == 0 {
		return
	}
	if len(labels) > 0 {
		s.SetServiceAuditBackendLabels(route.GetName(), labels)
	}
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
	s.serverLoopWg.Add(5)
	go s.leaderLoop()
	go s.etcdLeaderLoop()
	go s.serverMetricsLoop()
	go s.tsoAllocatorLoop()
	go s.encryptionKeyManagerLoop()
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

// tsoAllocatorLoop is used to run the TSO Allocator updating daemon.
func (s *Server) tsoAllocatorLoop() {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()
	s.tsoAllocatorManager.AllocatorDaemon(ctx)
	log.Info("server is closed, exit allocator loop")
}

// encryptionKeyManagerLoop is used to start monitor encryption key changes.
func (s *Server) encryptionKeyManagerLoop() {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()
	s.encryptionKeyManager.StartBackgroundLoop(ctx)
	log.Info("server is closed, exist encryption key manager loop")
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
		MaxPeerCount: uint32(s.persistOptions.GetMaxReplicas()),
	}

	// Set cluster meta
	clusterValue, err := clusterMeta.Marshal()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	clusterRootPath := endpoint.ClusterRootPath(s.rootPath)

	var ops []clientv3.Op
	ops = append(ops, clientv3.OpPut(clusterRootPath, string(clusterValue)))

	// Set bootstrap time
	// Because we will write the cluster meta into etcd directly,
	// so we need to handle the root key path manually here.
	bootstrapKey := endpoint.AppendToRootPath(s.rootPath, endpoint.ClusterBootstrapTimeKey())
	nano := time.Now().UnixNano()

	timeData := typeutil.Uint64ToBytes(uint64(nano))
	ops = append(ops, clientv3.OpPut(bootstrapKey, string(timeData)))

	// Set store meta
	storeMeta := req.GetStore()
	storePath := endpoint.AppendToRootPath(s.rootPath, endpoint.StorePath(storeMeta.GetId()))
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
	regionPath := endpoint.AppendToRootPath(s.rootPath, endpoint.RegionPath(req.GetRegion().GetId()))
	ops = append(ops, clientv3.OpPut(regionPath, string(regionValue)))

	// TODO: we must figure out a better way to handle bootstrap failed, maybe intervene manually.
	bootstrapCmp := clientv3.Compare(clientv3.CreateRevision(clusterRootPath), "=", 0)
	resp, err := kv.NewSlowLogTxn(s.client).If(bootstrapCmp).Then(ops...).Commit()
	if err != nil {
		return nil, errs.ErrEtcdTxnInternal.Wrap(err).GenWithStackByCause()
	}
	if !resp.Succeeded {
		log.Warn("cluster already bootstrapped", zap.Uint64("cluster-id", clusterID))
		return nil, errs.ErrEtcdTxnConflict.FastGenByArgs()
	}

	log.Info("bootstrap cluster ok", zap.Uint64("cluster-id", clusterID))
	err = s.storage.SaveRegion(req.GetRegion())
	if err != nil {
		log.Warn("save the bootstrap region failed", errs.ZapError(err))
	}
	err = s.storage.Flush()
	if err != nil {
		log.Warn("flush the bootstrap region failed", errs.ZapError(err))
	}

	if err := s.cluster.Start(s); err != nil {
		return nil, err
	}

	return &pdpb.BootstrapResponse{
		ReplicationStatus: s.cluster.GetReplicationMode().GetReplicationStatus(),
	}, nil
}

func (s *Server) createRaftCluster() error {
	if s.cluster.IsRunning() {
		return nil
	}

	return s.cluster.Start(s)
}

func (s *Server) stopRaftCluster() {
	failpoint.Inject("raftclusterIsBusy", func() {})
	s.cluster.Stop()
}

// GetAddr returns the server urls for clients.
func (s *Server) GetAddr() string {
	return s.cfg.AdvertiseClientUrls
}

// GetClientScheme returns the client URL scheme
func (s *Server) GetClientScheme() string {
	if len(s.cfg.Security.CertPath) == 0 && len(s.cfg.Security.KeyPath) == 0 {
		return "http"
	}
	return "https"
}

// GetMemberInfo returns the server member information.
func (s *Server) GetMemberInfo() *pdpb.Member {
	return typeutil.DeepClone(s.member.Member(), core.MemberFactory)
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

// GetHTTPClient returns builtin etcd client.
func (s *Server) GetHTTPClient() *http.Client {
	return s.httpClient
}

// GetLeader returns the leader of PD cluster(i.e the PD leader).
func (s *Server) GetLeader() *pdpb.Member {
	return s.member.GetLeader()
}

// GetMember returns the member of server.
func (s *Server) GetMember() *member.Member {
	return s.member
}

// GetStorage returns the backend storage of server.
func (s *Server) GetStorage() storage.Storage {
	return s.storage
}

// GetHistoryHotRegionStorage returns the backend storage of historyHotRegion.
func (s *Server) GetHistoryHotRegionStorage() *storage.HotRegionStorage {
	return s.hotRegionStorage
}

// SetStorage changes the storage only for test purpose.
// When we use it, we should prevent calling GetStorage, otherwise, it may cause a data race problem.
func (s *Server) SetStorage(storage storage.Storage) {
	s.storage = storage
}

// GetBasicCluster returns the basic cluster of server.
func (s *Server) GetBasicCluster() *core.BasicCluster {
	return s.basicCluster
}

// GetPersistOptions returns the schedule option.
func (s *Server) GetPersistOptions() *config.PersistOptions {
	return s.persistOptions
}

// GetServiceMiddlewarePersistOptions returns the service middleware persist option.
func (s *Server) GetServiceMiddlewarePersistOptions() *config.ServiceMiddlewarePersistOptions {
	return s.serviceMiddlewarePersistOptions
}

// GetHBStreams returns the heartbeat streams.
func (s *Server) GetHBStreams() *hbstream.HeartbeatStreams {
	return s.hbStreams
}

// GetAllocator returns the ID allocator of server.
func (s *Server) GetAllocator() id.Allocator {
	return s.idAllocator
}

// GetTSOAllocatorManager returns the manager of TSO Allocator.
func (s *Server) GetTSOAllocatorManager() *tso.AllocatorManager {
	return s.tsoAllocatorManager
}

// GetKeyspaceManager returns the keyspace manager of server.
func (s *Server) GetKeyspaceManager() *keyspace.Manager {
	return s.keyspaceManager
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

// GetMembers returns PD server list.
func (s *Server) GetMembers() ([]*pdpb.Member, error) {
	if s.IsClosed() {
		return nil, errs.ErrServerNotStarted.FastGenByArgs()
	}
	return cluster.GetMembers(s.GetClient())
}

// GetServiceMiddlewareConfig gets the service middleware config information.
func (s *Server) GetServiceMiddlewareConfig() *config.ServiceMiddlewareConfig {
	cfg := s.serviceMiddlewareCfg.Clone()
	cfg.AuditConfig = *s.serviceMiddlewarePersistOptions.GetAuditConfig().Clone()
	cfg.RateLimitConfig = *s.serviceMiddlewarePersistOptions.GetRateLimitConfig().Clone()
	return cfg
}

// SetEnableLocalTSO sets enable-local-tso flag of Server. This function only for test.
func (s *Server) SetEnableLocalTSO(enableLocalTSO bool) {
	s.cfg.EnableLocalTSO = enableLocalTSO
}

// GetConfig gets the config information.
func (s *Server) GetConfig() *config.Config {
	cfg := s.cfg.Clone()
	cfg.Schedule = *s.persistOptions.GetScheduleConfig().Clone()
	cfg.Replication = *s.persistOptions.GetReplicationConfig().Clone()
	cfg.PDServerCfg = *s.persistOptions.GetPDServerConfig().Clone()
	cfg.ReplicationMode = *s.persistOptions.GetReplicationModeConfig()
	cfg.LabelProperty = s.persistOptions.GetLabelPropertyConfig().Clone()
	cfg.ClusterVersion = *s.persistOptions.GetClusterVersion()
	if s.storage == nil {
		return cfg
	}
	sches, configs, err := s.storage.LoadAllScheduleConfig()
	if err != nil {
		return cfg
	}
	payload := make(map[string]interface{})
	for i, sche := range sches {
		var config interface{}
		err := schedule.DecodeConfig([]byte(configs[i]), &config)
		if err != nil {
			log.Error("failed to decode scheduler config",
				zap.String("config", configs[i]),
				zap.String("scheduler", sche),
				errs.ZapError(err))
			continue
		}
		payload[sche] = config
	}
	cfg.Schedule.SchedulersPayload = payload
	return cfg
}

// GetScheduleConfig gets the balance config information.
func (s *Server) GetScheduleConfig() *config.ScheduleConfig {
	return s.persistOptions.GetScheduleConfig().Clone()
}

// SetScheduleConfig sets the balance config information.
func (s *Server) SetScheduleConfig(cfg config.ScheduleConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	if err := cfg.Deprecated(); err != nil {
		return err
	}
	old := s.persistOptions.GetScheduleConfig()
	cfg.SchedulersPayload = nil
	s.persistOptions.SetScheduleConfig(&cfg)
	if err := s.persistOptions.Persist(s.storage); err != nil {
		s.persistOptions.SetScheduleConfig(old)
		log.Error("failed to update schedule config",
			zap.Reflect("new", cfg),
			zap.Reflect("old", old),
			errs.ZapError(err))
		return err
	}
	log.Info("schedule config is updated", zap.Reflect("new", cfg), zap.Reflect("old", old))
	return nil
}

// GetReplicationConfig get the replication config.
func (s *Server) GetReplicationConfig() *config.ReplicationConfig {
	return s.persistOptions.GetReplicationConfig().Clone()
}

// SetReplicationConfig sets the replication config.
func (s *Server) SetReplicationConfig(cfg config.ReplicationConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	old := s.persistOptions.GetReplicationConfig()
	if cfg.EnablePlacementRules != old.EnablePlacementRules {
		rc := s.GetRaftCluster()
		if rc == nil {
			return errs.ErrNotBootstrapped.GenWithStackByArgs()
		}
		if cfg.EnablePlacementRules {
			// initialize rule manager.
			if err := rc.GetRuleManager().Initialize(int(cfg.MaxReplicas), cfg.LocationLabels, cfg.IsolationLevel); err != nil {
				return err
			}
		} else {
			// NOTE: can be removed after placement rules feature is enabled by default.
			for _, s := range rc.GetStores() {
				if !s.IsRemoved() && s.IsTiFlash() {
					return errors.New("cannot disable placement rules with TiFlash nodes")
				}
			}
		}
	}

	var rule *placement.Rule
	if cfg.EnablePlacementRules {
		rc := s.GetRaftCluster()
		if rc == nil {
			return errs.ErrNotBootstrapped.GenWithStackByArgs()
		}
		// replication.MaxReplicas won't work when placement rule is enabled and not only have one default rule.
		defaultRule := rc.GetRuleManager().GetRule("pd", "default")

		CheckInDefaultRule := func() error {
			// replication config won't work when placement rule is enabled and exceeds one default rule
			if !(defaultRule != nil &&
				len(defaultRule.StartKey) == 0 && len(defaultRule.EndKey) == 0) {
				return errors.New("cannot update MaxReplicas, LocationLabels or IsolationLevel when placement rules feature is enabled and not only default rule exists, please update rule instead")
			}
			if !(defaultRule.Count == int(old.MaxReplicas) && typeutil.StringsEqual(defaultRule.LocationLabels, []string(old.LocationLabels)) && defaultRule.IsolationLevel == old.IsolationLevel) {
				return errors.New("cannot to update replication config, the default rules do not consistent with replication config, please update rule instead")
			}

			return nil
		}

		if !(cfg.MaxReplicas == old.MaxReplicas && typeutil.StringsEqual(cfg.LocationLabels, old.LocationLabels) && cfg.IsolationLevel == old.IsolationLevel) {
			if err := CheckInDefaultRule(); err != nil {
				return err
			}
			rule = defaultRule
		}
	}

	if rule != nil {
		rule.Count = int(cfg.MaxReplicas)
		rule.LocationLabels = cfg.LocationLabels
		rule.IsolationLevel = cfg.IsolationLevel
		rc := s.GetRaftCluster()
		if rc == nil {
			return errs.ErrNotBootstrapped.GenWithStackByArgs()
		}
		if err := rc.GetRuleManager().SetRule(rule); err != nil {
			log.Error("failed to update rule count",
				errs.ZapError(err))
			return err
		}
	}

	s.persistOptions.SetReplicationConfig(&cfg)
	if err := s.persistOptions.Persist(s.storage); err != nil {
		s.persistOptions.SetReplicationConfig(old)
		if rule != nil {
			rule.Count = int(old.MaxReplicas)
			rc := s.GetRaftCluster()
			if rc == nil {
				return errs.ErrNotBootstrapped.GenWithStackByArgs()
			}
			if e := rc.GetRuleManager().SetRule(rule); e != nil {
				log.Error("failed to roll back count of rule when update replication config", errs.ZapError(e))
			}
		}
		log.Error("failed to update replication config",
			zap.Reflect("new", cfg),
			zap.Reflect("old", old),
			errs.ZapError(err))
		return err
	}
	log.Info("replication config is updated", zap.Reflect("new", cfg), zap.Reflect("old", old))
	return nil
}

// GetAuditConfig gets the audit config information.
func (s *Server) GetAuditConfig() *config.AuditConfig {
	return s.serviceMiddlewarePersistOptions.GetAuditConfig().Clone()
}

// SetAuditConfig sets the audit config.
func (s *Server) SetAuditConfig(cfg config.AuditConfig) error {
	old := s.serviceMiddlewarePersistOptions.GetAuditConfig()
	s.serviceMiddlewarePersistOptions.SetAuditConfig(&cfg)
	if err := s.serviceMiddlewarePersistOptions.Persist(s.storage); err != nil {
		s.serviceMiddlewarePersistOptions.SetAuditConfig(old)
		log.Error("failed to update Audit config",
			zap.Reflect("new", cfg),
			zap.Reflect("old", old),
			errs.ZapError(err))
		return err
	}
	log.Info("Audit config is updated", zap.Reflect("new", cfg), zap.Reflect("old", old))
	return nil
}

// UpdateRateLimitConfig is used to update rate-limit config which will reserve old limiter-config
func (s *Server) UpdateRateLimitConfig(key, label string, value ratelimit.DimensionConfig) error {
	cfg := s.GetServiceMiddlewareConfig()
	rateLimitCfg := make(map[string]ratelimit.DimensionConfig)
	for label, item := range cfg.LimiterConfig {
		rateLimitCfg[label] = item
	}
	rateLimitCfg[label] = value
	return s.UpdateRateLimit(&cfg.RateLimitConfig, key, &rateLimitCfg)
}

// UpdateRateLimit is used to update rate-limit config which will overwrite limiter-config
func (s *Server) UpdateRateLimit(cfg *config.RateLimitConfig, key string, value interface{}) error {
	updated, found, err := jsonutil.AddKeyValue(cfg, key, value)
	if err != nil {
		return err
	}

	if !found {
		return errors.Errorf("config item %s not found", key)
	}

	if updated {
		err = s.SetRateLimitConfig(*cfg)
	}
	return err
}

// GetRateLimitConfig gets the rate limit config information.
func (s *Server) GetRateLimitConfig() *config.RateLimitConfig {
	return s.serviceMiddlewarePersistOptions.GetRateLimitConfig().Clone()
}

// SetRateLimitConfig sets the rate limit config.
func (s *Server) SetRateLimitConfig(cfg config.RateLimitConfig) error {
	old := s.serviceMiddlewarePersistOptions.GetRateLimitConfig()
	s.serviceMiddlewarePersistOptions.SetRateLimitConfig(&cfg)
	if err := s.serviceMiddlewarePersistOptions.Persist(s.storage); err != nil {
		s.serviceMiddlewarePersistOptions.SetRateLimitConfig(old)
		log.Error("failed to update Rate Limit config",
			zap.Reflect("new", cfg),
			zap.Reflect("old", old),
			errs.ZapError(err))
		return err
	}
	log.Info("Rate Limit config is updated", zap.Reflect("new", cfg), zap.Reflect("old", old))
	return nil
}

// GetPDServerConfig gets the balance config information.
func (s *Server) GetPDServerConfig() *config.PDServerConfig {
	return s.persistOptions.GetPDServerConfig().Clone()
}

// SetPDServerConfig sets the server config.
func (s *Server) SetPDServerConfig(cfg config.PDServerConfig) error {
	switch cfg.DashboardAddress {
	case "auto":
	case "none":
	default:
		if !strings.HasPrefix(cfg.DashboardAddress, "http") {
			cfg.DashboardAddress = fmt.Sprintf("%s://%s", s.GetClientScheme(), cfg.DashboardAddress)
		}
		if !cluster.IsClientURL(cfg.DashboardAddress, s.client) {
			return errors.Errorf("%s is not the client url of any member", cfg.DashboardAddress)
		}
	}
	if err := cfg.Validate(); err != nil {
		return err
	}

	old := s.persistOptions.GetPDServerConfig()
	s.persistOptions.SetPDServerConfig(&cfg)
	if err := s.persistOptions.Persist(s.storage); err != nil {
		s.persistOptions.SetPDServerConfig(old)
		log.Error("failed to update PDServer config",
			zap.Reflect("new", cfg),
			zap.Reflect("old", old),
			errs.ZapError(err))
		return err
	}
	log.Info("PD server config is updated", zap.Reflect("new", cfg), zap.Reflect("old", old))
	return nil
}

// SetLabelPropertyConfig sets the label property config.
func (s *Server) SetLabelPropertyConfig(cfg config.LabelPropertyConfig) error {
	old := s.persistOptions.GetLabelPropertyConfig()
	s.persistOptions.SetLabelPropertyConfig(cfg)
	if err := s.persistOptions.Persist(s.storage); err != nil {
		s.persistOptions.SetLabelPropertyConfig(old)
		log.Error("failed to update label property config",
			zap.Reflect("new", cfg),
			zap.Reflect("old", &old),
			errs.ZapError(err))
		return err
	}
	log.Info("label property config is updated", zap.Reflect("new", cfg), zap.Reflect("old", old))
	return nil
}

// SetLabelProperty inserts a label property config.
func (s *Server) SetLabelProperty(typ, labelKey, labelValue string) error {
	s.persistOptions.SetLabelProperty(typ, labelKey, labelValue)
	err := s.persistOptions.Persist(s.storage)
	if err != nil {
		s.persistOptions.DeleteLabelProperty(typ, labelKey, labelValue)
		log.Error("failed to update label property config",
			zap.String("typ", typ),
			zap.String("label-key", labelKey),
			zap.String("label-value", labelValue),
			zap.Reflect("config", s.persistOptions.GetLabelPropertyConfig()),
			errs.ZapError(err))
		return err
	}

	log.Info("label property config is updated", zap.Reflect("config", s.persistOptions.GetLabelPropertyConfig()))
	return nil
}

// DeleteLabelProperty deletes a label property config.
func (s *Server) DeleteLabelProperty(typ, labelKey, labelValue string) error {
	s.persistOptions.DeleteLabelProperty(typ, labelKey, labelValue)
	err := s.persistOptions.Persist(s.storage)
	if err != nil {
		s.persistOptions.SetLabelProperty(typ, labelKey, labelValue)
		log.Error("failed to delete label property config",
			zap.String("typ", typ),
			zap.String("label-key", labelKey),
			zap.String("label-value", labelValue),
			zap.Reflect("config", s.persistOptions.GetLabelPropertyConfig()),
			errs.ZapError(err))
		return err
	}

	log.Info("label property config is deleted", zap.Reflect("config", s.persistOptions.GetLabelPropertyConfig()))
	return nil
}

// GetLabelProperty returns the whole label property config.
func (s *Server) GetLabelProperty() config.LabelPropertyConfig {
	return s.persistOptions.GetLabelPropertyConfig().Clone()
}

// SetClusterVersion sets the version of cluster.
func (s *Server) SetClusterVersion(v string) error {
	version, err := versioninfo.ParseVersion(v)
	if err != nil {
		return err
	}
	old := s.persistOptions.GetClusterVersion()
	s.persistOptions.SetClusterVersion(version)
	err = s.persistOptions.Persist(s.storage)
	if err != nil {
		s.persistOptions.SetClusterVersion(old)
		log.Error("failed to update cluster version",
			zap.String("old-version", old.String()),
			zap.String("new-version", v),
			errs.ZapError(err))
		return err
	}
	log.Info("cluster version is updated", zap.String("new-version", v))
	return nil
}

// GetClusterVersion returns the version of cluster.
func (s *Server) GetClusterVersion() semver.Version {
	return *s.persistOptions.GetClusterVersion()
}

// GetTLSConfig get the security config.
func (s *Server) GetTLSConfig() *grpcutil.TLSConfig {
	return &s.cfg.Security.TLSConfig
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
		MaxPeerCount: uint32(s.persistOptions.GetMaxReplicas()),
	}
}

// GetServerOption gets the option of the server.
func (s *Server) GetServerOption() *config.PersistOptions {
	return s.persistOptions
}

// GetMetaRegions gets meta regions from cluster.
func (s *Server) GetMetaRegions() []*metapb.Region {
	rc := s.GetRaftCluster()
	if rc != nil {
		return rc.GetMetaRegions()
	}
	return nil
}

// GetRegions gets regions from cluster.
func (s *Server) GetRegions() []*core.RegionInfo {
	rc := s.GetRaftCluster()
	if rc != nil {
		return rc.GetRegions()
	}
	return nil
}

// GetServiceLabels returns ApiAccessPaths by given service label
// TODO: this function will be used for updating api rate limit config
func (s *Server) GetServiceLabels(serviceLabel string) []apiutil.AccessPath {
	if apis, ok := s.serviceLabels[serviceLabel]; ok {
		return apis
	}
	return nil
}

// GetAPIAccessServiceLabel returns service label by given access path
// TODO: this function will be used for updating api rate limit config
func (s *Server) GetAPIAccessServiceLabel(accessPath apiutil.AccessPath) string {
	if servicelabel, ok := s.apiServiceLabelMap[accessPath]; ok {
		return servicelabel
	}
	accessPathNoMethod := apiutil.NewAccessPath(accessPath.Path, "")
	if servicelabel, ok := s.apiServiceLabelMap[accessPathNoMethod]; ok {
		return servicelabel
	}
	return ""
}

// AddServiceLabel is used to add the relationship between service label and api access path
// TODO: this function will be used for updating api rate limit config
func (s *Server) AddServiceLabel(serviceLabel string, accessPath apiutil.AccessPath) {
	if slice, ok := s.serviceLabels[serviceLabel]; ok {
		slice = append(slice, accessPath)
		s.serviceLabels[serviceLabel] = slice
	} else {
		slice = []apiutil.AccessPath{accessPath}
		s.serviceLabels[serviceLabel] = slice
	}

	s.apiServiceLabelMap[accessPath] = serviceLabel
}

// GetAuditBackend returns audit backends
func (s *Server) GetAuditBackend() []audit.Backend {
	return s.auditBackends
}

// GetServiceAuditBackendLabels returns audit backend labels by serviceLabel
func (s *Server) GetServiceAuditBackendLabels(serviceLabel string) *audit.BackendLabels {
	return s.serviceAuditBackendLabels[serviceLabel]
}

// SetServiceAuditBackendLabels is used to add audit backend labels for service by service label
func (s *Server) SetServiceAuditBackendLabels(serviceLabel string, labels []string) {
	s.serviceAuditBackendLabels[serviceLabel] = &audit.BackendLabels{Labels: labels}
}

// GetServiceRateLimiter is used to get rate limiter
func (s *Server) GetServiceRateLimiter() *ratelimit.Limiter {
	return s.serviceRateLimiter
}

// IsInRateLimitAllowList returns whethis given service label is in allow lost
func (s *Server) IsInRateLimitAllowList(serviceLabel string) bool {
	return s.serviceRateLimiter.IsInAllowList(serviceLabel)
}

// UpdateServiceRateLimiter is used to update RateLimiter
func (s *Server) UpdateServiceRateLimiter(serviceLabel string, opts ...ratelimit.Option) ratelimit.UpdateStatus {
	return s.serviceRateLimiter.Update(serviceLabel, opts...)
}

// GetClusterStatus gets cluster status.
func (s *Server) GetClusterStatus() (*cluster.Status, error) {
	s.cluster.Lock()
	defer s.cluster.Unlock()
	return s.cluster.LoadClusterStatus()
}

// SetLogLevel sets log level.
func (s *Server) SetLogLevel(level string) error {
	if !isLevelLegal(level) {
		return errors.Errorf("log level %s is illegal", level)
	}
	s.cfg.Log.Level = level
	log.SetLevel(logutil.StringToZapLogLevel(level))
	log.Warn("log level changed", zap.String("level", log.GetLevel().String()))
	return nil
}

func isLevelLegal(level string) bool {
	switch strings.ToLower(level) {
	case "fatal", "error", "warn", "warning", "debug", "info":
		return true
	default:
		return false
	}
}

// GetReplicationModeConfig returns the replication mode config.
func (s *Server) GetReplicationModeConfig() *config.ReplicationModeConfig {
	return s.persistOptions.GetReplicationModeConfig().Clone()
}

// SetReplicationModeConfig sets the replication mode.
func (s *Server) SetReplicationModeConfig(cfg config.ReplicationModeConfig) error {
	if config.NormalizeReplicationMode(cfg.ReplicationMode) == "" {
		return errors.Errorf("invalid replication mode: %v", cfg.ReplicationMode)
	}

	old := s.persistOptions.GetReplicationModeConfig()
	s.persistOptions.SetReplicationModeConfig(&cfg)
	if err := s.persistOptions.Persist(s.storage); err != nil {
		s.persistOptions.SetReplicationModeConfig(old)
		log.Error("failed to update replication mode config",
			zap.Reflect("new", cfg),
			zap.Reflect("old", &old),
			errs.ZapError(err))
		return err
	}
	log.Info("replication mode config is updated", zap.Reflect("new", cfg), zap.Reflect("old", old))

	rc := s.GetRaftCluster()
	if rc != nil {
		err := rc.GetReplicationMode().UpdateConfig(cfg)
		if err != nil {
			log.Warn("failed to update replication mode", errs.ZapError(err))
			// revert to old config
			// NOTE: since we can't put the 2 storage mutations in a batch, it
			// is possible that memory and persistent data become different
			// (when below revert fail). They will become the same after PD is
			// restart or PD leader is changed.
			s.persistOptions.SetReplicationModeConfig(old)
			revertErr := s.persistOptions.Persist(s.storage)
			if revertErr != nil {
				log.Error("failed to revert replication mode persistent config", errs.ZapError(revertErr))
			}
		}
		return err
	}

	return nil
}

func (s *Server) leaderLoop() {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	for {
		if s.IsClosed() {
			log.Info("server is closed, return pd leader loop")
			return
		}

		leader, rev, checkAgain := s.member.CheckLeader()
		// add failpoint to test leader check go to stuck.
		failpoint.Inject("leaderLoopCheckAgain", func(val failpoint.Value) {
			memberString := val.(string)
			memberID, _ := strconv.ParseUint(memberString, 10, 64)
			if s.member.ID() == memberID {
				checkAgain = true
			}
		})
		if checkAgain {
			continue
		}
		if leader != nil {
			err := s.reloadConfigFromKV()
			if err != nil {
				log.Error("reload config failed", errs.ZapError(err))
				continue
			}
			// Check the cluster dc-location after the PD leader is elected
			go s.tsoAllocatorManager.ClusterDCLocationChecker()
			syncer := s.cluster.GetRegionSyncer()
			if s.persistOptions.IsUseRegionStorage() {
				syncer.StartSyncWithLeader(leader.GetClientUrls()[0])
			}
			log.Info("start to watch pd leader", zap.Stringer("pd-leader", leader))
			// WatchLeader will keep looping and never return unless the PD leader has changed.
			s.member.WatchLeader(s.serverLoopCtx, leader, rev)
			syncer.StopSyncWithLeader()
			log.Info("pd leader has changed, try to re-campaign a pd leader")
		}

		// To make sure the etcd leader and PD leader are on the same server.
		etcdLeader := s.member.GetEtcdLeader()
		if etcdLeader != s.member.ID() {
			if s.member.GetLeader() == nil {
				lastUpdated := s.member.GetLastLeaderUpdatedTime()
				// use random timeout to avoid leader campaigning storm.
				randomTimeout := time.Duration(rand.Intn(lostPDLeaderMaxTimeoutSecs))*time.Second + lostPDLeaderMaxTimeoutSecs*time.Second + lostPDLeaderReElectionFactor*s.cfg.ElectionInterval.Duration
				// add failpoint to test the campaign leader logic.
				failpoint.Inject("timeoutWaitPDLeader", func() {
					log.Info("timeoutWaitPDLeader is injected, skip wait other etcd leader be etcd leader")
					randomTimeout = time.Duration(rand.Intn(10))*time.Millisecond + 100*time.Millisecond
				})
				if lastUpdated.Add(randomTimeout).Before(time.Now()) && !lastUpdated.IsZero() && etcdLeader != 0 {
					log.Info("the pd leader is lost for a long time, try to re-campaign a pd leader with resign etcd leader",
						zap.Duration("timeout", randomTimeout),
						zap.Time("last-updated", lastUpdated),
						zap.String("current-leader-member-id", types.ID(etcdLeader).String()),
						zap.String("transferee-member-id", types.ID(s.member.ID()).String()),
					)
					s.member.MoveEtcdLeader(s.ctx, etcdLeader, s.member.ID())
				}
			}
			log.Info("skip campaigning of pd leader and check later",
				zap.String("server-name", s.Name()),
				zap.Uint64("etcd-leader-id", etcdLeader),
				zap.Uint64("member-id", s.member.ID()))
			time.Sleep(200 * time.Millisecond)
			continue
		}
		s.campaignLeader()
	}
}

func (s *Server) campaignLeader() {
	log.Info("start to campaign pd leader", zap.String("campaign-pd-leader-name", s.Name()))
	if err := s.member.CampaignLeader(s.cfg.LeaderLease); err != nil {
		if err.Error() == errs.ErrEtcdTxnConflict.Error() {
			log.Info("campaign pd leader meets error due to txn conflict, another PD server may campaign successfully",
				zap.String("campaign-pd-leader-name", s.Name()))
		} else {
			log.Error("campaign pd leader meets error due to etcd error",
				zap.String("campaign-pd-leader-name", s.Name()),
				errs.ZapError(err))
		}
		return
	}

	// Start keepalive the leadership and enable TSO service.
	// TSO service is strictly enabled/disabled by PD leader lease for 2 reasons:
	//   1. lease based approach is not affected by thread pause, slow runtime schedule, etc.
	//   2. load region could be slow. Based on lease we can recover TSO service faster.
	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	var resetLeaderOnce sync.Once
	defer resetLeaderOnce.Do(func() {
		cancel()
		s.member.ResetLeader()
	})

	// maintain the PD leadership, after this, TSO can be service.
	s.member.KeepLeader(ctx)
	log.Info("campaign pd leader ok", zap.String("campaign-pd-leader-name", s.Name()))

	allocator, err := s.tsoAllocatorManager.GetAllocator(tso.GlobalDCLocation)
	if err != nil {
		log.Error("failed to get the global TSO allocator", errs.ZapError(err))
		return
	}
	log.Info("initializing the global TSO allocator")
	if err := allocator.Initialize(0); err != nil {
		log.Error("failed to initialize the global TSO allocator", errs.ZapError(err))
		return
	}
	defer func() {
		s.tsoAllocatorManager.ResetAllocatorGroup(tso.GlobalDCLocation)
		failpoint.Inject("updateAfterResetTSO", func() {
			if err = allocator.UpdateTSO(); err != nil {
				panic(err)
			}
		})
	}()

	if err := s.reloadConfigFromKV(); err != nil {
		log.Error("failed to reload configuration", errs.ZapError(err))
		return
	}

	if err := s.persistOptions.LoadTTLFromEtcd(s.ctx, s.client); err != nil {
		log.Error("failed to load persistOptions from etcd", errs.ZapError(err))
		return
	}

	if err := s.encryptionKeyManager.SetLeadership(s.member.GetLeadership()); err != nil {
		log.Error("failed to initialize encryption", errs.ZapError(err))
		return
	}

	// Try to create raft cluster.
	if err := s.createRaftCluster(); err != nil {
		log.Error("failed to create raft cluster", errs.ZapError(err))
		return
	}
	defer s.stopRaftCluster()
	if err := s.idAllocator.Rebase(); err != nil {
		log.Error("failed to sync id from etcd", errs.ZapError(err))
		return
	}
	// EnableLeader to accept the remaining service, such as GetStore, GetRegion.
	s.member.EnableLeader()
	// Check the cluster dc-location after the PD leader is elected.
	go s.tsoAllocatorManager.ClusterDCLocationChecker()
	defer resetLeaderOnce.Do(func() {
		// as soon as cancel the leadership keepalive, then other member have chance
		// to be new leader.
		cancel()
		s.member.ResetLeader()
	})

	CheckPDVersion(s.persistOptions)
	log.Info("PD cluster leader is ready to serve", zap.String("pd-leader-name", s.Name()))

	leaderTicker := time.NewTicker(leaderTickInterval)
	defer leaderTicker.Stop()

	for {
		select {
		case <-leaderTicker.C:
			if !s.member.IsLeader() {
				log.Info("no longer a leader because lease has expired, pd leader will step down")
				return
			}
			// add failpoint to test exit leader, failpoint judge the member is the give value, then break
			failpoint.Inject("exitCampaignLeader", func(val failpoint.Value) {
				memberString := val.(string)
				memberID, _ := strconv.ParseUint(memberString, 10, 64)
				if s.member.ID() == memberID {
					log.Info("exit PD leader")
					failpoint.Return()
				}
			})

			etcdLeader := s.member.GetEtcdLeader()
			if etcdLeader != s.member.ID() {
				log.Info("etcd leader changed, resigns pd leadership", zap.String("old-pd-leader-name", s.Name()))
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

func (s *Server) reloadConfigFromKV() error {
	err := s.persistOptions.Reload(s.storage)
	if err != nil {
		return err
	}
	err = s.serviceMiddlewarePersistOptions.Reload(s.storage)
	if err != nil {
		return err
	}
	s.loadRateLimitConfig()
	useRegionStorage := s.persistOptions.IsUseRegionStorage()
	regionStorage := storage.TrySwitchRegionStorage(s.storage, useRegionStorage)
	if regionStorage != nil {
		if useRegionStorage {
			log.Info("server enable region storage")
		} else {
			log.Info("server disable region storage")
		}
	}
	return nil
}

func (s *Server) loadRateLimitConfig() {
	cfg := s.serviceMiddlewarePersistOptions.GetRateLimitConfig().LimiterConfig
	for key := range cfg {
		value := cfg[key]
		s.serviceRateLimiter.Update(key, ratelimit.UpdateDimensionConfig(&value))
	}
}

// ReplicateFileToMember is used to synchronize state to a member.
// Each member will write `data` to a local file named `name`.
// For security reason, data should be in JSON format.
func (s *Server) ReplicateFileToMember(ctx context.Context, member *pdpb.Member, name string, data []byte) error {
	clientUrls := member.GetClientUrls()
	if len(clientUrls) == 0 {
		log.Warn("failed to replicate file", zap.String("name", name), zap.String("member", member.GetName()))
		return errs.ErrClientURLEmpty.FastGenByArgs()
	}
	url := clientUrls[0] + filepath.Join("/pd/api/v1/admin/persist-file", name)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(data))
	req.Header.Set("PD-Allow-follower-handle", "true")
	res, err := s.httpClient.Do(req)
	if err != nil {
		log.Warn("failed to replicate file", zap.String("name", name), zap.String("member", member.GetName()), errs.ZapError(err))
		return errs.ErrSendRequest.Wrap(err).GenWithStackByCause()
	}
	// Since we don't read the body, we can close it immediately.
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		log.Warn("failed to replicate file", zap.String("name", name), zap.String("member", member.GetName()), zap.Int("status-code", res.StatusCode))
		return errs.ErrSendRequest.FastGenByArgs()
	}
	return nil
}

// PersistFile saves a file in DataDir.
func (s *Server) PersistFile(name string, data []byte) error {
	if name != replication.DrStatusFile {
		return errors.New("Invalid file name")
	}
	log.Info("persist file", zap.String("name", name), zap.Binary("data", data))
	path := filepath.Join(s.GetConfig().DataDir, name)
	if !isPathInDirectory(path, s.GetConfig().DataDir) {
		return errors.New("Invalid file path")
	}
	return os.WriteFile(path, data, 0644) // #nosec
}

// SaveTTLConfig save ttl config
func (s *Server) SaveTTLConfig(data map[string]interface{}, ttl time.Duration) error {
	for k := range data {
		if !config.IsSupportedTTLConfig(k) {
			return fmt.Errorf("unsupported ttl config %s", k)
		}
	}
	for k, v := range data {
		if err := s.persistOptions.SetTTLData(s.ctx, s.client, k, fmt.Sprint(v), ttl); err != nil {
			return err
		}
	}
	return nil
}

// IsTTLConfigExist returns true if the ttl config is existed for a given config.
func (s *Server) IsTTLConfigExist(key string) bool {
	if config.IsSupportedTTLConfig(key) {
		if _, ok := s.persistOptions.GetTTLData(key); ok {
			return true
		}
	}
	return false
}

// MarkSnapshotRecovering mark pd that we're recovering
// tikv will get this state during BR EBS restore.
// we write this info into etcd for simplicity, the key only stays inside etcd temporary
// during BR EBS restore in which period the cluster is not able to serve request.
// and is deleted after BR EBS restore is done.
func (s *Server) MarkSnapshotRecovering() error {
	log.Info("mark snapshot recovering")
	markPath := endpoint.AppendToRootPath(s.rootPath, recoveringMarkPath)
	// the value doesn't matter, set to a static string
	_, err := kv.NewSlowLogTxn(s.client).
		If(clientv3.Compare(clientv3.CreateRevision(markPath), "=", 0)).
		Then(clientv3.OpPut(markPath, "on")).
		Commit()
	// if other client already marked, return success too
	return err
}

// IsSnapshotRecovering check whether recovering-mark marked
func (s *Server) IsSnapshotRecovering(ctx context.Context) (bool, error) {
	markPath := endpoint.AppendToRootPath(s.rootPath, recoveringMarkPath)
	resp, err := s.client.Get(ctx, markPath)
	if err != nil {
		return false, err
	}
	return len(resp.Kvs) > 0, nil
}

// UnmarkSnapshotRecovering unmark recovering mark
func (s *Server) UnmarkSnapshotRecovering(ctx context.Context) error {
	log.Info("unmark snapshot recovering")
	markPath := endpoint.AppendToRootPath(s.rootPath, recoveringMarkPath)
	_, err := s.client.Delete(ctx, markPath)
	// if other client already unmarked, return success too
	return err
}

// RecoverAllocID recover alloc id. set current base id to input id
func (s *Server) RecoverAllocID(ctx context.Context, id uint64) error {
	return s.idAllocator.SetBase(id)
}

// GetGlobalTS returns global tso.
func (s *Server) GetGlobalTS() (uint64, error) {
	ts, err := s.tsoAllocatorManager.GetGlobalTSO()
	if err != nil {
		return 0, err
	}
	return tsoutil.GenerateTS(ts), nil
}

// GetExternalTS returns external timestamp.
func (s *Server) GetExternalTS() uint64 {
	rc := s.GetRaftCluster()
	if rc == nil {
		return 0
	}
	return rc.GetExternalTS()
}

// SetExternalTS returns external timestamp.
func (s *Server) SetExternalTS(externalTS uint64) error {
	globalTS, err := s.GetGlobalTS()
	if err != nil {
		return err
	}
	if tsoutil.CompareTimestampUint64(externalTS, globalTS) == 1 {
		desc := "the external timestamp should not be larger than global ts"
		log.Error(desc, zap.Uint64("request timestamp", externalTS), zap.Uint64("global ts", globalTS))
		return errors.New(desc)
	}
	c := s.GetRaftCluster()
	if c == nil {
		return errs.ErrNotBootstrapped.FastGenByArgs()
	}
	currentExternalTS := c.GetExternalTS()
	if tsoutil.CompareTimestampUint64(externalTS, currentExternalTS) != 1 {
		desc := "the external timestamp should be larger than now"
		log.Error(desc, zap.Uint64("request timestamp", externalTS), zap.Uint64("current external timestamp", currentExternalTS))
		return errors.New(desc)
	}

	return c.SetExternalTS(externalTS)
}

// SetClient sets the etcd client.
// Notes: it is only used for test.
func (s *Server) SetClient(client *clientv3.Client) {
	s.client = client
}
