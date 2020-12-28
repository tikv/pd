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
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	log "github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/etcdutil"
	"github.com/pingcap/pd/pkg/logutil"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/pkg/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	etcdTimeout           = time.Second * 3
	etcdStartTimeout      = time.Minute * 5
	serverMetricsInterval = time.Minute
	// pdRootPath for all pd servers.
	pdRootPath      = "/pd"
	pdAPIPrefix     = "/pd/"
	pdClusterIDPath = "/pd/cluster_id"
)

// EnableZap enable the zap logger in embed etcd.
var EnableZap = false

// Server is the pd server.
type Server struct {
	// Server state.
	isServing int64
	leader    atomic.Value

	// Configs and initial fields.
	cfg         *Config
	etcdCfg     *embed.Config
	scheduleOpt *scheduleOption
	handler     *Handler

	serverLoopCtx    context.Context
	serverLoopCancel func()
	serverLoopWg     sync.WaitGroup

	// Etcd and cluster informations.
	etcd      *embed.Etcd
	client    *clientv3.Client
	id        uint64 // etcd server id.
	clusterID uint64 // pd cluster id.
	rootPath  string
	member    *pdpb.Member // current PD's info.
	// memberValue is the serialized string of `member`. It will be save in
	// etcd leader key when the PD node is successfully elected as the leader
	// of the cluster. Every write will use it to check leadership.
	memberValue string

	// Server services.
	// for id allocator, we can use one allocator for
	// store, region and peer, because we just need
	// a unique ID.
	idAlloc *idAllocator
	// for kv operation.
	kv *core.KV
	// for namespace.
	classifier namespace.Classifier
	// for raft cluster
	cluster *RaftCluster
	// For tso, set after pd becomes leader.
	ts            atomic.Value
	lastSavedTime time.Time
	// For async region heartbeat.
	hbStreams *heartbeatStreams
	// Zap logger
	lg       *zap.Logger
	logProps *log.ZapProperties
}

// CreateServer creates the UNINITIALIZED pd server with given configuration.
func CreateServer(cfg *Config, apiRegister func(*Server) http.Handler) (*Server, error) {
	log.Info("PD Config", zap.Reflect("config", cfg))
	rand.Seed(time.Now().UnixNano())

	s := &Server{
		cfg:         cfg,
		scheduleOpt: newScheduleOption(cfg),
	}
	s.handler = newHandler(s)

	// Adjust etcd config.
	etcdCfg, err := s.cfg.genEmbedEtcdConfig()
	if err != nil {
		return nil, err
	}
	if apiRegister != nil {
		etcdCfg.UserHandlers = map[string]http.Handler{
			pdAPIPrefix: apiRegister(s),
		}
	}
	etcdCfg.ServiceRegister = func(gs *grpc.Server) { pdpb.RegisterPDServer(gs, s) }
	s.etcdCfg = etcdCfg
	if EnableZap {
		// The etcd master version has removed embed.Config.SetupLogging.
		// Now logger is set up automatically based on embed.Config.Logger,
		// Use zap logger in the test, otherwise will panic.
		// Reference: https://github.com/etcd-io/etcd/blob/master/embed/config_logging.go#L45
		s.etcdCfg.Logger = "zap"
		s.etcdCfg.LogOutputs = []string{"stdout"}
	}
	s.lg = cfg.logger
	s.logProps = cfg.logProps
	return s, nil
}

func (s *Server) startEtcd(ctx context.Context) error {
	log.Info("start embed etcd")
	ctx, cancel := context.WithTimeout(ctx, etcdStartTimeout)
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
	case <-ctx.Done():
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

	s.etcd = etcd
	s.client = client
	s.id = etcdServerID
	return nil
}

func (s *Server) startServer() error {
	var err error
	if err = s.initClusterID(); err != nil {
		return err
	}
	log.Info("init cluster id", zap.Uint64("cluster-id", s.clusterID))
	// It may lose accuracy if use float64 to store uint64. So we store the
	// cluster id in label.
	metadataGauge.WithLabelValues(fmt.Sprintf("cluster%d", s.clusterID)).Set(0)

	s.rootPath = path.Join(pdRootPath, strconv.FormatUint(s.clusterID, 10))
	s.member, s.memberValue = s.memberInfo()

	s.idAlloc = &idAllocator{s: s}
	kvBase := newEtcdKVBase(s)
	s.kv = core.NewKV(kvBase)
	s.cluster = newRaftCluster(s, s.clusterID)
	s.hbStreams = newHeartbeatStreams(s.clusterID)
	if s.classifier, err = namespace.CreateClassifier(s.cfg.NamespaceClassifier, s.kv, s.idAlloc); err != nil {
		return err
	}

	// Server has started.
	atomic.StoreInt64(&s.isServing, 1)
	return nil
}

func (s *Server) initClusterID() error {
	// Get any cluster key to parse the cluster ID.
	resp, err := kvGet(s.client, pdClusterIDPath)
	if err != nil {
		return err
	}

	// If no key exist, generate a random cluster ID.
	if len(resp.Kvs) == 0 {
		s.clusterID, err = initOrGetClusterID(s.client, pdClusterIDPath)
		return err
	}
	s.clusterID, err = bytesToUint64(resp.Kvs[0].Value)
	return err
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

	if s.etcd != nil {
		s.etcd.Close()
	}

	if s.hbStreams != nil {
		s.hbStreams.Close()
	}

	log.Info("close server")
}

// isClosed checks whether server is closed or not.
func (s *Server) isClosed() bool {
	return atomic.LoadInt64(&s.isServing) == 0
}

var timeMonitorOnce sync.Once

// Run runs the pd server.
func (s *Server) Run(ctx context.Context) error {
	timeMonitorOnce.Do(func() {
		go StartMonitor(time.Now, func() {
			log.Error("system time jumps backward")
			timeJumpBackCounter.Inc()
		})
	})

	if err := s.startEtcd(ctx); err != nil {
		return err
	}

	if err := s.startServer(); err != nil {
		return err
	}

	s.startServerLoop()

	return nil
}

func (s *Server) startServerLoop() {
	s.serverLoopCtx, s.serverLoopCancel = context.WithCancel(context.Background())
	s.serverLoopWg.Add(3)
	go s.leaderLoop()
	go s.etcdLeaderLoop()
	go s.serverMetricsLoop()
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
	etcdStateGauge.WithLabelValues("term").Set(float64(s.etcd.Server.Term()))
	etcdStateGauge.WithLabelValues("appliedIndex").Set(float64(s.etcd.Server.AppliedIndex()))
	etcdStateGauge.WithLabelValues("committedIndex").Set(float64(s.etcd.Server.CommittedIndex()))
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
		MaxPeerCount: uint32(s.scheduleOpt.rep.GetMaxReplicas()),
	}

	// Set cluster meta
	clusterValue, err := clusterMeta.Marshal()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	clusterRootPath := s.getClusterRootPath()

	var ops []clientv3.Op
	ops = append(ops, clientv3.OpPut(clusterRootPath, string(clusterValue)))

	// Set bootstrap time
	bootstrapKey := makeBootstrapTimeKey(clusterRootPath)
	nano := time.Now().UnixNano()

	timeData := uint64ToBytes(uint64(nano))
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
	resp, err := s.txn().If(bootstrapCmp).Then(ops...).Commit()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !resp.Succeeded {
		log.Warn("cluster already bootstrapped", zap.Uint64("cluster-id", clusterID))
		return nil, errors.Errorf("cluster %d already bootstrapped", clusterID)
	}

	log.Info("bootstrap cluster ok", zap.Uint64("cluster-id", clusterID))

	if err := s.cluster.start(); err != nil {
		return nil, err
	}

	return &pdpb.BootstrapResponse{}, nil
}

func (s *Server) createRaftCluster() error {
	if s.cluster.isRunning() {
		return nil
	}

	return s.cluster.start()
}

func (s *Server) stopRaftCluster() {
	s.cluster.stop()
}

// GetAddr returns the server urls for clients.
func (s *Server) GetAddr() string {
	return s.cfg.AdvertiseClientUrls
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

// ID returns the unique etcd ID for this server in etcd cluster.
func (s *Server) ID() uint64 {
	return s.id
}

// Name returns the unique etcd Name for this server in etcd cluster.
func (s *Server) Name() string {
	return s.cfg.Name
}

// ClusterID returns the cluster ID of this server.
func (s *Server) ClusterID() uint64 {
	return s.clusterID
}

// txn returns an etcd client transaction wrapper.
// The wrapper will set a request timeout to the context and log slow transactions.
func (s *Server) txn() clientv3.Txn {
	return newSlowLogTxn(s.client)
}

// leaderTxn returns txn() with a leader comparison to guarantee that
// the transaction can be executed only if the server is leader.
func (s *Server) leaderTxn(cs ...clientv3.Cmp) clientv3.Txn {
	return s.txn().If(append(cs, s.leaderCmp())...)
}

// GetConfig gets the config information.
func (s *Server) GetConfig() *Config {
	cfg := s.cfg.clone()
	cfg.Schedule = *s.scheduleOpt.load()
	cfg.Replication = *s.scheduleOpt.rep.load()
	namespaces := make(map[string]NamespaceConfig)
	for name, opt := range s.scheduleOpt.ns {
		namespaces[name] = *opt.load()
	}
	cfg.Namespace = namespaces
	cfg.LabelProperty = s.scheduleOpt.loadLabelPropertyConfig().clone()
	cfg.ClusterVersion = s.scheduleOpt.loadClusterVersion()
	return cfg
}

// GetScheduleConfig gets the balance config information.
func (s *Server) GetScheduleConfig() *ScheduleConfig {
	cfg := &ScheduleConfig{}
	*cfg = *s.scheduleOpt.load()
	return cfg
}

// SetScheduleConfig sets the balance config information.
func (s *Server) SetScheduleConfig(cfg ScheduleConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	old := s.scheduleOpt.load()
	s.scheduleOpt.store(&cfg)
	if err := s.scheduleOpt.persist(s.kv); err != nil {
		return err
	}
	log.Info("schedule config is updated", zap.Reflect("new", cfg), zap.Reflect("old", old))
	return nil
}

// GetReplicationConfig get the replication config.
func (s *Server) GetReplicationConfig() *ReplicationConfig {
	cfg := &ReplicationConfig{}
	*cfg = *s.scheduleOpt.rep.load()
	return cfg
}

// SetReplicationConfig sets the replication config.
func (s *Server) SetReplicationConfig(cfg ReplicationConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	old := s.scheduleOpt.rep.load()
	s.scheduleOpt.rep.store(&cfg)
	if err := s.scheduleOpt.persist(s.kv); err != nil {
		return err
	}
	log.Info("replication config is updated", zap.Reflect("new", cfg), zap.Reflect("old", old))
	return nil
}

// GetNamespaceConfig get the namespace config.
func (s *Server) GetNamespaceConfig(name string) *NamespaceConfig {
	if _, ok := s.scheduleOpt.ns[name]; !ok {
		return &NamespaceConfig{}
	}

	cfg := &NamespaceConfig{
		LeaderScheduleLimit:    s.scheduleOpt.GetLeaderScheduleLimit(name),
		RegionScheduleLimit:    s.scheduleOpt.GetRegionScheduleLimit(name),
		ReplicaScheduleLimit:   s.scheduleOpt.GetReplicaScheduleLimit(name),
		HotRegionScheduleLimit: s.scheduleOpt.GetHotRegionScheduleLimit(name),
		MergeScheduleLimit:     s.scheduleOpt.GetMergeScheduleLimit(name),
		MaxReplicas:            uint64(s.scheduleOpt.GetMaxReplicas(name)),
	}

	return cfg
}

// GetNamespaceConfigWithAdjust get the namespace config that replace zero value with global config value.
func (s *Server) GetNamespaceConfigWithAdjust(name string) *NamespaceConfig {
	cfg := s.GetNamespaceConfig(name)
	cfg.adjust(s.scheduleOpt)
	return cfg
}

// SetNamespaceConfig sets the namespace config.
func (s *Server) SetNamespaceConfig(name string, cfg NamespaceConfig) {
	if n, ok := s.scheduleOpt.ns[name]; ok {
		old := s.scheduleOpt.ns[name].load()
		n.store(&cfg)
		s.scheduleOpt.persist(s.kv)
		log.Info("namespace config is updated", zap.String("name", name), zap.Reflect("new", cfg), zap.Reflect("old", old))
	} else {
		s.scheduleOpt.ns[name] = newNamespaceOption(&cfg)
		s.scheduleOpt.persist(s.kv)
		log.Info("namespace config is added", zap.String("name", name), zap.Reflect("new", cfg))
	}
}

// DeleteNamespaceConfig deletes the namespace config.
func (s *Server) DeleteNamespaceConfig(name string) {
	if n, ok := s.scheduleOpt.ns[name]; ok {
		cfg := n.load()
		delete(s.scheduleOpt.ns, name)
		s.scheduleOpt.persist(s.kv)
		log.Info("namespace config is deleted", zap.String("name", name), zap.Reflect("config", *cfg))
	}
}

// SetLabelProperty inserts a label property config.
func (s *Server) SetLabelProperty(typ, labelKey, labelValue string) error {
	s.scheduleOpt.SetLabelProperty(typ, labelKey, labelValue)
	err := s.scheduleOpt.persist(s.kv)
	if err != nil {
		return err
	}
	log.Info("label property config is updated", zap.Reflect("config", s.scheduleOpt.loadLabelPropertyConfig()))
	return nil
}

// DeleteLabelProperty deletes a label property config.
func (s *Server) DeleteLabelProperty(typ, labelKey, labelValue string) error {
	s.scheduleOpt.DeleteLabelProperty(typ, labelKey, labelValue)
	err := s.scheduleOpt.persist(s.kv)
	if err != nil {
		return err
	}
	log.Info("label property config is deleted", zap.Reflect("config", s.scheduleOpt.loadLabelPropertyConfig()))
	return nil
}

// GetLabelProperty returns the whole label property config.
func (s *Server) GetLabelProperty() LabelPropertyConfig {
	return s.scheduleOpt.loadLabelPropertyConfig().clone()
}

// SetClusterVersion sets the version of cluster.
func (s *Server) SetClusterVersion(v string) error {
	version, err := ParseVersion(v)
	if err != nil {
		return err
	}
	s.scheduleOpt.SetClusterVersion(*version)
	err = s.scheduleOpt.persist(s.kv)
	if err != nil {
		return err
	}
	log.Info("cluster version is updated", zap.String("new-version", v))
	return nil
}

// GetClusterVersion returns the version of cluster.
func (s *Server) GetClusterVersion() semver.Version {
	return s.scheduleOpt.loadClusterVersion()
}

// GetSecurityConfig get the security config.
func (s *Server) GetSecurityConfig() *SecurityConfig {
	return &s.cfg.Security
}

// GetAllocator returns the id allocator of this server.
func (s *Server) GetAllocator() core.IDAllocator {
	return s.idAlloc
}

// IsNamespaceExist returns whether the namespace exists.
func (s *Server) IsNamespaceExist(name string) bool {
	return s.classifier.IsNamespaceExist(name)
}

func (s *Server) getClusterRootPath() string {
	return path.Join(s.rootPath, "raft")
}

// GetRaftCluster gets raft cluster.
// If cluster has not been bootstrapped, return nil.
func (s *Server) GetRaftCluster() *RaftCluster {
	if s.isClosed() || !s.cluster.isRunning() {
		return nil
	}
	return s.cluster
}

// GetCluster gets cluster.
func (s *Server) GetCluster() *metapb.Cluster {
	return &metapb.Cluster{
		Id:           s.clusterID,
		MaxPeerCount: uint32(s.scheduleOpt.rep.GetMaxReplicas()),
	}
}

// GetClusterStatus gets cluster status.
func (s *Server) GetClusterStatus() (*ClusterStatus, error) {
	s.cluster.Lock()
	defer s.cluster.Unlock()
	return s.cluster.loadClusterStatus()
}

func (s *Server) getAllocIDPath() string {
	return path.Join(s.rootPath, "alloc_id")
}

func (s *Server) getMemberLeaderPriorityPath(id uint64) string {
	return path.Join(s.rootPath, fmt.Sprintf("member/%d/leader_priority", id))
}

// SetMemberLeaderPriority saves a member's priority to be elected as the etcd leader.
func (s *Server) SetMemberLeaderPriority(id uint64, priority int) error {
	key := s.getMemberLeaderPriorityPath(id)
	res, err := s.leaderTxn().Then(clientv3.OpPut(key, strconv.Itoa(priority))).Commit()
	if err != nil {
		return errors.WithStack(err)
	}
	if !res.Succeeded {
		return errors.New("save leader priority failed, maybe not leader")
	}
	return nil
}

// DeleteMemberLeaderPriority removes a member's priority config.
func (s *Server) DeleteMemberLeaderPriority(id uint64) error {
	key := s.getMemberLeaderPriorityPath(id)
	res, err := s.leaderTxn().Then(clientv3.OpDelete(key)).Commit()
	if err != nil {
		return errors.WithStack(err)
	}
	if !res.Succeeded {
		return errors.New("delete leader priority failed, maybe not leader")
	}
	return nil
}

// GetMemberLeaderPriority loads a member's priority to be elected as the etcd leader.
func (s *Server) GetMemberLeaderPriority(id uint64) (int, error) {
	key := s.getMemberLeaderPriorityPath(id)
	res, err := kvGet(s.client, key)
	if err != nil {
		return 0, err
	}
	if len(res.Kvs) == 0 {
		return 0, nil
	}
	priority, err := strconv.ParseInt(string(res.Kvs[0].Value), 10, 32)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return int(priority), nil
}

// SetLogLevel sets log level.
func (s *Server) SetLogLevel(level string) {
	s.cfg.Log.Level = level
	log.SetLevel(logutil.StringToZapLogLevel(level))
	log.Warn("log level changed", zap.String("level", log.GetLevel().String()))
}

var healthURL = "/pd/ping"

// CheckHealth checks if members are healthy
func (s *Server) CheckHealth(members []*pdpb.Member) map[uint64]*pdpb.Member {
	unhealthMembers := make(map[uint64]*pdpb.Member)
	for _, member := range members {
		for _, cURL := range member.ClientUrls {
			resp, err := dialClient.Get(fmt.Sprintf("%s%s", cURL, healthURL))
			if resp != nil {
				resp.Body.Close()
			}
			if err != nil || resp.StatusCode != http.StatusOK {
				unhealthMembers[member.GetMemberId()] = member
				break
			}
		}
	}
	return unhealthMembers
}
