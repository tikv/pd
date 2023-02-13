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
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/systimemon"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server/cluster"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	serverMetricsInterval = time.Minute
	// tsoRootPath for all tso servers.
	tsoRootPath      = "/tso"
	tsoAPIPrefix     = "/tso/"
	tsoClusterIDPath = "/tso/cluster_id"
)

// If server doesn't implement all methods of bs.Server, this line will result in a clear
// error message like "*Server does not implement bs.Server (missing Method method)"
var _ bs.Server = (*Server)(nil)
var _ tso.GrpcServer = (*Server)(nil)

// Server is the TSO server, and it implements bs.Server and tso.GrpcServer.
type Server struct {
	diagnosticspb.DiagnosticsServer

	// Server start timestamp
	startTimestamp int64

	ctx       context.Context
	name      string
	clusterID uint64
	rootPath  string
	// etcd client
	client *clientv3.Client
	// http client
	httpClient          *http.Client
	member              *member.Member
	tsoAllocatorManager *tso.AllocatorManager
	// Store as map[string]*grpc.ClientConn
	clientConns sync.Map
	// Store as map[string]chan *tsoRequest
	tsoDispatcher sync.Map
	// Callback functions for different stages
	// startCallbacks will be called after the server is started.
	startCallbacks []func()
	// leaderCallbacks will be called after the server becomes leader.
	leaderCallbacks []func(context.Context)
}

// NewServer creates a new TSO server.
func NewServer(ctx context.Context, client *clientv3.Client, httpClient *http.Client,
	dxs diagnosticspb.DiagnosticsServer) *Server {
	return &Server{
		DiagnosticsServer: dxs,
		startTimestamp:    time.Now().Unix(),
		ctx:               ctx,
		name:              "TSO",
		client:            client,
		httpClient:        httpClient,
		member:            &member.Member{},
	}
}

// TODO: Implement the following methods defined in bs.Server

// Name returns the unique etcd Name for this server in etcd cluster.
func (s *Server) Name() string {
	return s.name
}

// Context returns the context of server.
func (s *Server) Context() context.Context {
	return s.ctx
}

// Run runs the TSO server.
func (s *Server) Run() error {
	go systimemon.StartMonitor(s.ctx, time.Now, func() {
		log.Error("system time jumps backward", errs.ZapError(errs.ErrIncorrectSystemTime))
		timeJumpBackCounter.Inc()
	})

	// TODO: Start gPRC server

	if err := s.startServer(s.ctx); err != nil {
		return err
	}

	s.startServerLoop(s.ctx)

	return nil
}

// Close closes the server.
func (s *Server) Close() {
}

// GetClient returns builtin etcd client.
func (s *Server) GetClient() *clientv3.Client {
	return s.client
}

// GetHTTPClient returns builtin http client.
func (s *Server) GetHTTPClient() *http.Client {
	return s.httpClient
}

// AddStartCallback adds a callback in the startServer phase.
func (s *Server) AddStartCallback(callbacks ...func()) {
	s.startCallbacks = append(s.startCallbacks, callbacks...)
}

// GetMember returns the member.
func (s *Server) GetMember() *member.Member {
	return s.member
}

// AddLeaderCallback adds the callback function when the server becomes leader.
func (s *Server) AddLeaderCallback(callbacks ...func(context.Context)) {
	s.leaderCallbacks = append(s.leaderCallbacks, callbacks...)
}

// Implement the following methods defined in tso.GrpcServer

// ClusterID returns the cluster ID of this server.
func (s *Server) ClusterID() uint64 {
	return s.clusterID
}

// IsClosed checks if the server loop is closed
func (s *Server) IsClosed() bool {
	// TODO: implement it
	return true
}

// IsLocalRequest checks if the forwarded host is the current host
func (s *Server) IsLocalRequest(forwardedHost string) bool {
	if forwardedHost == "" {
		return true
	}
	memberAddrs := s.GetMember().Member().GetClientUrls()
	for _, addr := range memberAddrs {
		if addr == forwardedHost {
			return true
		}
	}
	return false
}

// GetTSOAllocatorManager returns the manager of TSO Allocator.
func (s *Server) GetTSOAllocatorManager() *tso.AllocatorManager {
	return s.tsoAllocatorManager
}

// GetTSODispatcher gets the TSO Dispatcher
func (s *Server) GetTSODispatcher() *sync.Map {
	return &s.tsoDispatcher
}

// CreateTsoForwardStream creats the forward stream in the type of pdpb.PD_TsoClient
// which is the same type as tsopb.TSO_TsoClient.
func (s *Server) CreateTsoForwardStream(client *grpc.ClientConn) (pdpb.PD_TsoClient, context.CancelFunc, error) {
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(s.ctx)
	go checkStream(ctx, cancel, done)
	forwardStream, err := tsopb.NewTSOClient(client).Tso(ctx)
	done <- struct{}{}
	return (pdpb.PD_TsoClient)(forwardStream), cancel, err
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

// ValidateInternalRequest checks if server is closed, which is used to validate
// the gRPC communication between TSO servers internally.
// TODO: Check if the sender is from the global TSO allocator
func (s *Server) ValidateInternalRequest(_ *pdpb.RequestHeader, _ bool) error {
	if s.IsClosed() {
		return ErrNotStarted
	}
	return nil
}

// ValidateRequest checks if the keyspace replica is the primary and clusterID is matched.
// TODO: Check if the keyspace replica is the primary
func (s *Server) ValidateRequest(header *pdpb.RequestHeader) error {
	if s.IsClosed() {
		return ErrNotLeader
	}
	if header.GetClusterId() != s.clusterID {
		return status.Errorf(codes.FailedPrecondition, "mismatch cluster id, need %d but got %d", s.clusterID, header.GetClusterId())
	}
	return nil
}

// Implement other methods

// GetGlobalTS returns global tso.
func (s *Server) GetGlobalTS() (uint64, error) {
	ts, err := s.tsoAllocatorManager.GetGlobalTSO()
	if err != nil {
		return 0, err
	}
	return tsoutil.GenerateTS(ts), nil
}

// GetExternalTS returns external timestamp from the cache or the persistent storage.
// TODO: Implement GetExternalTS
func (s *Server) GetExternalTS() uint64 {
	return 0
}

// SetExternalTS saves external timestamp to cache and the persistent storage.
// TODO: Implement SetExternalTS
func (s *Server) SetExternalTS(externalTS uint64) error {
	return nil
}

// TODO: If goroutine here timeout after a stream is created successfully, we need to handle it correctly.
func checkStream(streamCtx context.Context, cancel context.CancelFunc, done chan struct{}) {
	select {
	case <-done:
		return
	case <-time.After(3 * time.Second):
		cancel()
	case <-streamCtx.Done():
	}
	<-done
}

// GetTLSConfig get the security config.
// TODO: implement it
func (s *Server) GetTLSConfig() *grpcutil.TLSConfig {
	return nil
}

// GetMembers returns TSO server list.
func (s *Server) GetMembers() ([]*pdpb.Member, error) {
	if s.IsClosed() {
		return nil, errs.ErrServerNotStarted.FastGenByArgs()
	}
	members, err := cluster.GetMembers(s.GetClient())
	return members, err
}

func (s *Server) startServer(ctx context.Context) error {
	var err error
	if s.clusterID, err = etcdutil.InitClusterID(s.client, tsoClusterIDPath); err != nil {
		return err
	}
	log.Info("init cluster id", zap.Uint64("cluster-id", s.clusterID))
	// It may lose accuracy if use float64 to store uint64. So we store the
	// cluster id in label.
	metadataGauge.WithLabelValues(fmt.Sprintf("cluster%d", s.clusterID)).Set(0)
	// The independent TSO service still reuses PD version info since PD and TSO are just
	// different service modes provided by the same pd-server binary
	serverInfo.WithLabelValues(versioninfo.PDReleaseVersion, versioninfo.PDGitHash).Set(float64(time.Now().Unix()))

	s.rootPath = path.Join(tsoRootPath, strconv.FormatUint(s.clusterID, 10))

	/*
	s.member.MemberInfo(s.cfg.AdvertiseClientUrls, s.cfg.AdvertisePeerUrls, s.Name(), s.rootPath)
	s.member.SetMemberDeployPath(s.member.ID())
	s.member.SetMemberBinaryVersion(s.member.ID(), versioninfo.PDReleaseVersion)
	s.member.SetMemberGitHash(s.member.ID(), versioninfo.PDGitHash)

	s.tsoAllocatorManager = tso.NewAllocatorManager(
		s.member, s.rootPath, s.cfg.IsLocalTSOEnabled(), s.cfg.GetTSOSaveInterval(), s.cfg.GetTSOUpdatePhysicalInterval(), s.cfg.GetTLSConfig(),
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
	s.encryptionKeyManager, err = encryption.NewManager(s.client, &s.cfg.Security.Encryption)
	if err != nil {
		return err
	}
	defaultStorage := storage.NewStorageWithEtcdBackend(s.client, s.rootPath)
	s.storage = storage.NewCoreStorage(defaultStorage, regionStorage)
	s.gcSafePointManager = gc.NewSafePointManager(s.storage)
	s.basicCluster = core.NewBasicCluster()
	s.cluster = cluster.NewRaftCluster(ctx, s.clusterID, syncer.NewRegionSyncer(s), s.client, s.httpClient)
	keyspaceIDAllocator := id.NewAllocator(&id.AllocatorParams{
		Client:    s.client,
		RootPath:  s.rootPath,
		AllocPath: endpoint.KeyspaceIDAlloc(),
		Label:     keyspace.AllocLabel,
		Member:    s.member.MemberValue(),
		Step:      keyspace.AllocStep,
	})
	// Run callbacks
	log.Info("triggering the start callback functions")
	for _, cb := range s.startCallbacks {
		cb()
	}

	// Server has started.
	atomic.StoreInt64(&s.isServing, 1)
	*/
	return nil
}