// Copyright 2018 TiKV Project Authors.
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

package tests

import (
	"context"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-semver/semver"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/dashboard"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/id"
	"github.com/tikv/pd/pkg/keyspace"
	ks "github.com/tikv/pd/pkg/keyspace/constant"
	rm_redirector "github.com/tikv/pd/pkg/mcs/resourcemanager/redirector"
	scheduling "github.com/tikv/pd/pkg/mcs/scheduling/server"
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/swaggerserver"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/apiv2"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/join"
)

// TestServer states.
const (
	Initial int32 = iota
	Running
	Starting
	Stop
	Destroy
)

var (
	// WaitLeaderReturnDelay represents the time interval of WaitLeader sleep before returning.
	WaitLeaderReturnDelay = 20 * time.Millisecond
	// WaitLeaderCheckInterval represents the time interval of WaitLeader running check.
	WaitLeaderCheckInterval = 500 * time.Millisecond
	// WaitLeaderRetryTimes represents the maximum number of loops of WaitLeader.
	WaitLeaderRetryTimes = 100

	// WaitPreAllocKeyspacesInterval represents the time interval of WaitPreAllocKeyspaces running check.
	WaitPreAllocKeyspacesInterval = 500 * time.Millisecond
	// WaitPreAllocKeyspacesRetryTimes represents the maximum number of loops of WaitPreAllocKeyspaces.
	WaitPreAllocKeyspacesRetryTimes = 100

	// defaultMaxRetryTimes is the default maximum retry times for starting servers.
	defaultMaxRetryTimes = 5
)

// TestServer is only for test.
type TestServer struct {
	syncutil.RWMutex
	server     *server.Server
	grpcServer *server.GrpcServer
	state      int32
}

var zapLogOnce sync.Once

// NewTestServer creates a new TestServer.
func NewTestServer(ctx context.Context, cfg *config.Config, services []string, handlers ...server.HandlerBuilder) (*TestServer, error) {
	// use temp dir to ensure test isolation.
	if cfg.DataDir == "" || strings.HasPrefix(cfg.DataDir, "default.") {
		tempDir, err := os.MkdirTemp("", "pd_tests")
		if err != nil {
			return nil, errors.Wrap(err, "failed to create safeguard temp data dir for test server")
		}
		cfg.DataDir = tempDir
	}
	// disable the heartbeat async runner in test
	cfg.Schedule.EnableHeartbeatConcurrentRunner = false
	err := logutil.SetupLogger(&cfg.Log, &cfg.Logger, &cfg.LogProps, cfg.Security.RedactInfoLog)
	if err != nil {
		return nil, err
	}
	zapLogOnce.Do(func() {
		log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
	})
	err = join.PrepareJoinCluster(cfg)
	if err != nil {
		return nil, err
	}

	var serviceBuilders []server.HandlerBuilder
	if len(handlers) > 0 {
		serviceBuilders = append(serviceBuilders, handlers...)
	} else {
		serviceBuilders = []server.HandlerBuilder{api.NewHandler, apiv2.NewV2Handler}
		if swaggerserver.Enabled() {
			serviceBuilders = append(serviceBuilders, swaggerserver.NewHandler)
		}
		serviceBuilders = append(serviceBuilders, dashboard.GetServiceBuilders()...)
		if !cfg.Microservice.IsResourceManagerFallbackEnabled() {
			serviceBuilders = append(serviceBuilders, rm_redirector.NewHandler)
		}
	}

	svr, err := server.CreateServer(ctx, cfg, services, serviceBuilders...)
	if err != nil {
		return nil, err
	}
	return &TestServer{
		server:     svr,
		grpcServer: &server.GrpcServer{Server: svr},
		state:      Initial,
	}, nil
}

// Run starts to run a TestServer.
func (s *TestServer) Run() error {
	s.Lock()
	if s.state != Initial && s.state != Stop {
		s.Unlock()
		return errors.Errorf("server(state%d) cannot run", s.state)
	}
	s.state = Starting
	s.Unlock()

	if err := s.server.Run(); err != nil {
		s.Lock()
		if s.state == Starting {
			s.state = Stop
		}
		s.Unlock()
		return err
	}

	s.Lock()
	if s.state == Starting {
		s.state = Running
	}
	s.Unlock()
	return nil
}

// Stop is used to stop a TestServer.
func (s *TestServer) Stop() error {
	s.Lock()
	defer s.Unlock()
	if s.state != Running && s.state != Starting {
		return errors.Errorf("server(state%d) cannot stop", s.state)
	}
	s.server.Close()
	s.state = Stop
	return nil
}

// Destroy is used to destroy a TestServer.
func (s *TestServer) Destroy() error {
	s.Lock()
	defer s.Unlock()
	if s.state == Running || s.state == Starting {
		s.server.Close()
	}
	if err := os.RemoveAll(s.server.GetConfig().DataDir); err != nil {
		return err
	}
	s.state = Destroy
	return nil
}

// ResetPDLeader resigns the leader of the server.
func (s *TestServer) ResetPDLeader() {
	s.Lock()
	defer s.Unlock()
	s.server.GetMember().Resign()
}

// ResignLeader resigns the leader of the server.
func (s *TestServer) ResignLeader() error {
	s.Lock()
	defer s.Unlock()
	s.server.GetMember().Resign()
	return s.server.GetMember().ResignEtcdLeader(s.server.Context(), s.server.Name(), "")
}

// ResignLeaderWithRetry resigns the leader of the server with retry.
func (s *TestServer) ResignLeaderWithRetry() (err error) {
	if !s.IsLeader() {
		return
	}
	// The default timeout of moving an etcd leader is 5 seconds,
	// set the retry times to 3 will get a maximum of ~15 seconds of trying.
	const retryCount = 3
	for retry := range retryCount {
		err = s.ResignLeader()
		if err == nil {
			return
		}
		// Do not retry if the last attempt fails.
		if retry == retryCount-1 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	return
}

// State returns the current TestServer's state.
func (s *TestServer) State() int32 {
	s.RLock()
	defer s.RUnlock()
	return s.state
}

// GetConfig returns the current TestServer's configuration.
func (s *TestServer) GetConfig() *config.Config {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetConfig()
}

// GetPersistOptions returns the current TestServer's schedule option.
func (s *TestServer) GetPersistOptions() *config.PersistOptions {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetPersistOptions()
}

// GetAllocator returns the current TestServer's ID allocator.
func (s *TestServer) GetAllocator() id.Allocator {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetAllocator()
}

// GetAddr returns the address of TestCluster.
func (s *TestServer) GetAddr() string {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetAddr()
}

// GetServer returns the real server of TestServer.
func (s *TestServer) GetServer() *server.Server {
	s.RLock()
	defer s.RUnlock()
	return s.server
}

// GetClusterID returns the cluster ID.
func (*TestServer) GetClusterID() uint64 {
	return keypath.ClusterID()
}

// GetLeader returns current leader of PD cluster.
func (s *TestServer) GetLeader() *pdpb.Member {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetLeader()
}

// GetKeyspaceManager returns the current TestServer's Keyspace Manager.
func (s *TestServer) GetKeyspaceManager() *keyspace.Manager {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetKeyspaceManager()
}

// SetKeyspaceManager sets the current TestServer's Keyspace Manager.
func (s *TestServer) SetKeyspaceManager(km *keyspace.Manager) {
	s.RLock()
	defer s.RUnlock()
	s.server.SetKeyspaceManager(km)
}

// GetCluster returns PD cluster.
func (s *TestServer) GetCluster() *metapb.Cluster {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetCluster()
}

// GetClusterVersion returns PD cluster version.
func (s *TestServer) GetClusterVersion() semver.Version {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetClusterVersion()
}

// GetServerID returns the unique etcd ID for this server in etcd cluster.
func (s *TestServer) GetServerID() uint64 {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetMember().ID()
}

// IsLeader returns whether the server is leader or not.
func (s *TestServer) IsLeader() bool {
	s.RLock()
	defer s.RUnlock()
	return !s.server.IsClosed() && s.server.GetMember().IsServing()
}

// GetEtcdLeader returns the builtin etcd leader.
func (s *TestServer) GetEtcdLeader() (string, error) {
	s.RLock()
	defer s.RUnlock()
	req := &pdpb.GetMembersRequest{Header: &pdpb.RequestHeader{ClusterId: keypath.ClusterID()}}
	members, _ := s.grpcServer.GetMembers(context.TODO(), req)
	if members.Header.GetError() != nil {
		return "", errors.WithStack(errors.New(members.Header.GetError().String()))
	}
	return members.GetEtcdLeader().GetName(), nil
}

// GetEtcdLeaderID returns the builtin etcd leader ID.
func (s *TestServer) GetEtcdLeaderID() (uint64, error) {
	s.RLock()
	defer s.RUnlock()
	req := &pdpb.GetMembersRequest{Header: &pdpb.RequestHeader{ClusterId: keypath.ClusterID()}}
	members, err := s.grpcServer.GetMembers(context.TODO(), req)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if members.GetHeader().GetError() != nil {
		return 0, errors.WithStack(errors.New(members.GetHeader().GetError().String()))
	}
	return members.GetEtcdLeader().GetMemberId(), nil
}

// MoveEtcdLeader moves etcd leader from old to new.
func (s *TestServer) MoveEtcdLeader(old, new uint64) error {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetMember().MoveEtcdLeader(context.Background(), old, new)
}

// GetEtcdClient returns the builtin etcd client.
func (s *TestServer) GetEtcdClient() *clientv3.Client {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetClient()
}

// GetHTTPClient returns the builtin http client.
func (s *TestServer) GetHTTPClient() *http.Client {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetHTTPClient()
}

// GetStores returns the stores of the cluster.
func (s *TestServer) GetStores() []*metapb.Store {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetRaftCluster().GetMetaStores()
}

// GetStore returns the store with a given store ID.
func (s *TestServer) GetStore(storeID uint64) *core.StoreInfo {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetRaftCluster().GetStore(storeID)
}

// GetRaftCluster returns Raft cluster.
// If cluster has not been bootstrapped, return nil.
func (s *TestServer) GetRaftCluster() *cluster.RaftCluster {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetRaftCluster()
}

// GetRegions returns all regions' information in detail.
func (s *TestServer) GetRegions() []*core.RegionInfo {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetRaftCluster().GetRegions()
}

// GetRegionInfoByID returns regionInfo by regionID from cluster.
func (s *TestServer) GetRegionInfoByID(regionID uint64) *core.RegionInfo {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetRaftCluster().GetRegion(regionID)
}

// GetAdjacentRegions returns regions' information that are adjacent with the specific region ID.
func (s *TestServer) GetAdjacentRegions(region *core.RegionInfo) []*core.RegionInfo {
	s.RLock()
	defer s.RUnlock()
	left, right := s.server.GetRaftCluster().GetAdjacentRegions(region)
	return []*core.RegionInfo{left, right}
}

// GetRangeHoles returns all range holes, i.e the key ranges without any region info.
func (s *TestServer) GetRangeHoles() [][]string {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetRaftCluster().GetRangeHoles()
}

// GetStoreRegions returns all regions' information with a given storeID.
func (s *TestServer) GetStoreRegions(storeID uint64) []*core.RegionInfo {
	s.RLock()
	defer s.RUnlock()
	return s.server.GetRaftCluster().GetStoreRegions(storeID)
}

// BootstrapCluster is used to bootstrap the cluster.
func (s *TestServer) BootstrapCluster() error {
	bootstrapReq := &pdpb.BootstrapRequest{
		Header: &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
		Store:  &metapb.Store{Id: 1, Address: "mock://tikv-1:1", LastHeartbeat: time.Now().UnixNano()},
		Region: &metapb.Region{Id: 2, Peers: []*metapb.Peer{{Id: 3, StoreId: 1, Role: metapb.PeerRole_Voter}}},
	}
	resp, err := s.grpcServer.Bootstrap(context.Background(), bootstrapReq)
	if err != nil {
		return err
	}
	if resp.GetHeader().GetError() != nil {
		return errors.New(resp.GetHeader().GetError().String())
	}

	err = s.waitPreAllocKeyspaces()
	if err != nil {
		return err
	}

	return nil
}

// WaitLeader is used to get instant leader info in order to
// make a test know the PD leader has been elected as soon as possible.
// If it exceeds the maximum number of loops, it will return nil.
func (s *TestServer) WaitLeader() bool {
	for range WaitLeaderRetryTimes {
		if s.server.GetMember().IsServing() {
			return true
		}
		time.Sleep(WaitLeaderCheckInterval)
	}
	return false
}

func (s *TestServer) waitPreAllocKeyspaces() error {
	keyspaces := s.GetConfig().Keyspace.GetPreAlloc()
	if len(keyspaces) == 0 {
		return nil
	}

	manager := s.GetKeyspaceManager()
	idx := 0
Outer:
	for range WaitPreAllocKeyspacesRetryTimes {
		for idx < len(keyspaces) {
			_, err := manager.LoadKeyspace(keyspaces[idx])
			if err != nil {
				// If the error is ErrEtcdTxnConflict, it means there is a temporary failure.
				if errors.ErrorEqual(err, errs.ErrKeyspaceNotFound) || errors.ErrorEqual(err, errs.ErrEtcdTxnConflict) {
					time.Sleep(WaitPreAllocKeyspacesInterval)
					continue Outer
				}
				return errors.Trace(err)
			}

			idx += 1
		}
		return nil
	}
	return errors.New("wait pre-alloc keyspaces retry limit exceeded")
}

// GetPreAllocKeyspaceIDs returns the pre-allocated keyspace IDs.
func (s *TestServer) GetPreAllocKeyspaceIDs() ([]uint32, error) {
	keyspaces := s.GetConfig().Keyspace.GetPreAlloc()
	ids := make([]uint32, 0, len(keyspaces))
	manager := s.GetKeyspaceManager()
	for _, keyspace := range keyspaces {
		meta, err := manager.LoadKeyspace(keyspace)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ids = append(ids, meta.GetId())
	}
	return ids, nil
}

// GetServicePrimaryAddr returns the primary address of the service.
func (s *TestServer) GetServicePrimaryAddr(ctx context.Context, serviceName string) (string, bool) {
	return s.server.GetServicePrimaryAddr(ctx, serviceName)
}

// TestCluster is only for test.
type TestCluster struct {
	ctx     context.Context
	config  *clusterConfig
	servers map[string]*TestServer
	// tsPool is used to check the TSO uniqueness among the test cluster
	tsPool struct {
		syncutil.Mutex
		pool map[uint64]struct{}
	}
	schedulingCluster *TestSchedulingCluster
	tsoCluster        *TestTSOCluster
	// services and opts are stored for recreating servers on port conflicts
	services []string
	opts     []ConfigOption
}

// ConfigOption is used to define customize settings in test.
// You can use serverName to customize a config for a certain
// server. Usually, the server name will be like `pd1`, `pd2`
// and so on, which determined by the number of servers you set.
type ConfigOption func(conf *config.Config, serverName string)

// WithGCTuner set WithGCTuner for tests
func WithGCTuner(enabled bool) ConfigOption {
	return func(conf *config.Config, _ string) {
		conf.PDServerCfg.EnableGOGCTuner = enabled
	}
}

// NewTestCluster creates a new TestCluster.
func NewTestCluster(ctx context.Context, initialServerCount int, opts ...ConfigOption) (*TestCluster, error) {
	return createTestCluster(ctx, initialServerCount, nil, nil, opts...)
}

// NewTestClusterWithHandlers creates a new TestCluster with handlers.
func NewTestClusterWithHandlers(ctx context.Context, initialServerCount int, handlers []server.HandlerBuilder, opts ...ConfigOption) (*TestCluster, error) {
	return createTestCluster(ctx, initialServerCount, nil, handlers, opts...)
}

// NewTestClusterWithKeyspaceGroup creates a new TestCluster with PD.
func NewTestClusterWithKeyspaceGroup(ctx context.Context, initialServerCount int, opts ...ConfigOption) (*TestCluster, error) {
	return createTestCluster(ctx, initialServerCount, []string{constant.PDServiceName}, nil, opts...)
}

func createTestCluster(ctx context.Context, initialServerCount int, services []string, handlers []server.HandlerBuilder, opts ...ConfigOption) (*TestCluster, error) {
	schedulers.Register()
	config := newClusterConfig(initialServerCount)
	servers := make(map[string]*TestServer)
	for _, cfg := range config.InitialServers {
		allOpts := append([]ConfigOption{WithGCTuner(false)}, opts...)
		serverConf, err := cfg.Generate(allOpts...)
		if err != nil {
			return nil, err
		}
		s, err := NewTestServer(ctx, serverConf, services, handlers...)
		if err != nil {
			return nil, err
		}
		servers[cfg.Name] = s
	}
	return &TestCluster{
		ctx:      ctx,
		config:   config,
		servers:  servers,
		services: services,
		opts:     opts,
		tsPool: struct {
			syncutil.Mutex
			pool map[uint64]struct{}
		}{
			pool: make(map[uint64]struct{}),
		},
	}, nil
}

// RestartTestPDCluster restarts the PD test cluster.
func RestartTestPDCluster(ctx context.Context, cluster *TestCluster) (*TestCluster, error) {
	return restartTestCluster(ctx, cluster, true)
}

func restartTestCluster(
	ctx context.Context, cluster *TestCluster, isKeyspaceGroupEnabled bool,
) (newTestCluster *TestCluster, err error) {
	schedulers.Register()
	newTestCluster = &TestCluster{
		ctx:      ctx,
		config:   cluster.config,
		servers:  make(map[string]*TestServer, len(cluster.servers)),
		services: cluster.services,
		opts:     cluster.opts,
		tsPool: struct {
			syncutil.Mutex
			pool map[uint64]struct{}
		}{
			pool: make(map[uint64]struct{}),
		},
	}

	var serverMap sync.Map
	var errorMap sync.Map
	wg := sync.WaitGroup{}
	for serverName, server := range newTestCluster.servers {
		serverCfg := server.GetConfig()
		wg.Add(1)
		go func(serverName string, server *TestServer) {
			defer wg.Done()
			err := server.Destroy()
			if err != nil {
				return
			}
			var (
				newServer *TestServer
				serverErr error
			)
			if isKeyspaceGroupEnabled {
				newServer, serverErr = NewTestServer(ctx, serverCfg, []string{constant.PDServiceName})
			} else {
				newServer, serverErr = NewTestServer(ctx, serverCfg, nil)
			}
			serverMap.Store(serverName, newServer)
			errorMap.Store(serverName, serverErr)
		}(serverName, server)
	}
	wg.Wait()

	errorMap.Range(func(key, value any) bool {
		if value != nil {
			err = value.(error)
			return false
		}
		serverName := key.(string)
		newServer, _ := serverMap.Load(serverName)
		newTestCluster.servers[serverName] = newServer.(*TestServer)
		return true
	})

	if err != nil {
		return nil, errors.New("failed to restart cluster. " + err.Error())
	}

	return newTestCluster, nil
}

// RunServer starts to run TestServer.
func RunServer(server *TestServer) <-chan error {
	resC := make(chan error)
	go func() { resC <- server.Run() }()
	return resC
}

// RunServers starts to run multiple TestServer.
func RunServers(servers []*TestServer) error {
	res := make([]<-chan error, len(servers))
	for i, s := range servers {
		res[i] = RunServer(s)
	}
	for _, c := range res {
		if err := <-c; err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// RunInitialServers starts to run servers in InitialServers.
func (c *TestCluster) RunInitialServers() error {
	return c.runInitialServersWithRetry(defaultMaxRetryTimes)
}

// runInitialServersWithRetry starts to run servers with port conflict handling.
func (c *TestCluster) runInitialServersWithRetry(maxRetries int) error {
	if maxRetries <= 0 {
		maxRetries = 1
	}
	var lastErr error
	for i := range maxRetries {
		servers := make([]*TestServer, 0, len(c.config.InitialServers))
		for _, conf := range c.config.InitialServers {
			servers = append(servers, c.GetServer(conf.Name))
		}

		lastErr = RunServers(servers)
		if lastErr == nil {
			return nil
		}

		errMsg := lastErr.Error()
		switch {
		case strings.Contains(errMsg, "address already in use"):
			log.Warn("port conflict detected, recreating servers with new ports",
				zap.Int("attempt", i+1),
				zap.Int("maxRetries", maxRetries),
				zap.Error(lastErr))

			// Stop and destroy all servers
			for _, s := range servers {
				if state := s.State(); state == Running || state == Starting {
					_ = s.Stop()
				}
				_ = s.Destroy()
			}

			// Recreate servers with new ports
			for _, conf := range c.config.InitialServers {
				// Regenerate config to get new ports
				conf.ClientURLs = tempurl.Alloc()
				conf.PeerURLs = tempurl.Alloc()
				conf.AdvertiseClientURLs = conf.ClientURLs
				conf.AdvertisePeerURLs = conf.PeerURLs

				// Use the original opts passed during cluster creation
				allOpts := append([]ConfigOption{WithGCTuner(false)}, c.opts...)
				serverConf, err := conf.Generate(allOpts...)
				if err != nil {
					return err
				}

				// Use the original services passed during cluster creation
				s, err := NewTestServer(c.ctx, serverConf, c.services)
				if err != nil {
					return err
				}
				c.servers[conf.Name] = s
			}

			// Wait before retry
			backoff := time.Duration(i+1) * 500 * time.Millisecond
			if backoff > 3*time.Second {
				backoff = 3 * time.Second
			}
			time.Sleep(backoff)
			continue
		case strings.Contains(errMsg, "ErrStartEtcd"):
			log.Warn("etcd start failed, will retry", zap.Error(lastErr))
			for _, s := range servers {
				if state := s.State(); state == Running || state == Starting {
					_ = s.Stop()
				}
			}
			time.Sleep(100 * time.Millisecond)
			continue
		default:
			// For other errors, don't retry
			return lastErr
		}
	}
	return errors.Wrapf(lastErr, "failed to start servers after %d retries", maxRetries)
}

// RunServersWithRetry starts to run multiple TestServer with retry logic.
func RunServersWithRetry(servers []*TestServer, maxRetries int) error {
	var lastErr error
	for range maxRetries {
		lastErr = RunServers(servers)
		if lastErr == nil {
			return nil
		}

		// If the error is related to etcd start cancellation, we should retry
		if strings.Contains(lastErr.Error(), "ErrCancelStartEtcd") ||
			strings.Contains(lastErr.Error(), "ErrStartEtcd") {
			log.Warn("etcd start failed, will retry", zap.Error(lastErr))
			// Stop any partially started servers before retrying
			for _, s := range servers {
				if state := s.State(); state == Running || state == Starting {
					_ = s.Stop()
				}
			}
			continue
		}

		// For other errors, don't retry
		return lastErr
	}
	return errors.Wrapf(lastErr, "failed to start servers after %d retries", maxRetries)
}

// StopAll is used to stop all servers.
func (c *TestCluster) StopAll() error {
	for _, s := range c.servers {
		if err := s.Stop(); err != nil {
			return err
		}
	}
	return nil
}

// DeleteServer is used to delete a server.
func (c *TestCluster) DeleteServer(name string) {
	delete(c.servers, name)
}

// GetServer returns a server with a given name.
func (c *TestCluster) GetServer(name string) *TestServer {
	return c.servers[name]
}

// GetServers returns all servers.
func (c *TestCluster) GetServers() map[string]*TestServer {
	return c.servers
}

// GetLeader returns the leader of all servers
func (c *TestCluster) GetLeader() string {
	for name, s := range c.servers {
		if s.IsLeader() {
			return name
		}
	}
	return ""
}

// GetFollower returns an follower of all servers
func (c *TestCluster) GetFollower() string {
	for name, s := range c.servers {
		if !s.server.IsClosed() && !s.server.GetMember().IsServing() {
			return name
		}
	}
	return ""
}

// GetLeaderServer returns the leader server of all servers
func (c *TestCluster) GetLeaderServer() *TestServer {
	return c.GetServer(c.GetLeader())
}

// WaitLeader is used to get leader.
// If it exceeds the maximum number of loops, it will return an empty string.
func (c *TestCluster) WaitLeader(ops ...WaitOption) string {
	option := &WaitOp{
		retryTimes:   WaitLeaderRetryTimes,
		waitInterval: WaitLeaderCheckInterval,
	}
	for _, op := range ops {
		op(option)
	}
	for range option.retryTimes {
		counter := make(map[string]int)
		running := 0
		for _, s := range c.servers {
			s.RLock()
			if s.state == Running {
				running++
			}
			s.RUnlock()
			n := s.GetLeader().GetName()
			if n != "" {
				counter[n]++
			}
		}
		for name, num := range counter {
			if num == running && c.GetServer(name).IsLeader() {
				time.Sleep(WaitLeaderReturnDelay)
				return name
			}
		}
		time.Sleep(option.waitInterval)
	}
	return ""
}

// WaitRegionSyncerClientsReady is used to wait the region syncer clients establish the connection.
// n means wait n clients.
func (c *TestCluster) WaitRegionSyncerClientsReady(n int) bool {
	option := &WaitOp{
		retryTimes:   40,
		waitInterval: WaitLeaderCheckInterval,
	}
	for range option.retryTimes {
		name := c.GetLeader()
		if len(name) == 0 {
			time.Sleep(option.waitInterval)
			continue
		}
		leaderServer := c.GetServer(name)
		clus := leaderServer.GetServer().GetRaftCluster()
		if clus != nil {
			if len(clus.GetRegionSyncer().GetAllDownstreamNames()) == n {
				return true
			}
		}
		time.Sleep(option.waitInterval)
	}
	return false
}

// ResignLeader resigns the leader of the cluster.
func (c *TestCluster) ResignLeader() error {
	leader := c.GetLeader()
	if leader != "" {
		return c.servers[leader].ResignLeader()
	}
	return errors.New("no leader")
}

// GetCluster returns PD cluster.
func (c *TestCluster) GetCluster() *metapb.Cluster {
	leader := c.GetLeader()
	return c.servers[leader].GetCluster()
}

// GetClusterStatus returns raft cluster status.
func (c *TestCluster) GetClusterStatus() (*cluster.Status, error) {
	leader := c.GetLeader()
	return c.servers[leader].GetRaftCluster().LoadClusterStatus()
}

// GetEtcdClient returns the builtin etcd client.
func (c *TestCluster) GetEtcdClient() *clientv3.Client {
	leader := c.GetLeader()
	return c.servers[leader].GetEtcdClient()
}

// GetHTTPClient returns the builtin http client.
func (c *TestCluster) GetHTTPClient() *http.Client {
	leader := c.GetLeader()
	return c.servers[leader].GetHTTPClient()
}

// GetConfig returns the current TestCluster's configuration.
func (c *TestCluster) GetConfig() *clusterConfig {
	return c.config
}

// HandleRegionHeartbeat processes RegionInfo reports from the client.
func (c *TestCluster) HandleRegionHeartbeat(region *core.RegionInfo) error {
	leader := c.GetLeader()
	cluster := c.servers[leader].GetRaftCluster()
	return cluster.HandleRegionHeartbeat(region)
}

// HandleRegionBuckets processes BucketInfo reports from the client.
func (c *TestCluster) HandleRegionBuckets(b *metapb.Buckets) error {
	leader := c.GetLeader()
	cluster := c.servers[leader].GetRaftCluster()
	return cluster.HandleRegionBuckets(b)
}

// Join is used to add a new TestServer into the cluster.
func (c *TestCluster) Join(ctx context.Context, opts ...ConfigOption) (*TestServer, error) {
	conf, err := c.config.join().Generate(opts...)
	if err != nil {
		return nil, err
	}
	s, err := NewTestServer(ctx, conf, nil)
	if err != nil {
		return nil, err
	}
	c.servers[conf.Name] = s
	return s, nil
}

// JoinWithKeyspaceGroup is used to add a new TestServer into the cluster with keyspace group enabled.
func (c *TestCluster) JoinWithKeyspaceGroup(ctx context.Context, opts ...ConfigOption) (*TestServer, error) {
	conf, err := c.config.join().Generate(opts...)
	if err != nil {
		return nil, err
	}
	s, err := NewTestServer(ctx, conf, []string{constant.PDServiceName})
	if err != nil {
		return nil, err
	}
	c.servers[conf.Name] = s
	return s, nil
}

// Destroy is used to destroy a TestCluster.
func (c *TestCluster) Destroy() {
	for _, s := range c.servers {
		err := s.Destroy()
		if err != nil {
			log.Error("failed to destroy the cluster:", errs.ZapError(err))
		}
	}
	if c.schedulingCluster != nil {
		c.schedulingCluster.Destroy()
	}
	if c.tsoCluster != nil {
		c.tsoCluster.Destroy()
	}
}

// CheckTSOUnique will check whether the TSO is unique among the cluster in the past and present.
func (c *TestCluster) CheckTSOUnique(ts uint64) bool {
	c.tsPool.Lock()
	defer c.tsPool.Unlock()
	if _, exist := c.tsPool.pool[ts]; exist {
		return false
	}
	c.tsPool.pool[ts] = struct{}{}
	return true
}

// GetSchedulingPrimaryServer returns the scheduling primary server.
func (c *TestCluster) GetSchedulingPrimaryServer() *scheduling.Server {
	if c.schedulingCluster == nil {
		return nil
	}
	return c.schedulingCluster.GetPrimaryServer()
}

// GetDefaultTSOPrimaryServer returns the primary TSO server for the default keyspace.
func (c *TestCluster) GetDefaultTSOPrimaryServer() *tso.Server {
	if c.tsoCluster == nil {
		return nil
	}
	return c.tsoCluster.GetPrimaryServer(ks.DefaultKeyspaceID, ks.DefaultKeyspaceGroupID)
}

// SetSchedulingCluster sets the scheduling cluster.
func (c *TestCluster) SetSchedulingCluster(cluster *TestSchedulingCluster) {
	c.schedulingCluster = cluster
}

// SetTSOCluster sets the TSO cluster.
func (c *TestCluster) SetTSOCluster(cluster *TestTSOCluster) {
	c.tsoCluster = cluster
}

// WaitOp represent the wait configuration
type WaitOp struct {
	retryTimes   int
	waitInterval time.Duration
}

// WaitOption represent the wait configuration
type WaitOption func(*WaitOp)

// WithRetryTimes indicates the retry times
func WithRetryTimes(r int) WaitOption {
	return func(op *WaitOp) { op.retryTimes = r }
}

// WithWaitInterval indicates the wait interval
func WithWaitInterval(i time.Duration) WaitOption {
	return func(op *WaitOp) { op.waitInterval = i }
}
