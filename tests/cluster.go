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
	"sync"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/autoscaling"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/dashboard"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/id"
	"github.com/tikv/pd/pkg/keyspace"
	scheduling "github.com/tikv/pd/pkg/mcs/scheduling/server"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/swaggerserver"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/apiv2"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/join"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestServer states.
const (
	Initial int32 = iota
	Running
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
func NewTestServer(ctx context.Context, cfg *config.Config) (*TestServer, error) {
	return createTestServer(ctx, cfg, nil)
}

// NewTestAPIServer creates a new TestServer.
func NewTestAPIServer(ctx context.Context, cfg *config.Config) (*TestServer, error) {
	return createTestServer(ctx, cfg, []string{constant.APIServiceName})
}

func createTestServer(ctx context.Context, cfg *config.Config, services []string) (*TestServer, error) {
	//  disable the heartbeat async runner in test
	cfg.Schedule.EnableHeartbeatConcurrentRunner = false
	err := logutil.SetupLogger(cfg.Log, &cfg.Logger, &cfg.LogProps, cfg.Security.RedactInfoLog)
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
	serviceBuilders := []server.HandlerBuilder{api.NewHandler, apiv2.NewV2Handler, autoscaling.NewHandler}
	if swaggerserver.Enabled() {
		serviceBuilders = append(serviceBuilders, swaggerserver.NewHandler)
	}
	serviceBuilders = append(serviceBuilders, dashboard.GetServiceBuilders()...)
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
	defer s.Unlock()
	if s.state != Initial && s.state != Stop {
		return errors.Errorf("server(state%d) cannot run", s.state)
	}
	if err := s.server.Run(); err != nil {
		return err
	}
	s.state = Running
	return nil
}

// Stop is used to stop a TestServer.
func (s *TestServer) Stop() error {
	s.Lock()
	defer s.Unlock()
	if s.state != Running {
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
	if s.state == Running {
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
	s.server.GetMember().ResetLeader()
}

// ResignLeader resigns the leader of the server.
func (s *TestServer) ResignLeader() error {
	s.Lock()
	defer s.Unlock()
	s.server.GetMember().ResetLeader()
	return s.server.GetMember().ResignEtcdLeader(s.server.Context(), s.server.Name(), "")
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

// SetEnableLocalTSO sets the enable-local-tso flag of the TestServer.
func (s *TestServer) SetEnableLocalTSO(enableLocalTSO bool) {
	s.server.SetEnableLocalTSO(enableLocalTSO)
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

// GetAllocatorLeader returns current allocator leader
// of PD cluster for given dc-location.
func (s *TestServer) GetAllocatorLeader(dcLocation string) *pdpb.Member {
	// For the leader of Global TSO Allocator, it's the PD leader
	if dcLocation == tso.GlobalDCLocation {
		return s.GetLeader()
	}
	tsoAllocatorManager := s.GetTSOAllocatorManager()
	allocator, err := tsoAllocatorManager.GetAllocator(dcLocation)
	if err != nil {
		return nil
	}
	return allocator.(*tso.LocalTSOAllocator).GetAllocatorLeader()
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
	return !s.server.IsClosed() && s.server.GetMember().IsLeader()
}

// IsAllocatorLeader returns whether the server is a TSO Allocator leader or not.
func (s *TestServer) IsAllocatorLeader(dcLocation string) bool {
	if dcLocation == tso.GlobalDCLocation {
		return s.IsLeader()
	}
	tsoAllocatorManager := s.GetTSOAllocatorManager()
	allocator, err := tsoAllocatorManager.GetAllocator(dcLocation)
	if err != nil {
		return false
	}
	return !s.server.IsClosed() && allocator.(*tso.LocalTSOAllocator).IsAllocatorLeader()
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
		Store:  &metapb.Store{Id: 1, Address: "mock://1", LastHeartbeat: time.Now().UnixNano()},
		Region: &metapb.Region{Id: 2, Peers: []*metapb.Peer{{Id: 3, StoreId: 1, Role: metapb.PeerRole_Voter}}},
	}
	resp, err := s.grpcServer.Bootstrap(context.Background(), bootstrapReq)
	if err != nil {
		return err
	}
	if resp.GetHeader().GetError() != nil {
		return errors.New(resp.GetHeader().GetError().String())
	}
	return nil
}

// WaitLeader is used to get instant leader info in order to
// make a test know the PD leader has been elected as soon as possible.
// If it exceeds the maximum number of loops, it will return nil.
func (s *TestServer) WaitLeader() bool {
	for range WaitLeaderRetryTimes {
		if s.server.GetMember().IsLeader() {
			return true
		}
		time.Sleep(WaitLeaderCheckInterval)
	}
	return false
}

// GetTSOAllocatorManager returns the server's TSO Allocator Manager.
func (s *TestServer) GetTSOAllocatorManager() *tso.AllocatorManager {
	return s.server.GetTSOAllocatorManager()
}

// GetServicePrimaryAddr returns the primary address of the service.
func (s *TestServer) GetServicePrimaryAddr(ctx context.Context, serviceName string) (string, bool) {
	return s.server.GetServicePrimaryAddr(ctx, serviceName)
}

// TestCluster is only for test.
type TestCluster struct {
	config  *clusterConfig
	servers map[string]*TestServer
	// tsPool is used to check the TSO uniqueness among the test cluster
	tsPool struct {
		syncutil.Mutex
		pool map[uint64]struct{}
	}
	schedulingCluster *TestSchedulingCluster
}

// ConfigOption is used to define customize settings in test.
// You can use serverName to customize a config for a certain
// server. Usually, the server name will be like `pd1`, `pd2`
// and so on, which determined by the number of servers you set.
type ConfigOption func(conf *config.Config, serverName string)

// NewTestCluster creates a new TestCluster.
func NewTestCluster(ctx context.Context, initialServerCount int, opts ...ConfigOption) (*TestCluster, error) {
	return createTestCluster(ctx, initialServerCount, false, opts...)
}

// NewTestAPICluster creates a new TestCluster with API service.
func NewTestAPICluster(ctx context.Context, initialServerCount int, opts ...ConfigOption) (*TestCluster, error) {
	return createTestCluster(ctx, initialServerCount, true, opts...)
}

func createTestCluster(ctx context.Context, initialServerCount int, isAPIServiceMode bool, opts ...ConfigOption) (*TestCluster, error) {
	schedulers.Register()
	config := newClusterConfig(initialServerCount)
	servers := make(map[string]*TestServer)
	for _, cfg := range config.InitialServers {
		serverConf, err := cfg.Generate(opts...)
		if err != nil {
			return nil, err
		}
		var s *TestServer
		if isAPIServiceMode {
			s, err = NewTestAPIServer(ctx, serverConf)
		} else {
			s, err = NewTestServer(ctx, serverConf)
		}
		if err != nil {
			return nil, err
		}
		servers[cfg.Name] = s
	}
	return &TestCluster{
		config:  config,
		servers: servers,
		tsPool: struct {
			syncutil.Mutex
			pool map[uint64]struct{}
		}{
			pool: make(map[uint64]struct{}),
		},
	}, nil
}

// RestartTestAPICluster restarts the API test cluster.
func RestartTestAPICluster(ctx context.Context, cluster *TestCluster) (*TestCluster, error) {
	return restartTestCluster(ctx, cluster, true)
}

func restartTestCluster(
	ctx context.Context, cluster *TestCluster, isAPIServiceMode bool,
) (newTestCluster *TestCluster, err error) {
	schedulers.Register()
	newTestCluster = &TestCluster{
		config:  cluster.config,
		servers: make(map[string]*TestServer, len(cluster.servers)),
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
			server.Destroy()
			var (
				newServer *TestServer
				serverErr error
			)
			if isAPIServiceMode {
				newServer, serverErr = NewTestAPIServer(ctx, serverCfg)
			} else {
				newServer, serverErr = NewTestServer(ctx, serverCfg)
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
	servers := make([]*TestServer, 0, len(c.config.InitialServers))
	for _, conf := range c.config.InitialServers {
		servers = append(servers, c.GetServer(conf.Name))
	}
	return RunServers(servers)
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
		if !s.server.IsClosed() && !s.server.GetMember().IsLeader() {
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

// WaitAllocatorLeader is used to get the Local TSO Allocator leader.
// If it exceeds the maximum number of loops, it will return an empty string.
func (c *TestCluster) WaitAllocatorLeader(dcLocation string, ops ...WaitOption) string {
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
			if s.state == Running && s.GetTSOAllocatorManager().EnableLocalTSO() {
				running++
			}
			serverName := s.GetAllocatorLeader(dcLocation).GetName()
			if serverName != "" {
				counter[serverName]++
			}
		}
		for serverName, num := range counter {
			if num == running && c.GetServer(serverName).IsAllocatorLeader(dcLocation) {
				return serverName
			}
		}
		time.Sleep(option.waitInterval)
	}
	return ""
}

// WaitAllLeaders will block and wait for the election of PD leader and all Local TSO Allocator leaders.
func (c *TestCluster) WaitAllLeaders(re *require.Assertions, dcLocations map[string]string) {
	c.WaitLeader()
	c.CheckClusterDCLocation()
	// Wait for each DC's Local TSO Allocator leader
	wg := sync.WaitGroup{}
	for _, dcLocation := range dcLocations {
		wg.Add(1)
		go func(dc string) {
			testutil.Eventually(re, func() bool {
				return c.WaitAllocatorLeader(dc) != ""
			})
			wg.Done()
		}(dcLocation)
	}
	wg.Wait()
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

// HandleReportBuckets processes BucketInfo reports from the client.
func (c *TestCluster) HandleReportBuckets(b *metapb.Buckets) error {
	leader := c.GetLeader()
	cluster := c.servers[leader].GetRaftCluster()
	return cluster.HandleReportBuckets(b)
}

// Join is used to add a new TestServer into the cluster.
func (c *TestCluster) Join(ctx context.Context, opts ...ConfigOption) (*TestServer, error) {
	conf, err := c.config.join().Generate(opts...)
	if err != nil {
		return nil, err
	}
	s, err := NewTestServer(ctx, conf)
	if err != nil {
		return nil, err
	}
	c.servers[conf.Name] = s
	return s, nil
}

// JoinAPIServer is used to add a new TestAPIServer into the cluster.
func (c *TestCluster) JoinAPIServer(ctx context.Context, opts ...ConfigOption) (*TestServer, error) {
	conf, err := c.config.join().Generate(opts...)
	if err != nil {
		return nil, err
	}
	s, err := NewTestAPIServer(ctx, conf)
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
}

// CheckClusterDCLocation will force the cluster to do the dc-location check in order to speed up the test.
func (c *TestCluster) CheckClusterDCLocation() {
	wg := sync.WaitGroup{}
	for _, server := range c.GetServers() {
		wg.Add(1)
		go func(s *TestServer) {
			s.GetTSOAllocatorManager().ClusterDCLocationChecker()
			wg.Done()
		}(server)
	}
	wg.Wait()
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

// SetSchedulingCluster sets the scheduling cluster.
func (c *TestCluster) SetSchedulingCluster(cluster *TestSchedulingCluster) {
	c.schedulingCluster = cluster
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
