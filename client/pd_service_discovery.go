// Copyright 2019 TiKV Project Authors.
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

package pd

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/grpcutil"
	"github.com/tikv/pd/client/retry"
	"github.com/tikv/pd/client/tlsutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

const (
	globalDCLocation            = "global"
	memberUpdateInterval        = time.Minute
	serviceModeUpdateInterval   = 3 * time.Second
	updateMemberTimeout         = time.Second // Use a shorter timeout to recover faster from network isolation.
	updateMemberBackOffBaseTime = 100 * time.Millisecond
)

// MemberHealthCheckInterval might be changed in the unit to shorten the testing time.
var MemberHealthCheckInterval = time.Second

type apiKind int

const (
	forwardAPIKind apiKind = iota
	apiKindCount
)

type serviceType int

const (
	apiService serviceType = iota
	tsoService
)

// ServiceDiscovery defines the general interface for service discovery on a quorum-based cluster
// or a primary/secondary configured cluster.
type ServiceDiscovery interface {
	// Init initialize the concrete client underlying
	Init() error
	// Close releases all resources
	Close()
	// GetClusterID returns the ID of the cluster
	GetClusterID() uint64
	// GetKeyspaceID returns the ID of the keyspace
	GetKeyspaceID() uint32
	// GetKeyspaceGroupID returns the ID of the keyspace group
	GetKeyspaceGroupID() uint32
	// GetServiceURLs returns the URLs of the servers providing the service
	GetServiceURLs() []string
	// GetServingEndpointClientConn returns the grpc client connection of the serving endpoint
	// which is the leader in a quorum-based cluster or the primary in a primary/secondary
	// configured cluster.
	GetServingEndpointClientConn() *grpc.ClientConn
	// GetClientConns returns the mapping {addr -> a gRPC connection}
	GetClientConns() *sync.Map
	// GetServingAddr returns the serving endpoint which is the leader in a quorum-based cluster
	// or the primary in a primary/secondary configured cluster.
	GetServingAddr() string
	// GetBackupAddrs gets the addresses of the current reachable backup service
	// endpoints. Backup service endpoints are followers in a quorum-based cluster or
	// secondaries in a primary/secondary configured cluster.
	GetBackupAddrs() []string
	// GetServiceClient tries to get the leader/primary ServiceClient.
	// If the leader ServiceClient meets network problem,
	// it returns a follower/secondary ServiceClient which can forward the request to leader.
	GetServiceClient() ServiceClient
	// GetOrCreateGRPCConn returns the corresponding grpc client connection of the given addr
	GetOrCreateGRPCConn(addr string) (*grpc.ClientConn, error)
	// ScheduleCheckMemberChanged is used to trigger a check to see if there is any membership change
	// among the leader/followers in a quorum-based cluster or among the primary/secondaries in a
	// primary/secondary configured cluster.
	ScheduleCheckMemberChanged()
	// CheckMemberChanged immediately check if there is any membership change among the leader/followers
	// in a quorum-based cluster or among the primary/secondaries in a primary/secondary configured cluster.
	CheckMemberChanged() error
	// AddServingAddrSwitchedCallback adds callbacks which will be called when the leader
	// in a quorum-based cluster or the primary in a primary/secondary configured cluster
	// is switched.
	AddServingAddrSwitchedCallback(callbacks ...func())
	// AddServiceAddrsSwitchedCallback adds callbacks which will be called when any leader/follower
	// in a quorum-based cluster or any primary/secondary in a primary/secondary configured cluster
	// is changed.
	AddServiceAddrsSwitchedCallback(callbacks ...func())
}

// ServiceClient is an interface that defines a set of operations for a raw PD gRPC client to specific PD server.
type ServiceClient interface {
	// GetAddress returns the address information of the PD server.
	GetAddress() string
	// GetClientConn returns the gRPC connection of the service client
	GetClientConn() *grpc.ClientConn
	// BuildGRPCTargetContext builds a context object with a gRPC context.
	// ctx: the original context object.
	// mustLeader: whether must send to leader.
	BuildGRPCTargetContext(ctx context.Context, mustLeader bool) context.Context
	// IsConnectedToLeader returns whether the connected PD server is leader.
	IsConnectedToLeader() bool
	// Available returns if the network or other availability for the current service client is available.
	Available() bool
	// NeedRetry checks if client need to retry based on the PD server error response.
	// And It will mark the client as unavailable if the pd error shows the follower can't handle request.
	NeedRetry(*pdpb.Error, error) bool
}

var (
	_ ServiceClient = (*pdServiceClient)(nil)
	_ ServiceClient = (*pdServiceAPIClient)(nil)
)

type pdServiceClient struct {
	addr       string
	conn       *grpc.ClientConn
	isLeader   bool
	leaderAddr string

	networkFailure atomic.Bool
}

func newPDServiceClient(addr, leaderAddr string, conn *grpc.ClientConn, isLeader bool) ServiceClient {
	cli := &pdServiceClient{
		addr:       addr,
		conn:       conn,
		isLeader:   isLeader,
		leaderAddr: leaderAddr,
	}
	if conn == nil {
		cli.networkFailure.Store(true)
	}
	return cli
}

// GetAddress implements ServiceClient.
func (c *pdServiceClient) GetAddress() string {
	if c == nil {
		return ""
	}
	return c.addr
}

// BuildGRPCTargetContext implements ServiceClient.
func (c *pdServiceClient) BuildGRPCTargetContext(ctx context.Context, toLeader bool) context.Context {
	if c == nil || c.isLeader {
		return ctx
	}
	if toLeader {
		return grpcutil.BuildForwardContext(ctx, c.leaderAddr)
	}
	return grpcutil.BuildFollowerHandleContext(ctx)
}

// IsConnectedToLeader implements ServiceClient.
func (c *pdServiceClient) IsConnectedToLeader() bool {
	if c == nil {
		return false
	}
	return c.isLeader
}

// Available implements ServiceClient.
func (c *pdServiceClient) Available() bool {
	if c == nil {
		return false
	}
	return !c.networkFailure.Load()
}

func (c *pdServiceClient) checkNetworkAvailable(ctx context.Context) {
	if c == nil || c.conn == nil {
		return
	}
	healthCli := healthpb.NewHealthClient(c.conn)
	resp, err := healthCli.Check(ctx, &healthpb.HealthCheckRequest{Service: ""})
	failpoint.Inject("unreachableNetwork1", func(val failpoint.Value) {
		if val, ok := val.(string); (ok && val == c.GetAddress()) || !ok {
			resp = nil
			err = status.New(codes.Unavailable, "unavailable").Err()
		}
	})
	rpcErr, ok := status.FromError(err)
	if (ok && isNetworkError(rpcErr.Code())) || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		c.networkFailure.Store(true)
	} else {
		c.networkFailure.Store(false)
	}
}

func isNetworkError(code codes.Code) bool {
	return code == codes.Unavailable || code == codes.DeadlineExceeded
}

// GetClientConn implements ServiceClient.
func (c *pdServiceClient) GetClientConn() *grpc.ClientConn {
	if c == nil {
		return nil
	}
	return c.conn
}

// NeedRetry implements ServiceClient.
func (c *pdServiceClient) NeedRetry(pdErr *pdpb.Error, err error) bool {
	if c.IsConnectedToLeader() {
		return false
	}
	return !(err == nil && pdErr == nil)
}

type errFn func(pdErr *pdpb.Error) bool

func emptyErrorFn(pdErr *pdpb.Error) bool {
	return false
}

func regionAPIErrorFn(pdErr *pdpb.Error) bool {
	return pdErr.GetType() == pdpb.ErrorType_REGION_NOT_FOUND
}

// pdServiceAPIClient is a specific API client for PD service.
// It extends the pdServiceClient and adds additional fields for managing availability
type pdServiceAPIClient struct {
	ServiceClient
	fn errFn

	unavailable      atomic.Bool
	unavailableUntil atomic.Value
}

func newPDServiceAPIClient(client ServiceClient, f errFn) ServiceClient {
	return &pdServiceAPIClient{
		ServiceClient: client,
		fn:            f,
	}
}

// Available implements ServiceClient.
func (c *pdServiceAPIClient) Available() bool {
	return c.ServiceClient.Available() && !c.unavailable.Load()
}

// markAsAvailable is used to try to mark the client as available if unavailable status is expired.
func (c *pdServiceAPIClient) markAsAvailable() {
	if !c.unavailable.Load() {
		return
	}
	until := c.unavailableUntil.Load().(time.Time)
	if time.Now().After(until) {
		c.unavailable.Store(false)
	}
}

// NeedRetry implements ServiceClient.
func (c *pdServiceAPIClient) NeedRetry(pdErr *pdpb.Error, err error) bool {
	if c.IsConnectedToLeader() {
		return false
	}
	if err == nil && pdErr == nil {
		return false
	}
	if c.fn(pdErr) && c.unavailable.CompareAndSwap(false, true) {
		c.unavailableUntil.Store(time.Now().Add(time.Second * 10))
		failpoint.Inject("fastCheckAvailable", func() {
			c.unavailableUntil.Store(time.Now().Add(time.Millisecond * 100))
		})
	}
	return true
}

// pdServiceBalancerNode is a balancer node for PD service.
// It extends the pdServiceClient and adds additional fields for the next polling client in the chain.
type pdServiceBalancerNode struct {
	*pdServiceAPIClient
	next *pdServiceBalancerNode
}

// pdServiceBalancer is a load balancer for PD service clients.
// It is used to balance the request to all servers and manage the connections to multiple PD service nodes.
type pdServiceBalancer struct {
	mu        sync.Mutex
	now       *pdServiceBalancerNode
	totalNode int
	errFn     errFn
}

func newPDServiceBalancer(fn errFn) *pdServiceBalancer {
	return &pdServiceBalancer{
		errFn: fn,
	}
}
func (c *pdServiceBalancer) set(clients []ServiceClient) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(clients) == 0 {
		return
	}
	c.totalNode = len(clients)
	head := &pdServiceBalancerNode{
		pdServiceAPIClient: newPDServiceAPIClient(clients[0], c.errFn).(*pdServiceAPIClient),
	}
	head.next = head
	last := head
	for i := 1; i < c.totalNode; i++ {
		next := &pdServiceBalancerNode{
			pdServiceAPIClient: newPDServiceAPIClient(clients[i], c.errFn).(*pdServiceAPIClient),
			next:               head,
		}
		head = next
		last.next = head
	}
	c.now = head
}

func (c *pdServiceBalancer) check() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < c.totalNode; i++ {
		c.now.markAsAvailable()
		c.next()
	}
}

func (c *pdServiceBalancer) next() {
	c.now = c.now.next
}

func (c *pdServiceBalancer) get() (ret ServiceClient) {
	c.mu.Lock()
	defer c.mu.Unlock()
	i := 0
	if c.now == nil {
		return nil
	}
	for ; i < c.totalNode; i++ {
		if c.now.Available() {
			ret = c.now
			c.next()
			return
		}
		c.next()
	}
	return
}

type updateKeyspaceIDFunc func() error
type tsoLocalServAddrsUpdatedFunc func(map[string]string) error
type tsoGlobalServAddrUpdatedFunc func(string) error

type tsoAllocatorEventSource interface {
	// SetTSOLocalServAddrsUpdatedCallback adds a callback which will be called when the local tso
	// allocator leader list is updated.
	SetTSOLocalServAddrsUpdatedCallback(callback tsoLocalServAddrsUpdatedFunc)
	// SetTSOGlobalServAddrUpdatedCallback adds a callback which will be called when the global tso
	// allocator leader is updated.
	SetTSOGlobalServAddrUpdatedCallback(callback tsoGlobalServAddrUpdatedFunc)
}

var _ ServiceDiscovery = (*pdServiceDiscovery)(nil)
var _ tsoAllocatorEventSource = (*pdServiceDiscovery)(nil)

// pdServiceDiscovery is the service discovery client of PD/API service which is quorum based
type pdServiceDiscovery struct {
	isInitialized bool

	urls atomic.Value // Store as []string
	// PD leader URL
	leader atomic.Value // Store as pdServiceClient
	// PD follower URLs
	followers         sync.Map // Store as map[string]pdServiceClient
	apiCandidateNodes [apiKindCount]*pdServiceBalancer
	// PD follower URLs. Only for tso.
	followerAddresses atomic.Value // Store as []string

	clusterID uint64
	// addr -> a gRPC connection
	clientConns sync.Map // Store as map[string]*grpc.ClientConn

	// serviceModeUpdateCb will be called when the service mode gets updated
	serviceModeUpdateCb func(pdpb.ServiceMode)
	// leaderSwitchedCbs will be called after the leader switched
	leaderSwitchedCbs []func()
	// membersChangedCbs will be called after there is any membership change in the
	// leader and followers
	membersChangedCbs []func()
	// tsoLocalAllocLeadersUpdatedCb will be called when the local tso allocator
	// leader list is updated. The input is a map {DC Location -> Leader Addr}
	tsoLocalAllocLeadersUpdatedCb tsoLocalServAddrsUpdatedFunc
	// tsoGlobalAllocLeaderUpdatedCb will be called when the global tso allocator
	// leader is updated.
	tsoGlobalAllocLeaderUpdatedCb tsoGlobalServAddrUpdatedFunc

	checkMembershipCh chan struct{}

	wg        *sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once

	updateKeyspaceIDCb updateKeyspaceIDFunc
	keyspaceID         uint32
	tlsCfg             *tlsutil.TLSConfig
	// Client option.
	option *option
}

// newPDServiceDiscovery returns a new PD service discovery-based client.
func newPDServiceDiscovery(
	ctx context.Context, cancel context.CancelFunc,
	wg *sync.WaitGroup,
	serviceModeUpdateCb func(pdpb.ServiceMode),
	updateKeyspaceIDCb updateKeyspaceIDFunc,
	keyspaceID uint32,
	urls []string, tlsCfg *tlsutil.TLSConfig, option *option,
) *pdServiceDiscovery {
	pdsd := &pdServiceDiscovery{
		checkMembershipCh:   make(chan struct{}, 1),
		ctx:                 ctx,
		cancel:              cancel,
		wg:                  wg,
		apiCandidateNodes:   [apiKindCount]*pdServiceBalancer{newPDServiceBalancer(emptyErrorFn)},
		serviceModeUpdateCb: serviceModeUpdateCb,
		updateKeyspaceIDCb:  updateKeyspaceIDCb,
		keyspaceID:          keyspaceID,
		tlsCfg:              tlsCfg,
		option:              option,
	}
	pdsd.urls.Store(urls)
	return pdsd
}

func (c *pdServiceDiscovery) Init() error {
	if c.isInitialized {
		return nil
	}

	if err := c.initRetry(c.initClusterID); err != nil {
		c.cancel()
		return err
	}
	if err := c.initRetry(c.updateMember); err != nil {
		c.cancel()
		return err
	}
	log.Info("[pd] init cluster id", zap.Uint64("cluster-id", c.clusterID))

	// We need to update the keyspace ID before we discover and update the service mode
	// so that TSO in API mode can be initialized with the correct keyspace ID.
	if c.updateKeyspaceIDCb != nil {
		if err := c.updateKeyspaceIDCb(); err != nil {
			return err
		}
	}

	if err := c.checkServiceModeChanged(); err != nil {
		log.Warn("[pd] failed to check service mode and will check later", zap.Error(err))
	}

	c.wg.Add(3)
	go c.updateMemberLoop()
	go c.updateServiceModeLoop()
	go c.memberHealthCheckLoop()

	c.isInitialized = true
	return nil
}

func (c *pdServiceDiscovery) initRetry(f func() error) error {
	var err error
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for i := 0; i < c.option.maxRetryTimes; i++ {
		if err = f(); err == nil {
			return nil
		}
		select {
		case <-c.ctx.Done():
			return err
		case <-ticker.C:
		}
	}
	return errors.WithStack(err)
}

func (c *pdServiceDiscovery) updateMemberLoop() {
	defer c.wg.Done()

	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	ticker := time.NewTicker(memberUpdateInterval)
	defer ticker.Stop()

	bo := retry.InitialBackOffer(updateMemberBackOffBaseTime, updateMemberTimeout)
	for {
		select {
		case <-ctx.Done():
			log.Info("[pd] exit member loop due to context canceled")
			return
		case <-ticker.C:
		case <-c.checkMembershipCh:
		}
		failpoint.Inject("skipUpdateMember", func() {
			failpoint.Continue()
		})
		if err := bo.Exec(ctx, c.updateMember); err != nil {
			log.Error("[pd] failed to update member", zap.Strings("urls", c.GetServiceURLs()), errs.ZapError(err))
		}
	}
}

func (c *pdServiceDiscovery) updateServiceModeLoop() {
	defer c.wg.Done()
	failpoint.Inject("skipUpdateServiceMode", func() {
		failpoint.Return()
	})
	failpoint.Inject("usePDServiceMode", func() {
		c.serviceModeUpdateCb(pdpb.ServiceMode_PD_SVC_MODE)
		failpoint.Return()
	})

	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	ticker := time.NewTicker(serviceModeUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err := c.checkServiceModeChanged(); err != nil {
			log.Error("[pd] failed to update service mode",
				zap.Strings("urls", c.GetServiceURLs()), errs.ZapError(err))
			c.ScheduleCheckMemberChanged() // check if the leader changed
		}
	}
}
func (c *pdServiceDiscovery) memberHealthCheckLoop() {
	defer c.wg.Done()

	memberCheckLoopCtx, memberCheckLoopCancel := context.WithCancel(c.ctx)
	defer memberCheckLoopCancel()

	ticker := time.NewTicker(MemberHealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.checkLeaderHealth(memberCheckLoopCtx)
			c.checkFollowerHealth(memberCheckLoopCtx)
		}
	}
}

func (c *pdServiceDiscovery) checkLeaderHealth(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	defer cancel()
	leader := c.getLeaderServiceClient()
	leader.checkNetworkAvailable(ctx)
}

func (c *pdServiceDiscovery) checkFollowerHealth(ctx context.Context) {
	c.followers.Range(func(key, value any) bool {
		// To ensure that the leader's healthy check is not delayed, shorten the duration.
		ctx, cancel := context.WithTimeout(ctx, MemberHealthCheckInterval/3)
		defer cancel()
		serviceClient := value.(*pdServiceClient)
		serviceClient.checkNetworkAvailable(ctx)
		return true
	})
	for _, balancer := range c.apiCandidateNodes {
		balancer.check()
	}
}

// Close releases all resources.
func (c *pdServiceDiscovery) Close() {
	c.closeOnce.Do(func() {
		log.Info("[pd] close pd service discovery client")
		c.clientConns.Range(func(key, cc interface{}) bool {
			if err := cc.(*grpc.ClientConn).Close(); err != nil {
				log.Error("[pd] failed to close grpc clientConn", errs.ZapError(errs.ErrCloseGRPCConn, err))
			}
			c.clientConns.Delete(key)
			return true
		})
	})
}

// GetClusterID returns the ClusterID.
func (c *pdServiceDiscovery) GetClusterID() uint64 {
	return c.clusterID
}

// GetKeyspaceID returns the ID of the keyspace
func (c *pdServiceDiscovery) GetKeyspaceID() uint32 {
	return c.keyspaceID
}

// SetKeyspaceID sets the ID of the keyspace
func (c *pdServiceDiscovery) SetKeyspaceID(keyspaceID uint32) {
	c.keyspaceID = keyspaceID
}

// GetKeyspaceGroupID returns the ID of the keyspace group
func (c *pdServiceDiscovery) GetKeyspaceGroupID() uint32 {
	// PD/API service only supports the default keyspace group
	return defaultKeySpaceGroupID
}

// DiscoverMicroservice discovers the microservice with the specified type and returns the server urls.
func (c *pdServiceDiscovery) discoverMicroservice(svcType serviceType) (urls []string, err error) {
	switch svcType {
	case apiService:
		urls = c.GetServiceURLs()
	case tsoService:
		leaderAddr := c.getLeaderAddr()
		if len(leaderAddr) > 0 {
			clusterInfo, err := c.getClusterInfo(c.ctx, leaderAddr, c.option.timeout)
			if err != nil {
				log.Error("[pd] failed to get cluster info",
					zap.String("leader-addr", leaderAddr), errs.ZapError(err))
				return nil, err
			}
			urls = clusterInfo.TsoUrls
		} else {
			err = errors.New("failed to get leader addr")
			return nil, err
		}
	default:
		panic("invalid service type")
	}

	return urls, nil
}

// GetServiceURLs returns the URLs of the servers.
// For testing use. It should only be called when the client is closed.
func (c *pdServiceDiscovery) GetServiceURLs() []string {
	return c.urls.Load().([]string)
}

// GetServingEndpointClientConn returns the grpc client connection of the serving endpoint
// which is the leader in a quorum-based cluster or the primary in a primary/secondary
// configured cluster.
func (c *pdServiceDiscovery) GetServingEndpointClientConn() *grpc.ClientConn {
	if cc, ok := c.clientConns.Load(c.getLeaderAddr()); ok {
		return cc.(*grpc.ClientConn)
	}
	return nil
}

// GetClientConns returns the mapping {addr -> a gRPC connection}
func (c *pdServiceDiscovery) GetClientConns() *sync.Map {
	return &c.clientConns
}

// GetServingAddr returns the leader address
func (c *pdServiceDiscovery) GetServingAddr() string {
	return c.getLeaderAddr()
}

// GetBackupAddrs gets the addresses of the current reachable followers
// in a quorum-based cluster. Used for tso currently.
func (c *pdServiceDiscovery) GetBackupAddrs() []string {
	return c.getFollowerAddrs()
}

// getLeaderServiceClient returns the leader ServiceClient.
func (c *pdServiceDiscovery) getLeaderServiceClient() *pdServiceClient {
	leader := c.leader.Load()
	if leader == nil {
		return nil
	}
	return leader.(*pdServiceClient)
}

// getServiceClientByKind returns ServiceClient of the specific kind.
func (c *pdServiceDiscovery) getServiceClientByKind(kind apiKind) ServiceClient {
	client := c.apiCandidateNodes[kind].get()
	if client == nil {
		return nil
	}
	return client
}

// GetServiceClient returns the leader/primary ServiceClient if it is healthy.
func (c *pdServiceDiscovery) GetServiceClient() ServiceClient {
	leaderClient := c.getLeaderServiceClient()
	if c.option.enableForwarding && !leaderClient.Available() {
		if followerClient := c.getServiceClientByKind(forwardAPIKind); followerClient != nil {
			log.Debug("[pd] use follower client", zap.String("addr", followerClient.GetAddress()))
			return followerClient
		}
	}
	if leaderClient == nil {
		return nil
	}
	return leaderClient
}

// ScheduleCheckMemberChanged is used to check if there is any membership
// change among the leader and the followers.
func (c *pdServiceDiscovery) ScheduleCheckMemberChanged() {
	select {
	case c.checkMembershipCh <- struct{}{}:
	default:
	}
}

// CheckMemberChanged Immediately check if there is any membership change among the leader/followers in a
// quorum-based cluster or among the primary/secondaries in a primary/secondary configured cluster.
func (c *pdServiceDiscovery) CheckMemberChanged() error {
	return c.updateMember()
}

// AddServingAddrSwitchedCallback adds callbacks which will be called
// when the leader is switched.
func (c *pdServiceDiscovery) AddServingAddrSwitchedCallback(callbacks ...func()) {
	c.leaderSwitchedCbs = append(c.leaderSwitchedCbs, callbacks...)
}

// AddServiceAddrsSwitchedCallback adds callbacks which will be called when
// any leader/follower is changed.
func (c *pdServiceDiscovery) AddServiceAddrsSwitchedCallback(callbacks ...func()) {
	c.membersChangedCbs = append(c.membersChangedCbs, callbacks...)
}

// SetTSOLocalServAddrsUpdatedCallback adds a callback which will be called when the local tso
// allocator leader list is updated.
func (c *pdServiceDiscovery) SetTSOLocalServAddrsUpdatedCallback(callback tsoLocalServAddrsUpdatedFunc) {
	c.tsoLocalAllocLeadersUpdatedCb = callback
}

// SetTSOGlobalServAddrUpdatedCallback adds a callback which will be called when the global tso
// allocator leader is updated.
func (c *pdServiceDiscovery) SetTSOGlobalServAddrUpdatedCallback(callback tsoGlobalServAddrUpdatedFunc) {
	addr := c.getLeaderAddr()
	if len(addr) > 0 {
		callback(addr)
	}
	c.tsoGlobalAllocLeaderUpdatedCb = callback
}

// getLeaderAddr returns the leader address.
func (c *pdServiceDiscovery) getLeaderAddr() string {
	return c.getLeaderServiceClient().GetAddress()
}

// getFollowerAddrs returns the follower address.
func (c *pdServiceDiscovery) getFollowerAddrs() []string {
	followerAddrs := c.followerAddresses.Load()
	if followerAddrs == nil {
		return []string{}
	}
	return followerAddrs.([]string)
}

func (c *pdServiceDiscovery) initClusterID() error {
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	clusterID := uint64(0)
	for _, url := range c.GetServiceURLs() {
		members, err := c.getMembers(ctx, url, c.option.timeout)
		if err != nil || members.GetHeader() == nil {
			log.Warn("[pd] failed to get cluster id", zap.String("url", url), errs.ZapError(err))
			continue
		}
		if clusterID == 0 {
			clusterID = members.GetHeader().GetClusterId()
			continue
		}
		failpoint.Inject("skipClusterIDCheck", func() {
			failpoint.Continue()
		})
		// All URLs passed in should have the same cluster ID.
		if members.GetHeader().GetClusterId() != clusterID {
			return errors.WithStack(errUnmatchedClusterID)
		}
	}
	// Failed to init the cluster ID.
	if clusterID == 0 {
		return errors.WithStack(errFailInitClusterID)
	}
	c.clusterID = clusterID
	return nil
}

func (c *pdServiceDiscovery) checkServiceModeChanged() error {
	leaderAddr := c.getLeaderAddr()
	if len(leaderAddr) == 0 {
		return errors.New("no leader found")
	}

	clusterInfo, err := c.getClusterInfo(c.ctx, leaderAddr, c.option.timeout)
	if err != nil {
		if strings.Contains(err.Error(), "Unimplemented") {
			// If the method is not supported, we set it to pd mode.
			// TODO: it's a hack way to solve the compatibility issue.
			// we need to remove this after all maintained version supports the method.
			c.serviceModeUpdateCb(pdpb.ServiceMode_PD_SVC_MODE)
			return nil
		}
		return err
	}
	if clusterInfo == nil || len(clusterInfo.ServiceModes) == 0 {
		return errors.WithStack(errNoServiceModeReturned)
	}
	c.serviceModeUpdateCb(clusterInfo.ServiceModes[0])
	return nil
}

func (c *pdServiceDiscovery) updateMember() error {
	for i, url := range c.GetServiceURLs() {
		failpoint.Inject("skipFirstUpdateMember", func() {
			if i == 0 {
				failpoint.Continue()
			}
		})

		members, err := c.getMembers(c.ctx, url, updateMemberTimeout)
		// Check the cluster ID.
		if err == nil && members.GetHeader().GetClusterId() != c.clusterID {
			err = errs.ErrClientUpdateMember.FastGenByArgs("cluster id does not match")
		}
		// Check the TSO Allocator Leader.
		var errTSO error
		if err == nil {
			if members.GetLeader() == nil || len(members.GetLeader().GetClientUrls()) == 0 {
				err = errs.ErrClientGetLeader.FastGenByArgs("leader address doesn't exist")
			}
			// Still need to update TsoAllocatorLeaders, even if there is no PD leader
			errTSO = c.switchTSOAllocatorLeaders(members.GetTsoAllocatorLeaders())
		}

		// Failed to get members
		if err != nil {
			log.Info("[pd] cannot update member from this address",
				zap.String("address", url),
				errs.ZapError(err))
			select {
			case <-c.ctx.Done():
				return errors.WithStack(err)
			default:
				continue
			}
		}

		c.updateURLs(members.GetMembers())
		if err := c.updateServiceClient(members.GetMembers(), members.GetLeader()); err != nil {
			return err
		}

		// If `switchLeader` succeeds but `switchTSOAllocatorLeader` has an error,
		// the error of `switchTSOAllocatorLeader` will be returned.
		return errTSO
	}
	return errs.ErrClientGetMember.FastGenByArgs()
}

func (c *pdServiceDiscovery) getClusterInfo(ctx context.Context, url string, timeout time.Duration) (*pdpb.GetClusterInfoResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cc, err := c.GetOrCreateGRPCConn(url)
	if err != nil {
		return nil, err
	}
	clusterInfo, err := pdpb.NewPDClient(cc).GetClusterInfo(ctx, &pdpb.GetClusterInfoRequest{})
	if err != nil {
		attachErr := errors.Errorf("error:%s target:%s status:%s", err, cc.Target(), cc.GetState().String())
		return nil, errs.ErrClientGetClusterInfo.Wrap(attachErr).GenWithStackByCause()
	}
	if clusterInfo.GetHeader().GetError() != nil {
		attachErr := errors.Errorf("error:%s target:%s status:%s", clusterInfo.GetHeader().GetError().String(), cc.Target(), cc.GetState().String())
		return nil, errs.ErrClientGetClusterInfo.Wrap(attachErr).GenWithStackByCause()
	}
	return clusterInfo, nil
}

func (c *pdServiceDiscovery) getMembers(ctx context.Context, url string, timeout time.Duration) (*pdpb.GetMembersResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cc, err := c.GetOrCreateGRPCConn(url)
	if err != nil {
		return nil, err
	}
	members, err := pdpb.NewPDClient(cc).GetMembers(ctx, &pdpb.GetMembersRequest{})
	if err != nil {
		attachErr := errors.Errorf("error:%s target:%s status:%s", err, cc.Target(), cc.GetState().String())
		return nil, errs.ErrClientGetMember.Wrap(attachErr).GenWithStackByCause()
	}
	if members.GetHeader().GetError() != nil {
		attachErr := errors.Errorf("error:%s target:%s status:%s", members.GetHeader().GetError().String(), cc.Target(), cc.GetState().String())
		return nil, errs.ErrClientGetMember.Wrap(attachErr).GenWithStackByCause()
	}
	return members, nil
}

func (c *pdServiceDiscovery) updateURLs(members []*pdpb.Member) {
	urls := make([]string, 0, len(members))
	for _, m := range members {
		urls = append(urls, m.GetClientUrls()...)
	}

	sort.Strings(urls)
	oldURLs := c.GetServiceURLs()
	// the url list is same.
	if reflect.DeepEqual(oldURLs, urls) {
		return
	}
	c.urls.Store(urls)
	// Update the connection contexts when member changes if TSO Follower Proxy is enabled.
	if c.option.getEnableTSOFollowerProxy() {
		// Run callbacks to reflect the membership changes in the leader and followers.
		for _, cb := range c.membersChangedCbs {
			cb()
		}
	}
	log.Info("[pd] update member urls", zap.Strings("old-urls", oldURLs), zap.Strings("new-urls", urls))
}

func (c *pdServiceDiscovery) switchLeader(addrs []string) (bool, error) {
	// FIXME: How to safely compare leader urls? For now, only allows one client url.
	addr := addrs[0]
	oldLeader := c.getLeaderServiceClient()
	if addr == oldLeader.GetAddress() && oldLeader.GetClientConn() != nil {
		return false, nil
	}

	newConn, err := c.GetOrCreateGRPCConn(addr)
	// If gRPC connect is created successfully or leader is new, still saves.
	if addr != oldLeader.GetAddress() || newConn != nil {
		// Set PD leader and Global TSO Allocator (which is also the PD leader)
		leaderClient := newPDServiceClient(addr, addr, newConn, true)
		c.leader.Store(leaderClient)
	}
	// Run callbacks
	if c.tsoGlobalAllocLeaderUpdatedCb != nil {
		if err := c.tsoGlobalAllocLeaderUpdatedCb(addr); err != nil {
			return true, err
		}
	}
	for _, cb := range c.leaderSwitchedCbs {
		cb()
	}
	log.Info("[pd] switch leader", zap.String("new-leader", addr), zap.String("old-leader", oldLeader.GetAddress()))
	return true, err
}

func (c *pdServiceDiscovery) updateFollowers(members []*pdpb.Member, leader *pdpb.Member) (changed bool) {
	followers := make(map[string]*pdServiceClient)
	c.followers.Range(func(key, value any) bool {
		followers[key.(string)] = value.(*pdServiceClient)
		return true
	})
	var followerAddrs []string
	for _, member := range members {
		if member.GetMemberId() != leader.GetMemberId() {
			if len(member.GetClientUrls()) > 0 {
				followerAddrs = append(followerAddrs, member.GetClientUrls()...)

				// FIXME: How to safely compare urls(also for leader)? For now, only allows one client url.
				addr := member.GetClientUrls()[0]
				if client, ok := c.followers.Load(addr); ok {
					if client.(*pdServiceClient).GetClientConn() == nil {
						conn, err := c.GetOrCreateGRPCConn(addr)
						if err != nil || conn == nil {
							log.Warn("[pd] failed to connect follower", zap.String("follower", addr), errs.ZapError(err))
							continue
						}
						follower := newPDServiceClient(addr, leader.GetClientUrls()[0], conn, false)
						c.followers.Store(addr, follower)
						changed = true
					}
					delete(followers, addr)
				} else {
					changed = true
					conn, err := c.GetOrCreateGRPCConn(addr)
					follower := newPDServiceClient(addr, leader.GetClientUrls()[0], conn, false)
					if err != nil || conn == nil {
						log.Warn("[pd] failed to connect follower", zap.String("follower", addr), errs.ZapError(err))
					}
					c.followers.LoadOrStore(addr, follower)
				}
			}
		}
	}
	if len(followers) > 0 {
		changed = true
		for key := range followers {
			c.followers.Delete(key)
		}
	}
	c.followerAddresses.Store(followerAddrs)
	return
}

func (c *pdServiceDiscovery) updateServiceClient(members []*pdpb.Member, leader *pdpb.Member) error {
	leaderChanged, err := c.switchLeader(leader.GetClientUrls())
	followerChanged := c.updateFollowers(members, leader)
	// don't need to recreate balancer if no changess.
	if !followerChanged && !leaderChanged {
		return err
	}
	// If error is not nil, still updates candidates.
	clients := make([]ServiceClient, 0)
	c.followers.Range(func(_, value any) bool {
		clients = append(clients, value.(*pdServiceClient))
		return true
	})
	leaderClient := c.getLeaderServiceClient()
	if leaderClient != nil {
		clients = append(clients, leaderClient)
	}
	// create candidate services for all kinds of request.
	for i := 0; i < int(apiKindCount); i++ {
		c.apiCandidateNodes[i].set(clients)
	}
	return err
}

func (c *pdServiceDiscovery) switchTSOAllocatorLeaders(allocatorMap map[string]*pdpb.Member) error {
	if len(allocatorMap) == 0 {
		return nil
	}

	allocMap := make(map[string]string)
	// Switch to the new one
	for dcLocation, member := range allocatorMap {
		if len(member.GetClientUrls()) == 0 {
			continue
		}
		allocMap[dcLocation] = member.GetClientUrls()[0]
	}

	// Run the callback to reflect any possible change in the local tso allocators.
	if c.tsoLocalAllocLeadersUpdatedCb != nil {
		if err := c.tsoLocalAllocLeadersUpdatedCb(allocMap); err != nil {
			return err
		}
	}

	return nil
}

// GetOrCreateGRPCConn returns the corresponding grpc client connection of the given addr
func (c *pdServiceDiscovery) GetOrCreateGRPCConn(addr string) (*grpc.ClientConn, error) {
	return grpcutil.GetOrCreateGRPCConn(c.ctx, &c.clientConns, addr, c.tlsCfg, c.option.gRPCDialOptions...)
}
