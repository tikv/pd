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

package servicediscovery

import (
	"context"
	"crypto/tls"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/retry"
	"github.com/tikv/pd/client/pkg/utils/grpcutil"
	"github.com/tikv/pd/client/pkg/utils/tlsutil"
)

const (
	// MemberUpdateInterval is the interval to update the member list.
	MemberUpdateInterval = time.Minute
	// UpdateMemberTimeout is the timeout to update the member list.
	// Use a shorter timeout to recover faster from network isolation.
	UpdateMemberTimeout = time.Second
	// UpdateMemberBackOffBaseTime is the base time to back off when updating the member list.
	UpdateMemberBackOffBaseTime = 100 * time.Millisecond

	serviceModeUpdateInterval = 3 * time.Second
)

// MemberHealthCheckInterval might be changed in the unit to shorten the testing time.
var MemberHealthCheckInterval = time.Second

// APIKind defines how this API should be handled.
type APIKind int

const (
	// ForwardAPIKind means this API should be forwarded from the followers to the leader.
	ForwardAPIKind APIKind = iota
	// UniversalAPIKind means this API can be handled by both the leader and the followers.
	UniversalAPIKind

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
	// SetKeyspaceID sets the ID of the keyspace
	SetKeyspaceID(id uint32)
	// GetKeyspaceGroupID returns the ID of the keyspace group
	GetKeyspaceGroupID() uint32
	// GetServiceURLs returns the URLs of the servers providing the service
	GetServiceURLs() []string
	// GetServingEndpointClientConn returns the grpc client connection of the serving endpoint
	// which is the leader in a quorum-based cluster or the primary in a primary/secondary
	// configured cluster.
	GetServingEndpointClientConn() *grpc.ClientConn
	// GetClientConns returns the mapping {URL -> a gRPC connection}
	GetClientConns() *sync.Map
	// GetServingURL returns the serving endpoint which is the leader in a quorum-based cluster
	// or the primary in a primary/secondary configured cluster.
	GetServingURL() string
	// GetBackupURLs gets the URLs of the current reachable backup service
	// endpoints. Backup service endpoints are followers in a quorum-based cluster or
	// secondaries in a primary/secondary configured cluster.
	GetBackupURLs() []string
	// GetServiceClient tries to get the leader/primary ServiceClient.
	// If the leader ServiceClient meets network problem,
	// it returns a follower/secondary ServiceClient which can forward the request to leader.
	GetServiceClient() ServiceClient
	// GetServiceClientByKind tries to get the ServiceClient with the given API kind.
	GetServiceClientByKind(kind APIKind) ServiceClient
	// GetAllServiceClients tries to get all ServiceClient.
	// If the leader is not nil, it will put the leader service client first in the slice.
	GetAllServiceClients() []ServiceClient
	// GetOrCreateGRPCConn returns the corresponding grpc client connection of the given url.
	GetOrCreateGRPCConn(url string) (*grpc.ClientConn, error)
	// ScheduleCheckMemberChanged is used to trigger a check to see if there is any membership change
	// among the leader/followers in a quorum-based cluster or among the primary/secondaries in a
	// primary/secondary configured cluster.
	ScheduleCheckMemberChanged()
	// CheckMemberChanged immediately check if there is any membership change among the leader/followers
	// in a quorum-based cluster or among the primary/secondaries in a primary/secondary configured cluster.
	CheckMemberChanged() error
	// AddServingURLSwitchedCallback adds callbacks which will be called when the leader
	// in a quorum-based cluster or the primary in a primary/secondary configured cluster
	// is switched.
	AddServingURLSwitchedCallback(callbacks ...func())
	// AddServiceURLsSwitchedCallback adds callbacks which will be called when any leader/follower
	// in a quorum-based cluster or any primary/secondary in a primary/secondary configured cluster
	// is changed.
	AddServiceURLsSwitchedCallback(callbacks ...func())
}

// ServiceClient is an interface that defines a set of operations for a raw PD gRPC client to specific PD server.
type ServiceClient interface {
	// GetURL returns the client url of the PD/etcd server.
	GetURL() string
	// GetClientConn returns the gRPC connection of the service client.
	// It returns nil if the connection is not available.
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
	url       string
	conn      *grpc.ClientConn
	isLeader  bool
	leaderURL string

	networkFailure atomic.Bool
}

// NOTE: In the current implementation, the URL passed in is bound to have a scheme,
// because it is processed in `newPDServiceDiscovery`, and the url returned by etcd member owns the scheme.
// When testing, the URL is also bound to have a scheme.
func newPDServiceClient(url, leaderURL string, conn *grpc.ClientConn, isLeader bool) ServiceClient {
	cli := &pdServiceClient{
		url:       url,
		conn:      conn,
		isLeader:  isLeader,
		leaderURL: leaderURL,
	}
	if conn == nil {
		cli.networkFailure.Store(true)
	}
	return cli
}

// GetURL implements ServiceClient.
func (c *pdServiceClient) GetURL() string {
	if c == nil {
		return ""
	}
	return c.url
}

// BuildGRPCTargetContext implements ServiceClient.
func (c *pdServiceClient) BuildGRPCTargetContext(ctx context.Context, toLeader bool) context.Context {
	if c == nil || c.isLeader {
		return ctx
	}
	if toLeader {
		return grpcutil.BuildForwardContext(ctx, c.leaderURL)
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
		if val, ok := val.(string); (ok && val == c.GetURL()) || !ok {
			resp = nil
			err = status.New(codes.Unavailable, "unavailable").Err()
		}
	})
	rpcErr, ok := status.FromError(err)
	if (ok && errs.IsNetworkError(rpcErr.Code())) || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		c.networkFailure.Store(true)
	} else {
		c.networkFailure.Store(false)
	}
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

type errFn func(*pdpb.Error) bool

func emptyErrorFn(*pdpb.Error) bool {
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
		pdServiceAPIClient: newPDServiceAPIClient(clients[c.totalNode-1], c.errFn).(*pdServiceAPIClient),
	}
	head.next = head
	last := head
	for i := c.totalNode - 2; i >= 0; i-- {
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
	for range c.totalNode {
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

// UpdateKeyspaceIDFunc is the function type for updating the keyspace ID.
type UpdateKeyspaceIDFunc func() error
type tsoLeaderURLUpdatedFunc func(string) error

// TSOEventSource subscribes to events related to changes in the TSO leader/primary from the service discovery.
type TSOEventSource interface {
	// SetTSOLeaderURLUpdatedCallback adds a callback which will be called when the TSO leader/primary is updated.
	SetTSOLeaderURLUpdatedCallback(callback tsoLeaderURLUpdatedFunc)
}

var (
	_ ServiceDiscovery = (*pdServiceDiscovery)(nil)
	_ TSOEventSource   = (*pdServiceDiscovery)(nil)
)

// pdServiceDiscovery is the service discovery client of PD/API service which is quorum based
type pdServiceDiscovery struct {
	isInitialized bool

	urls atomic.Value // Store as []string
	// PD leader
	leader atomic.Value // Store as pdServiceClient
	// PD follower
	followers sync.Map // Store as map[string]pdServiceClient
	// PD leader and PD followers
	all               atomic.Value // Store as []pdServiceClient
	apiCandidateNodes [apiKindCount]*pdServiceBalancer
	// PD follower URLs. Only for tso.
	followerURLs atomic.Value // Store as []string

	clusterID uint64
	// url -> a gRPC connection
	clientConns sync.Map // Store as map[string]*grpc.ClientConn

	// serviceModeUpdateCb will be called when the service mode gets updated
	serviceModeUpdateCb func(pdpb.ServiceMode)
	// leaderSwitchedCbs will be called after the leader switched
	leaderSwitchedCbs []func()
	// membersChangedCbs will be called after there is any membership change in the
	// leader and followers
	membersChangedCbs []func()
	// tsoLeaderUpdatedCb will be called when the TSO leader is updated.
	tsoLeaderUpdatedCb tsoLeaderURLUpdatedFunc

	checkMembershipCh chan struct{}

	wg        *sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once

	updateKeyspaceIDFunc UpdateKeyspaceIDFunc
	keyspaceID           uint32
	tlsCfg               *tls.Config
	// Client option.
	option *opt.Option
}

// NewDefaultPDServiceDiscovery returns a new default PD service discovery-based client.
func NewDefaultPDServiceDiscovery(
	ctx context.Context, cancel context.CancelFunc,
	urls []string, tlsCfg *tls.Config,
) ServiceDiscovery {
	var wg sync.WaitGroup
	return NewPDServiceDiscovery(ctx, cancel, &wg, nil, nil, constants.DefaultKeyspaceID, urls, tlsCfg, opt.NewOption())
}

// NewPDServiceDiscovery returns a new PD service discovery-based client.
func NewPDServiceDiscovery(
	ctx context.Context, cancel context.CancelFunc,
	wg *sync.WaitGroup,
	serviceModeUpdateCb func(pdpb.ServiceMode),
	updateKeyspaceIDFunc UpdateKeyspaceIDFunc,
	keyspaceID uint32,
	urls []string, tlsCfg *tls.Config, option *opt.Option,
) ServiceDiscovery {
	pdsd := &pdServiceDiscovery{
		checkMembershipCh:    make(chan struct{}, 1),
		ctx:                  ctx,
		cancel:               cancel,
		wg:                   wg,
		apiCandidateNodes:    [apiKindCount]*pdServiceBalancer{newPDServiceBalancer(emptyErrorFn), newPDServiceBalancer(regionAPIErrorFn)},
		serviceModeUpdateCb:  serviceModeUpdateCb,
		updateKeyspaceIDFunc: updateKeyspaceIDFunc,
		keyspaceID:           keyspaceID,
		tlsCfg:               tlsCfg,
		option:               option,
	}
	urls = tlsutil.AddrsToURLs(urls, tlsCfg)
	pdsd.urls.Store(urls)
	return pdsd
}

// Init initializes the PD service discovery.
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
	if c.keyspaceID == constants.NullKeyspaceID && c.updateKeyspaceIDFunc != nil {
		if err := c.initRetry(c.updateKeyspaceIDFunc); err != nil {
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
	for range c.option.MaxRetryTimes {
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
	ticker := time.NewTicker(MemberUpdateInterval)
	defer ticker.Stop()

	bo := retry.InitialBackoffer(UpdateMemberBackOffBaseTime, UpdateMemberTimeout, UpdateMemberBackOffBaseTime)
	for {
		select {
		case <-ctx.Done():
			log.Info("[pd] exit member loop due to context canceled")
			return
		case <-ticker.C:
		case <-c.checkMembershipCh:
		}
		err := bo.Exec(ctx, c.updateMember)
		if err != nil {
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
	failpoint.Inject("fastUpdateServiceMode", func() {
		ticker.Reset(10 * time.Millisecond)
	})
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
	ctx, cancel := context.WithTimeout(ctx, c.option.Timeout)
	defer cancel()
	leader := c.getLeaderServiceClient()
	leader.checkNetworkAvailable(ctx)
}

func (c *pdServiceDiscovery) checkFollowerHealth(ctx context.Context) {
	c.followers.Range(func(_, value any) bool {
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
	if c == nil {
		return
	}
	c.closeOnce.Do(func() {
		log.Info("[pd] close pd service discovery client")
		c.clientConns.Range(func(key, cc any) bool {
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
func (*pdServiceDiscovery) GetKeyspaceGroupID() uint32 {
	// PD/API service only supports the default keyspace group
	return constants.DefaultKeyspaceGroupID
}

// DiscoverMicroservice discovers the microservice with the specified type and returns the server urls.
func (c *pdServiceDiscovery) discoverMicroservice(svcType serviceType) (urls []string, err error) {
	switch svcType {
	case apiService:
		urls = c.GetServiceURLs()
	case tsoService:
		leaderURL := c.getLeaderURL()
		if len(leaderURL) > 0 {
			clusterInfo, err := c.getClusterInfo(c.ctx, leaderURL, c.option.Timeout)
			if err != nil {
				log.Error("[pd] failed to get cluster info",
					zap.String("leader-url", leaderURL), errs.ZapError(err))
				return nil, err
			}
			urls = clusterInfo.TsoUrls
		} else {
			err = errors.New("failed to get leader url")
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
	if cc, ok := c.clientConns.Load(c.getLeaderURL()); ok {
		return cc.(*grpc.ClientConn)
	}
	return nil
}

// GetClientConns returns the mapping {URL -> a gRPC connection}
func (c *pdServiceDiscovery) GetClientConns() *sync.Map {
	return &c.clientConns
}

// GetServingURL returns the leader url
func (c *pdServiceDiscovery) GetServingURL() string {
	return c.getLeaderURL()
}

// GetBackupURLs gets the URLs of the current reachable followers
// in a quorum-based cluster. Used for tso currently.
func (c *pdServiceDiscovery) GetBackupURLs() []string {
	return c.getFollowerURLs()
}

// getLeaderServiceClient returns the leader ServiceClient.
func (c *pdServiceDiscovery) getLeaderServiceClient() *pdServiceClient {
	leader := c.leader.Load()
	if leader == nil {
		return nil
	}
	return leader.(*pdServiceClient)
}

// GetServiceClientByKind returns ServiceClient of the specific kind.
func (c *pdServiceDiscovery) GetServiceClientByKind(kind APIKind) ServiceClient {
	client := c.apiCandidateNodes[kind].get()
	if client == nil {
		return nil
	}
	return client
}

// GetServiceClient returns the leader/primary ServiceClient if it is healthy.
func (c *pdServiceDiscovery) GetServiceClient() ServiceClient {
	leaderClient := c.getLeaderServiceClient()
	if c.option.EnableForwarding && !leaderClient.Available() {
		if followerClient := c.GetServiceClientByKind(ForwardAPIKind); followerClient != nil {
			log.Debug("[pd] use follower client", zap.String("url", followerClient.GetURL()))
			return followerClient
		}
	}
	if leaderClient == nil {
		return nil
	}
	return leaderClient
}

// GetAllServiceClients implements ServiceDiscovery
func (c *pdServiceDiscovery) GetAllServiceClients() []ServiceClient {
	all := c.all.Load()
	if all == nil {
		return nil
	}
	ret := all.([]ServiceClient)
	return append(ret[:0:0], ret...)
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

// AddServingURLSwitchedCallback adds callbacks which will be called
// when the leader is switched.
func (c *pdServiceDiscovery) AddServingURLSwitchedCallback(callbacks ...func()) {
	c.leaderSwitchedCbs = append(c.leaderSwitchedCbs, callbacks...)
}

// AddServiceURLsSwitchedCallback adds callbacks which will be called when
// any leader/follower is changed.
func (c *pdServiceDiscovery) AddServiceURLsSwitchedCallback(callbacks ...func()) {
	c.membersChangedCbs = append(c.membersChangedCbs, callbacks...)
}

// SetTSOLeaderURLUpdatedCallback adds a callback which will be called when the TSO leader is updated.
func (c *pdServiceDiscovery) SetTSOLeaderURLUpdatedCallback(callback tsoLeaderURLUpdatedFunc) {
	url := c.getLeaderURL()
	if len(url) > 0 {
		if err := callback(url); err != nil {
			log.Error("[tso] failed to call back when tso leader url update", zap.String("url", url), errs.ZapError(err))
		}
	}
	c.tsoLeaderUpdatedCb = callback
}

// getLeaderURL returns the leader URL.
func (c *pdServiceDiscovery) getLeaderURL() string {
	return c.getLeaderServiceClient().GetURL()
}

// getFollowerURLs returns the follower URLs.
func (c *pdServiceDiscovery) getFollowerURLs() []string {
	followerURLs := c.followerURLs.Load()
	if followerURLs == nil {
		return []string{}
	}
	return followerURLs.([]string)
}

func (c *pdServiceDiscovery) initClusterID() error {
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	clusterID := uint64(0)
	for _, url := range c.GetServiceURLs() {
		members, err := c.getMembers(ctx, url, c.option.Timeout)
		if err != nil || members.GetHeader() == nil {
			log.Warn("[pd] failed to get cluster id", zap.String("url", url), errs.ZapError(err))
			continue
		}
		if clusterID == 0 {
			clusterID = members.GetHeader().GetClusterId()
			continue
		}
		// All URLs passed in should have the same cluster ID.
		if members.GetHeader().GetClusterId() != clusterID {
			return errors.WithStack(errs.ErrUnmatchedClusterID)
		}
	}
	// Failed to init the cluster ID.
	if clusterID == 0 {
		return errors.WithStack(errs.ErrFailInitClusterID)
	}
	c.clusterID = clusterID
	return nil
}

func (c *pdServiceDiscovery) checkServiceModeChanged() error {
	leaderURL := c.getLeaderURL()
	if len(leaderURL) == 0 {
		return errors.New("no leader found")
	}

	clusterInfo, err := c.getClusterInfo(c.ctx, leaderURL, c.option.Timeout)
	if err != nil {
		if strings.Contains(err.Error(), "Unimplemented") {
			// If the method is not supported, we set it to pd mode.
			// TODO: it's a hack way to solve the compatibility issue.
			// we need to remove this after all maintained version supports the method.
			if c.serviceModeUpdateCb != nil {
				c.serviceModeUpdateCb(pdpb.ServiceMode_PD_SVC_MODE)
			}
			return nil
		}
		return err
	}
	if clusterInfo == nil || len(clusterInfo.ServiceModes) == 0 {
		return errors.WithStack(errs.ErrNoServiceModeReturned)
	}
	if c.serviceModeUpdateCb != nil {
		c.serviceModeUpdateCb(clusterInfo.ServiceModes[0])
	}
	return nil
}

func (c *pdServiceDiscovery) updateMember() error {
	for _, url := range c.GetServiceURLs() {
		members, err := c.getMembers(c.ctx, url, UpdateMemberTimeout)
		// Check the cluster ID.
		updatedClusterID := members.GetHeader().GetClusterId()
		if err == nil && updatedClusterID != c.clusterID {
			log.Warn("[pd] cluster id does not match",
				zap.Uint64("updated-cluster-id", updatedClusterID),
				zap.Uint64("expected-cluster-id", c.clusterID))
			err = errs.ErrClientUpdateMember.FastGenByArgs(fmt.Sprintf("cluster id does not match: %d != %d", updatedClusterID, c.clusterID))
		}
		if err == nil && (members.GetLeader() == nil || len(members.GetLeader().GetClientUrls()) == 0) {
			err = errs.ErrClientGetLeader.FastGenByArgs("leader url doesn't exist")
		}
		// Failed to get members
		if err != nil {
			log.Info("[pd] cannot update member from this url",
				zap.String("url", url),
				errs.ZapError(err))
			select {
			case <-c.ctx.Done():
				return errors.WithStack(err)
			default:
				continue
			}
		}
		c.updateURLs(members.GetMembers())

		return c.updateServiceClient(members.GetMembers(), members.GetLeader())
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
	// Run callbacks to reflect the membership changes in the leader and followers.
	for _, cb := range c.membersChangedCbs {
		cb()
	}
	log.Info("[pd] update member urls", zap.Strings("old-urls", oldURLs), zap.Strings("new-urls", urls))
}

func (c *pdServiceDiscovery) switchLeader(url string) (bool, error) {
	oldLeader := c.getLeaderServiceClient()
	if url == oldLeader.GetURL() && oldLeader.GetClientConn() != nil {
		return false, nil
	}

	newConn, err := c.GetOrCreateGRPCConn(url)
	// If gRPC connect is created successfully or leader is new, still saves.
	if url != oldLeader.GetURL() || newConn != nil {
		leaderClient := newPDServiceClient(url, url, newConn, true)
		c.leader.Store(leaderClient)
	}
	// Run callbacks
	if c.tsoLeaderUpdatedCb != nil {
		if err := c.tsoLeaderUpdatedCb(url); err != nil {
			return true, err
		}
	}
	for _, cb := range c.leaderSwitchedCbs {
		cb()
	}
	log.Info("[pd] switch leader", zap.String("new-leader", url), zap.String("old-leader", oldLeader.GetURL()))
	return true, err
}

func (c *pdServiceDiscovery) updateFollowers(members []*pdpb.Member, leaderID uint64, leaderURL string) (changed bool) {
	followers := make(map[string]*pdServiceClient)
	c.followers.Range(func(key, value any) bool {
		followers[key.(string)] = value.(*pdServiceClient)
		return true
	})
	var followerURLs []string
	for _, member := range members {
		if member.GetMemberId() != leaderID {
			if len(member.GetClientUrls()) > 0 {
				// Now we don't apply ServiceClient for TSO Follower Proxy, so just keep the all URLs.
				followerURLs = append(followerURLs, member.GetClientUrls()...)

				// FIXME: How to safely compare urls(also for leader)? For now, only allows one client url.
				url := tlsutil.PickMatchedURL(member.GetClientUrls(), c.tlsCfg)
				if client, ok := c.followers.Load(url); ok {
					if client.(*pdServiceClient).GetClientConn() == nil {
						conn, err := c.GetOrCreateGRPCConn(url)
						if err != nil || conn == nil {
							log.Warn("[pd] failed to connect follower", zap.String("follower", url), errs.ZapError(err))
							continue
						}
						follower := newPDServiceClient(url, leaderURL, conn, false)
						c.followers.Store(url, follower)
						changed = true
					}
					delete(followers, url)
				} else {
					changed = true
					conn, err := c.GetOrCreateGRPCConn(url)
					follower := newPDServiceClient(url, leaderURL, conn, false)
					if err != nil || conn == nil {
						log.Warn("[pd] failed to connect follower", zap.String("follower", url), errs.ZapError(err))
					}
					c.followers.LoadOrStore(url, follower)
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
	c.followerURLs.Store(followerURLs)
	return
}

func (c *pdServiceDiscovery) updateServiceClient(members []*pdpb.Member, leader *pdpb.Member) error {
	// FIXME: How to safely compare leader urls? For now, only allows one client url.
	leaderURL := tlsutil.PickMatchedURL(leader.GetClientUrls(), c.tlsCfg)
	leaderChanged, err := c.switchLeader(leaderURL)
	followerChanged := c.updateFollowers(members, leader.GetMemberId(), leaderURL)
	// don't need to recreate balancer if no changes.
	if !followerChanged && !leaderChanged {
		return err
	}
	// If error is not nil, still updates candidates.
	clients := make([]ServiceClient, 0)
	leaderClient := c.getLeaderServiceClient()
	if leaderClient != nil {
		clients = append(clients, leaderClient)
	}
	c.followers.Range(func(_, value any) bool {
		clients = append(clients, value.(*pdServiceClient))
		return true
	})
	c.all.Store(clients)
	// create candidate services for all kinds of request.
	for i := range apiKindCount {
		c.apiCandidateNodes[i].set(clients)
	}
	return err
}

// GetOrCreateGRPCConn returns the corresponding grpc client connection of the given URL.
func (c *pdServiceDiscovery) GetOrCreateGRPCConn(url string) (*grpc.ClientConn, error) {
	return grpcutil.GetOrCreateGRPCConn(c.ctx, &c.clientConns, url, c.tlsCfg, c.option.GRPCDialOptions...)
}
