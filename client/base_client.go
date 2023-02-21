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
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/grpcutil"
	"github.com/tikv/pd/client/tlsutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	globalDCLocation     = "global"
	memberUpdateInterval = time.Minute
)

type tsoRequest struct {
	start      time.Time
	clientCtx  context.Context
	requestCtx context.Context
	done       chan error
	physical   int64
	logical    int64
	dcLocation string
	keyspaceID uint32
}

// BaseClient defines the general interface for service discovery on a quorum-based cluster
// or a primary/secondy configured cluster.
type BaseClient interface {
	// Init initialize the concrete client underlying
	Init() error
	// Close all grpc client connnections
	CloseClientConns()
	// GetClusterID returns the ID of the cluster
	GetClusterID(context.Context) uint64
	// GetTSOAllocators returns {dc-location -> TSO allocator leader URL} connection map
	GetTSOAllocators() *sync.Map
	// GetTSOAllocatorLeaderAddrByDCLocation returns the tso allocator of the given dcLocation
	GetTSOAllocatorLeaderAddrByDCLocation(dcLocation string) (string, bool)
	// GetTSOAllocatorClientConnByDCLocation returns the tso allocator grpc client connection
	// of the given dcLocation
	GetTSOAllocatorClientConnByDCLocation(dcLocation string) (*grpc.ClientConn, string)
	// GetServingEndpointAddr returns the grpc client connection of the serving endpoint
	// which is the leader in a quorum-based cluster or the primary in a primary/secondy
	// configured cluster.
	GetServingEndpointClientConn() *grpc.ClientConn
	// GetServingEndpointAddr returns the serving endpoint which is the leader
	// in a quorum-based cluster or the primary in a primary/secondy configured cluster.
	GetServingEndpointAddr() string
	// GetBackupEndpointsAddrs gets the addresses of the current reachable and healthy
	// backup service endpoints randomly. Backup service endpoints are followers in a
	// quorum-based cluster or secondaries in a primary/secondary configured cluster.
	GetBackupEndpointsAddrs() []string
	// GetOrCreateGRPCConn returns the corresponding grpc client connection of the given addr
	GetOrCreateGRPCConn(addr string) (*grpc.ClientConn, error)
	// ScheduleCheckIfMembershipChanged is used to trigger a check to see if there is any
	// membership change among the leader/followers in a quorum-based cluster or among
	// the primary/secondaries in a primary/secondy configured cluster.
	ScheduleCheckIfMembershipChanged()
	// Immediately checkif there is any membership change among the leader/followers in a
	// quorum-based cluster or among the primary/secondaries in a primary/secondy configured cluster.
	CheckIfMembershipChanged() error
	// AddServiceEndpointSwitchedCallback adds callbacks which will be called when the leader
	// in a quorum-based cluster or the primary in a primary/secondary configured cluster
	// is switched.
	AddServiceEndpointSwitchedCallback(callbacks ...func())
	// AddServiceEndpointsChangedCallback adds callbacks which will be called when any leader/follower
	// in a quorum-based cluster or the primary in a primary/secondary configured cluster is changed.
	AddServiceEndpointsChangedCallback(callbacks ...func())
	// CreateTsoStream creates a TSO stream to send/recv timestamps
	CreateTsoStream(ctx context.Context, cancel context.CancelFunc, cc *grpc.ClientConn) (interface{}, error)
	// TryConnectToTSOWithProxy will create multiple streams to all the service endpoints to work as
	// a TSO proxy to reduce the pressure of the main serving service endpoint.
	TryConnectToTSOWithProxy(dispatcherCtx context.Context, dc string, connectionCtxs *sync.Map) error
	// ProcessTSORequests processes TSO requests in streaming mode to get timestamps
	ProcessTSORequests(stream interface{}, dcLocation string, requests []*tsoRequest,
		batchStartTime time.Time) (physical, logical int64, suffixBits uint32, err error)

	// GetURLs returns the URLs of the servers.
	// For testing use. It should only be called when the client is closed.
	GetURLs() []string
	// GetTSOAllocatorLeaderURLs returns the urls of the tso allocator leaders
	// For testing use.
	GetTSOAllocatorLeaderURLs() map[string]string
}

var _ BaseClient = (*pdBaseClient)(nil)

// pdBaseClient is the service discovery client of PD/API service which is quorum based
type pdBaseClient struct {
	urls atomic.Value // Store as []string
	// PD leader URL
	leader atomic.Value // Store as string
	// PD follower URLs
	followers atomic.Value // Store as []string

	clusterID uint64
	// addr -> a gRPC connection
	clientConns sync.Map // Store as map[string]*grpc.ClientConn
	// dc-location -> TSO allocator leader URL
	tsoAllocators sync.Map // Store as map[string]string

	// leaderSwitchedCallbacks will be called after the leader swichted
	leaderSwitchedCallbacks []func()
	// leaderSwitchedCallbacks will be called after there is any membership
	// change in the leader and followers
	membersChangedCallbacks []func()

	checkMembershipCh chan struct{}

	wg     *sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	security SecurityOption

	// Client option.
	option *option
}

// SecurityOption records options about tls
type SecurityOption struct {
	CAPath   string
	CertPath string
	KeyPath  string

	SSLCABytes   []byte
	SSLCertBytes []byte
	SSLKEYBytes  []byte
}

// newBaseClient returns a new baseClient.
func newBaseClient(ctx context.Context, cancel context.CancelFunc,
	wg *sync.WaitGroup, urls []string, security SecurityOption, option *option) BaseClient {
	bc := &pdBaseClient{
		checkMembershipCh: make(chan struct{}, 1),
		ctx:               ctx,
		cancel:            cancel,
		wg:                wg,
		security:          security,
		option:            option,
	}
	bc.urls.Store(urls)
	return bc
}

func (c *pdBaseClient) Init() error {
	if err := c.initRetry(c.initClusterID); err != nil {
		c.cancel()
		return err
	}
	if err := c.initRetry(c.updateMember); err != nil {
		c.cancel()
		return err
	}
	log.Info("[pd] init cluster id", zap.Uint64("cluster-id", c.clusterID))

	c.wg.Add(1)
	go c.memberLoop()
	return nil
}

func (c *pdBaseClient) initRetry(f func() error) error {
	var err error
	for i := 0; i < c.option.maxRetryTimes; i++ {
		if err = f(); err == nil {
			return nil
		}
		select {
		case <-c.ctx.Done():
			return err
		case <-time.After(time.Second):
		}
	}
	return errors.WithStack(err)
}

func (c *pdBaseClient) memberLoop() {
	defer c.wg.Done()

	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()

	for {
		select {
		case <-c.checkMembershipCh:
		case <-time.After(memberUpdateInterval):
		case <-ctx.Done():
			return
		}
		failpoint.Inject("skipUpdateMember", func() {
			failpoint.Continue()
		})
		if err := c.updateMember(); err != nil {
			log.Error("[pd] failed to update member", errs.ZapError(err))
		}
	}
}

// Close all grpc client connnections
func (c *pdBaseClient) CloseClientConns() {
	c.clientConns.Range(func(_, cc interface{}) bool {
		if err := cc.(*grpc.ClientConn).Close(); err != nil {
			log.Error("[pd] failed to close gRPC clientConn", errs.ZapError(errs.ErrCloseGRPCConn, err))
		}
		return true
	})
}

// getClusterID returns the ClusterID.
func (c *pdBaseClient) GetClusterID(context.Context) uint64 {
	return c.clusterID
}

// GetTSOAllocators returns {dc-location -> TSO allocator leader URL} connection map
func (c *pdBaseClient) GetTSOAllocators() *sync.Map {
	return &c.tsoAllocators
}

// GetServingEndpointAddr returns the grpc client connection of the serving endpoint
// which is the leader in a quorum-based cluster or the primary in a primary/secondy
// configured cluster.
func (c *pdBaseClient) GetServingEndpointClientConn() *grpc.ClientConn {
	if cc, ok := c.clientConns.Load(c.getLeaderAddr()); ok {
		return cc.(*grpc.ClientConn)
	}
	return nil
}

// GetServingEndpointAddr returns the leader address
func (c *pdBaseClient) GetServingEndpointAddr() string {
	return c.getLeaderAddr()
}

// GetBackupEndpointsAddrs gets the addresses of the current reachable and healthy followers
// in a quorum-based cluster.
func (c *pdBaseClient) GetBackupEndpointsAddrs() []string {
	return c.getFollowerAddrs()
}

// ScheduleCheckIfMembershipChanged is used to check if there is any membership
// change among the leader and the followers.
func (c *pdBaseClient) ScheduleCheckIfMembershipChanged() {
	select {
	case c.checkMembershipCh <- struct{}{}:
	default:
	}
}

// Immediately check if there is any membership change among the leader/followers in a
// quorum-based cluster or among the primary/secondaries in a primary/secondy configured cluster.
func (c *pdBaseClient) CheckIfMembershipChanged() error {
	return c.updateMember()
}

// AddServiceEndpointSwitchedCallback adds callbacks which will be called
// when the leader is switched.
func (c *pdBaseClient) AddServiceEndpointSwitchedCallback(callbacks ...func()) {
	c.leaderSwitchedCallbacks = append(c.leaderSwitchedCallbacks, callbacks...)
}

// AddServiceEndpointsChangedCallback adds callbacks which will be called when
// any leader/follower is changed.
func (c *pdBaseClient) AddServiceEndpointsChangedCallback(callbacks ...func()) {
	c.membersChangedCallbacks = append(c.membersChangedCallbacks, callbacks...)
}

// GetLeaderAddr returns the leader address.
func (c *pdBaseClient) getLeaderAddr() string {
	leaderAddr := c.leader.Load()
	if leaderAddr == nil {
		return ""
	}
	return leaderAddr.(string)
}

// GetFollowerAddrs returns the follower address.
func (c *pdBaseClient) getFollowerAddrs() []string {
	followerAddrs := c.followers.Load()
	if followerAddrs == nil {
		return []string{}
	}
	return followerAddrs.([]string)
}

// GetURLs returns the URLs of the servers.
// For testing use. It should only be called when the client is closed.
func (c *pdBaseClient) GetURLs() []string {
	return c.urls.Load().([]string)
}

// GetTSOAllocatorLeaderURLs returns the urls of the tso allocator leaders
// For testing use.
func (c *pdBaseClient) GetTSOAllocatorLeaderURLs() map[string]string {
	allocatorLeaders := make(map[string]string)
	c.tsoAllocators.Range(func(dcLocation, url interface{}) bool {
		allocatorLeaders[dcLocation.(string)] = url.(string)
		return true
	})
	return allocatorLeaders
}

// GetTSOAllocatorLeaderAddrByDCLocation returns the tso allocator of the given dcLocation
func (c *pdBaseClient) GetTSOAllocatorLeaderAddrByDCLocation(dcLocation string) (string, bool) {
	url, exist := c.tsoAllocators.Load(dcLocation)
	if !exist {
		return "", false
	}
	return url.(string), true
}

// GetTSOAllocatorClientConnByDCLocation returns the tso allocator grpc client connection of the given dcLocation
func (c *pdBaseClient) GetTSOAllocatorClientConnByDCLocation(dcLocation string) (*grpc.ClientConn, string) {
	url, ok := c.tsoAllocators.Load(dcLocation)
	if !ok {
		panic(fmt.Sprintf("the allocator leader in %s should exist", dcLocation))
	}
	cc, ok := c.clientConns.Load(url)
	if !ok {
		panic(fmt.Sprintf("the client connection of %s in %s should exist", url, dcLocation))
	}
	return cc.(*grpc.ClientConn), url.(string)
}

func (c *pdBaseClient) gcAllocatorLeaderAddr(curAllocatorMap map[string]*pdpb.Member) {
	// Clean up the old TSO allocators
	c.tsoAllocators.Range(func(dcLocationKey, _ interface{}) bool {
		dcLocation := dcLocationKey.(string)
		// Skip the Global TSO Allocator
		if dcLocation == globalDCLocation {
			return true
		}
		if _, exist := curAllocatorMap[dcLocation]; !exist {
			log.Info("[pd] delete unused tso allocator", zap.String("dc-location", dcLocation))
			c.tsoAllocators.Delete(dcLocation)
		}
		return true
	})
}

func (c *pdBaseClient) initClusterID() error {
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	var clusterID uint64
	for _, u := range c.GetURLs() {
		members, err := c.getMembers(ctx, u, c.option.timeout)
		if err != nil || members.GetHeader() == nil {
			log.Warn("[pd] failed to get cluster id", zap.String("url", u), errs.ZapError(err))
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

func (c *pdBaseClient) updateMember() error {
	for i, u := range c.GetURLs() {
		failpoint.Inject("skipFirstUpdateMember", func() {
			if i == 0 {
				failpoint.Continue()
			}
		})
		members, err := c.getMembers(c.ctx, u, updateMemberTimeout)
		// Check the cluster ID.
		if err == nil && members.GetHeader().GetClusterId() != c.clusterID {
			err = errs.ErrClientUpdateMember.FastGenByArgs("cluster id does not match")
		}
		// Check the TSO Allocator Leader.
		var errTSO error
		if err == nil {
			if members.GetLeader() == nil || len(members.GetLeader().GetClientUrls()) == 0 {
				err = errs.ErrClientGetLeader.FastGenByArgs("leader address don't exist")
			}
			// Still need to update TsoAllocatorLeaders, even if there is no PD leader
			errTSO = c.switchTSOAllocatorLeader(members.GetTsoAllocatorLeaders())
		}

		// Failed to get members
		if err != nil {
			log.Info("[pd] cannot update member from this address",
				zap.String("address", u),
				errs.ZapError(err))
			select {
			case <-c.ctx.Done():
				return errors.WithStack(err)
			default:
				continue
			}
		}

		c.updateURLs(members.GetMembers())
		c.updateFollowers(members.GetMembers(), members.GetLeader())
		if err := c.switchLeader(members.GetLeader().GetClientUrls()); err != nil {
			return err
		}

		// If `switchLeader` succeeds but `switchTSOAllocatorLeader` has an error,
		// the error of `switchTSOAllocatorLeader` will be returned.
		return errTSO
	}
	return errs.ErrClientGetMember.FastGenByArgs(c.GetURLs())
}

func (c *pdBaseClient) getMembers(ctx context.Context, url string, timeout time.Duration) (*pdpb.GetMembersResponse, error) {
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

func (c *pdBaseClient) updateURLs(members []*pdpb.Member) {
	urls := make([]string, 0, len(members))
	for _, m := range members {
		urls = append(urls, m.GetClientUrls()...)
	}

	sort.Strings(urls)
	oldURLs := c.GetURLs()
	// the url list is same.
	if reflect.DeepEqual(oldURLs, urls) {
		return
	}
	c.urls.Store(urls)
	// Update the connection contexts when member changes if TSO Follower Proxy is enabled.
	if c.option.getEnableTSOFollowerProxy() {
		// Run callbacks to refelect the membership changes in the leader and followers.
		for _, cb := range c.membersChangedCallbacks {
			cb()
		}
	}
	log.Info("[pd] update member urls", zap.Strings("old-urls", oldURLs), zap.Strings("new-urls", urls))
}

func (c *pdBaseClient) switchLeader(addrs []string) error {
	// FIXME: How to safely compare leader urls? For now, only allows one client url.
	addr := addrs[0]
	oldLeader := c.getLeaderAddr()
	if addr == oldLeader {
		return nil
	}

	if _, err := c.GetOrCreateGRPCConn(addr); err != nil {
		log.Warn("[pd] failed to connect leader", zap.String("leader", addr), errs.ZapError(err))
		return err
	}
	// Set PD leader and Global TSO Allocator (which is also the PD leader)
	c.leader.Store(addr)
	c.tsoAllocators.Store(globalDCLocation, addr)
	// Run callbacks
	for _, cb := range c.leaderSwitchedCallbacks {
		cb()
	}
	log.Info("[pd] switch leader", zap.String("new-leader", addr), zap.String("old-leader", oldLeader))
	return nil
}

func (c *pdBaseClient) updateFollowers(members []*pdpb.Member, leader *pdpb.Member) {
	var addrs []string
	for _, member := range members {
		if member.GetMemberId() != leader.GetMemberId() {
			if len(member.GetClientUrls()) > 0 {
				addrs = append(addrs, member.GetClientUrls()...)
			}
		}
	}
	c.followers.Store(addrs)
}

func (c *pdBaseClient) switchTSOAllocatorLeader(allocatorMap map[string]*pdpb.Member) error {
	if len(allocatorMap) == 0 {
		return nil
	}
	// Switch to the new one
	for dcLocation, member := range allocatorMap {
		if len(member.GetClientUrls()) == 0 {
			continue
		}
		addr := member.GetClientUrls()[0]
		oldAddr, exist := c.GetTSOAllocatorLeaderAddrByDCLocation(dcLocation)
		if exist && addr == oldAddr {
			continue
		}
		if _, err := c.GetOrCreateGRPCConn(addr); err != nil {
			log.Warn("[pd] failed to connect dc tso allocator leader",
				zap.String("dc-location", dcLocation),
				zap.String("leader", addr),
				errs.ZapError(err))
			return err
		}
		c.tsoAllocators.Store(dcLocation, addr)
		log.Info("[pd] switch dc tso allocator leader",
			zap.String("dc-location", dcLocation),
			zap.String("new-leader", addr),
			zap.String("old-leader", oldAddr))
	}
	// Garbage collection of the old TSO allocator leaders
	c.gcAllocatorLeaderAddr(allocatorMap)
	return nil
}

// GetOrCreateGRPCConn returns the corresponding grpc client connection of the given addr
func (c *pdBaseClient) GetOrCreateGRPCConn(addr string) (*grpc.ClientConn, error) {
	conn, ok := c.clientConns.Load(addr)
	if ok {
		return conn.(*grpc.ClientConn), nil
	}
	tlsCfg, err := tlsutil.TLSConfig{
		CAPath:   c.security.CAPath,
		CertPath: c.security.CertPath,
		KeyPath:  c.security.KeyPath,

		SSLCABytes:   c.security.SSLCABytes,
		SSLCertBytes: c.security.SSLCertBytes,
		SSLKEYBytes:  c.security.SSLKEYBytes,
	}.ToTLSConfig()
	if err != nil {
		return nil, err
	}
	dCtx, cancel := context.WithTimeout(c.ctx, dialTimeout)
	defer cancel()
	cc, err := grpcutil.GetClientConn(dCtx, addr, tlsCfg, c.option.gRPCDialOptions...)
	if err != nil {
		return nil, err
	}
	if old, ok := c.clientConns.Load(addr); ok {
		cc.Close()
		log.Debug("use old connection", zap.String("target", cc.Target()), zap.String("state", cc.GetState().String()))
		return old.(*grpc.ClientConn), nil
	}
	c.clientConns.Store(addr, cc)
	return cc, nil
}

// CreateTsoStream creates a TSO stream to send/recv timestamps
func (c *pdBaseClient) createTsoStreamInternal(ctx context.Context, cancel context.CancelFunc, client pdpb.PDClient) (interface{}, error) {
	done := make(chan struct{})
	// TODO: we need to handle a conner case that this goroutine is timeout while the stream is successfully created.
	go c.checkStreamTimeout(ctx, cancel, done)
	stream, err := client.Tso(ctx)
	done <- struct{}{}
	return stream, err
}

func (c *pdBaseClient) checkStreamTimeout(ctx context.Context, cancel context.CancelFunc, done chan struct{}) {
	select {
	case <-done:
		return
	case <-time.After(c.option.timeout):
		cancel()
	case <-ctx.Done():
	}
	<-done
}

func (c *pdBaseClient) requestHeader() *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: c.clusterID,
	}
}

// CreateTsoStream creates a TSO stream to send/recv timestamps
func (c *pdBaseClient) CreateTsoStream(ctx context.Context, cancel context.CancelFunc, cc *grpc.ClientConn) (interface{}, error) {
	return c.createTsoStreamInternal(ctx, cancel, pdpb.NewPDClient(cc))
}

func (c *pdBaseClient) getAllClients() map[string]pdpb.PDClient {
	var (
		addrs   = c.GetURLs()
		clients = make(map[string]pdpb.PDClient, len(addrs))
		cc      *grpc.ClientConn
		err     error
	)
	for _, addr := range addrs {
		if len(addrs) == 0 {
			continue
		}
		if cc, err = c.GetOrCreateGRPCConn(addr); err != nil {
			continue
		}
		healthCtx, healthCancel := context.WithTimeout(c.ctx, c.option.timeout)
		resp, err := healthpb.NewHealthClient(cc).Check(healthCtx, &healthpb.HealthCheckRequest{Service: ""})
		healthCancel()
		if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			clients[addr] = pdpb.NewPDClient(cc)
		}
	}
	return clients
}

// TryConnectToTSOWithProxy will create multiple streams to all the PD servers to work as a TSO proxy
// to reduce the pressure of PD leader.
func (c *pdBaseClient) TryConnectToTSOWithProxy(
	dispatcherCtx context.Context,
	dc string,
	connectionCtxs *sync.Map,
) error {
	clients := c.getAllClients()
	leaderAddr := c.getLeaderAddr()
	forwardedHost, ok := c.GetTSOAllocatorLeaderAddrByDCLocation(dc)
	if !ok {
		return errors.Errorf("cannot find the allocator leader in %s", dc)
	}
	// GC the stale one.
	connectionCtxs.Range(func(addr, cc interface{}) bool {
		if _, ok := clients[addr.(string)]; !ok {
			cc.(*tsoConnectionContext).cancel()
			connectionCtxs.Delete(addr)
		}
		return true
	})
	// Update the missing one.
	for addr, client := range clients {
		if _, ok = connectionCtxs.Load(addr); ok {
			continue
		}
		cctx, cancel := context.WithCancel(dispatcherCtx)
		// Do not proxy the leader client.
		if addr != leaderAddr {
			log.Info("[pd] use follower to forward tso stream to do the proxy", zap.String("dc", dc), zap.String("addr", addr))
			cctx = grpcutil.BuildForwardContext(cctx, forwardedHost)
		}
		// Create the TSO stream.
		stream, err := c.createTsoStreamInternal(cctx, cancel, client)
		if err == nil {
			if addr != leaderAddr {
				forwardedHostTrim := trimHTTPPrefix(forwardedHost)
				addrTrim := trimHTTPPrefix(addr)
				requestForwarded.WithLabelValues(forwardedHostTrim, addrTrim).Set(1)
			}
			connectionCtxs.Store(addr, &tsoConnectionContext{addr, stream, cctx, cancel})
			continue
		}
		log.Error("[pd] create the tso stream failed", zap.String("dc", dc), zap.String("addr", addr), errs.ZapError(err))
		cancel()
	}
	return nil
}

// ProcessTSORequests processes TSO requests in streaming mode to get timestamps
func (c *pdBaseClient) ProcessTSORequests(stream interface{}, dcLocation string, requests []*tsoRequest,
	batchStartTime time.Time) (physical, logical int64, suffixBits uint32, err error) {
	tsoStream := stream.(pdpb.PD_TsoClient)

	start := time.Now()
	count := int64(len(requests))
	req := &pdpb.TsoRequest{
		Header:     c.requestHeader(),
		Count:      uint32(count),
		DcLocation: dcLocation,
	}

	if err = tsoStream.Send(req); err != nil {
		err = errors.WithStack(err)
		return
	}
	tsoBatchSendLatency.Observe(float64(time.Since(batchStartTime)))
	resp, err := tsoStream.Recv()
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	requestDurationTSO.Observe(time.Since(start).Seconds())
	tsoBatchSize.Observe(float64(count))

	if resp.GetCount() != uint32(count) {
		err = errors.WithStack(errTSOLength)
		return
	}

	physical, logical, suffixBits = resp.GetTimestamp().GetPhysical(), resp.GetTimestamp().GetLogical(), resp.GetTimestamp().GetSuffixBits()
	return
}
