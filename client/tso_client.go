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

package pd

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/log"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/grpcutil"
	"github.com/tikv/pd/client/tlsutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// TSOClient manages resource group info and token request.
type TSOClient interface {
	// GetTSWithinKeyspace gets a timestamp within the given keyspace from the TSO service
	GetTSWithinKeyspace(ctx context.Context, keyspaceID uint32) (int64, int64, error)
	// GetTSWithinKeyspaceAsync gets a timestamp within the given keyspace from the TSO service,
	// without block the caller.
	GetTSWithinKeyspaceAsync(ctx context.Context, keyspaceID uint32) TSFuture
	// GetLocalTSWithinKeyspace gets a local timestamp within the given keyspace from the TSO service
	GetLocalTSWithinKeyspace(ctx context.Context, dcLocation string, keyspaceID uint32) (int64, int64, error)
	// GetLocalTSWithinKeyspaceAsync gets a local timestamp within the given keyspace from the TSO service,
	// without block the caller.
	GetLocalTSWithinKeyspaceAsync(ctx context.Context, dcLocation string, keyspaceID uint32) TSFuture
}

// GetTSWithinKeyspace gets a timestamp within the given keyspace from the TSO service
// TODO: Refactor and share the TSO streaming framework in the PD client. The implementation
// here is in a basic manner and only for testing and integration purpose -- no batching,
// no async, no pooling, no forwarding, no retry and no deliberate error handling.
func (c *client) GetTSWithinKeyspace(ctx context.Context, keyspaceID uint32) (physical int64, logical int64, err error) {
	resp := c.GetTSWithinKeyspaceAsync(ctx, keyspaceID)
	return resp.Wait()
}

// GetLocalTSWithinKeyspace gets a local timestamp within the given keyspace from the TSO service
func (c *client) GetLocalTSWithinKeyspace(ctx context.Context, dcLocation string, keyspaceID uint32) (physical int64, logical int64, err error) {
	resp := c.GetLocalTSWithinKeyspaceAsync(ctx, dcLocation, keyspaceID)
	return resp.Wait()
}

// GetTSWithinKeyspaceAsync gets a timestamp within the given keyspace from the TSO service,
// without block the caller.
func (c *client) GetTSWithinKeyspaceAsync(ctx context.Context, keyspaceID uint32) TSFuture {
	return c.GetLocalTSWithinKeyspaceAsync(ctx, globalDCLocation, keyspaceID)
}

// GetLocalTSWithinKeyspaceAsync gets a local timestamp within the given keyspace from the TSO service,
// without block the caller.
// TODO: implement the following API
func (c *client) GetLocalTSWithinKeyspaceAsync(ctx context.Context, dcLocation string, keyspaceID uint32) TSFuture {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("GetLocalTSWithinKeyspaceAsync", opentracing.ChildOf(span.Context()))
		ctx = opentracing.ContextWithSpan(ctx, span)
	}
	req := tsoReqPool.Get().(*tsoRequest)
	req.requestCtx = ctx
	req.clientCtx = c.ctx
	req.start = time.Now()
	req.dcLocation = dcLocation
	req.keyspaceID = keyspaceID
	if err := c.dispatchRequest(dcLocation, req); err != nil {
		// Wait for a while and try again
		time.Sleep(50 * time.Millisecond)
		if err = c.dispatchRequest(dcLocation, req); err != nil {
			req.done <- err
		}
	}
	return req
}

var _ BaseClient = (*tsoBaseClient)(nil)

// tsoBaseClient is the service discovery client of TSO microservice which is primary/standby configured
type tsoBaseClient struct {
	urls atomic.Value // Store as []string
	// TSO Primary URL
	primary atomic.Value // Store as string
	// TSO Secondary URLs
	secondaries atomic.Value // Store as []string

	// addr -> a gRPC connection
	clientConns sync.Map // Store as map[string]*grpc.ClientConn
	// dc-location -> TSO allocator primary URL
	tsoAllocators sync.Map // Store as map[string]string

	// primarySwitchedCallbacks will be called after the primary swichted
	primarySwitchedCallbacks []func()
	// membersChangedCallbacks will be called after there is any membership
	// change in the primary and followers
	membersChangedCallbacks []func()
	// tsoAllocatorLeaderSwitchedCallback will be called when any keyspace group tso
	// allocator primary is switched.
	tsoAllocatorLeaderSwitchedCallback []func()

	checkMembershipCh chan struct{}

	wg     *sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	tlsCfg *tlsutil.TLSConfig

	// Client option.
	option *option
}

// newTSOBaseClient returns a new baseClient.
func newTSOBaseClient(ctx context.Context, cancel context.CancelFunc,
	wg *sync.WaitGroup, urls []string, tlsCfg *tlsutil.TLSConfig, option *option) BaseClient {
	bc := &tsoBaseClient{
		checkMembershipCh: make(chan struct{}, 1),
		ctx:               ctx,
		cancel:            cancel,
		wg:                wg,
		tlsCfg:            tlsCfg,
		option:            option,
	}
	bc.urls.Store(urls)
	// TODO: fill the missing part for service discovery
	bc.switchPrimary(urls)

	_, err := bc.GetOrCreateGRPCConn(bc.getPrimaryAddr())
	if err != nil {
		return nil
	}

	return bc
}

// Init initialize the concrete client underlying
func (c *tsoBaseClient) Init() error {
	return nil
}

// Close all grpc client connnections
func (c *tsoBaseClient) CloseClientConns() {
	c.clientConns.Range(func(_, cc interface{}) bool {
		if err := cc.(*grpc.ClientConn).Close(); err != nil {
			log.Error("[pd] failed to close gRPC clientConn", errs.ZapError(errs.ErrCloseGRPCConn, err))
		}
		return true
	})
}

// GetClusterID returns the ID of the cluster
func (c *tsoBaseClient) GetClusterID(context.Context) uint64 {
	return 0
}

// GetURLs returns the URLs of the servers.
// For testing use. It should only be called when the client is closed.
func (c *tsoBaseClient) GetURLs() []string {
	return c.urls.Load().([]string)
}

// GetTSOAllocators returns {dc-location -> TSO allocator primary URL} connection map
func (c *tsoBaseClient) GetTSOAllocators() *sync.Map {
	return &c.tsoAllocators
}

// GetTSOAllocatorServingAddrByDCLocation returns the tso allocator of the given dcLocation
func (c *tsoBaseClient) GetTSOAllocatorServingAddrByDCLocation(dcLocation string) (string, bool) {
	url, exist := c.tsoAllocators.Load(dcLocation)
	if !exist {
		return "", false
	}
	return url.(string), true
}

// GetTSOAllocatorClientConnByDCLocation returns the tso allocator grpc client connection
// of the given dcLocation
func (c *tsoBaseClient) GetTSOAllocatorClientConnByDCLocation(dcLocation string) (*grpc.ClientConn, string) {
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

// GetServingEndpointAddr returns the grpc client connection of the serving endpoint
// which is the primary in a primary/secondy configured cluster.
func (c *tsoBaseClient) GetServingEndpointClientConn() *grpc.ClientConn {
	if cc, ok := c.clientConns.Load(c.getPrimaryAddr()); ok {
		return cc.(*grpc.ClientConn)
	}
	return nil
}

// GetServingEndpointAddr returns the serving endpoint which is the primary in a
// primary/secondy configured cluster.
func (c *tsoBaseClient) GetServingEndpointAddr() string {
	return c.getPrimaryAddr()
}

// GetBackupEndpointsAddrs gets the addresses of the current reachable and healthy
// backup service endpoints randomly. Backup service endpoints are secondaries in
// a primary/secondary configured cluster.
func (c *tsoBaseClient) GetBackupEndpointsAddrs() []string {
	return c.getSecondaryAddrs()
}

// GetOrCreateGRPCConn returns the corresponding grpc client connection of the given addr
func (c *tsoBaseClient) GetOrCreateGRPCConn(addr string) (*grpc.ClientConn, error) {
	return grpcutil.GetOrCreateGRPCConn(c.ctx, &c.clientConns, addr, c.tlsCfg, c.option.gRPCDialOptions...)
}

// ScheduleCheckIfMembershipChanged is used to trigger a check to see if there is any
// membership change among the primary/secondaries in a primary/secondy configured cluster.
func (c *tsoBaseClient) ScheduleCheckIfMembershipChanged() {

}

// Immediately checkif there is any membership change among the primary/secondaries in
// a primary/secondy configured cluster.
func (c *tsoBaseClient) CheckIfMembershipChanged() error {
	return nil
}

// AddServiceEndpointSwitchedCallback adds callbacks which will be called when the primary in
// a primary/secondary configured cluster is switched.
func (c *tsoBaseClient) AddServiceEndpointSwitchedCallback(callbacks ...func()) {
	c.primarySwitchedCallbacks = append(c.primarySwitchedCallbacks, callbacks...)
}

// AddServiceEndpointsChangedCallback adds callbacks which will be called when any primary/secondary
// in a primary/secondary configured cluster is changed.
func (c *tsoBaseClient) AddServiceEndpointsChangedCallback(callbacks ...func()) {
	c.membersChangedCallbacks = append(c.membersChangedCallbacks, callbacks...)
}

// AddTSOAllocatorServiceEndpointSwitchedCallback adds callbacks which will be called
// when any keyspace group tso allocator primary is switched.
func (c *tsoBaseClient) AddTSOAllocatorServiceEndpointSwitchedCallback(callbacks ...func()) {
	c.tsoAllocatorLeaderSwitchedCallback = append(c.tsoAllocatorLeaderSwitchedCallback, callbacks...)
}

// getPrimaryAddr returns the primary address.
func (c *tsoBaseClient) getPrimaryAddr() string {
	primaryAddr := c.primary.Load()
	if primaryAddr == nil {
		return ""
	}
	return primaryAddr.(string)
}

// getSecondaryAddrs returns the secondary addresses.
func (c *tsoBaseClient) getSecondaryAddrs() []string {
	secondaryAddrs := c.secondaries.Load()
	if secondaryAddrs == nil {
		return []string{}
	}
	return secondaryAddrs.([]string)
}

func (c *tsoBaseClient) switchPrimary(addrs []string) error {
	// FIXME: How to safely compare primary urls? For now, only allows one client url.
	addr := addrs[0]
	oldPrimary := c.getPrimaryAddr()
	if addr == oldPrimary {
		return nil
	}

	if _, err := c.GetOrCreateGRPCConn(addr); err != nil {
		log.Warn("[pd] failed to connect primary", zap.String("primary", addr), errs.ZapError(err))
		return err
	}
	// Set PD primary and Global TSO Allocator (which is also the PD primary)
	c.primary.Store(addr)
	c.tsoAllocators.Store(globalDCLocation, addr)
	// Run callbacks
	for _, cb := range c.primarySwitchedCallbacks {
		cb()
	}
	log.Info("[tso] switch primary", zap.String("new-primary", addr), zap.String("old-primary", oldPrimary))
	return nil
}
