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
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/tsopb"
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
		span = opentracing.StartSpan("GetLocalTSAsync", opentracing.ChildOf(span.Context()))
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

// tsoBaseClient is the service discovery client of TSO microservice which is primary/standby configured
type tsoBaseClient struct {
	clusterID uint64
	// addr -> a gRPC connection
	clientConns sync.Map // Store as map[string]*grpc.ClientConn
	// dc-location -> TSO allocator leader URL
	tsoAllocators sync.Map // Store as map[string]string

	// primarySwitchedCallbacks will be called after the primary swichted
	primarySwitchedCallbacks []func()
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

var _ BaseClient = (*tsoBaseClient)(nil)

// Init initialize the concrete client underlying
func (c *tsoBaseClient) Init() error {
	return nil
}

// Close all grpc client connnections
func (c *tsoBaseClient) CloseClientConns() {
}

// GetClusterID returns the ID of the cluster
func (c *tsoBaseClient) GetClusterID(context.Context) uint64 {
	return 0
}

// GetTSOAllocators returns {dc-location -> TSO allocator leader URL} connection map
func (c *tsoBaseClient) GetTSOAllocators() *sync.Map {
	return nil
}

// GetTSOAllocatorLeaderAddrByDCLocation returns the tso allocator of the given dcLocation
func (c *tsoBaseClient) GetTSOAllocatorLeaderAddrByDCLocation(dcLocation string) (string, bool) {
	return "", false
}

// GetTSOAllocatorClientConnByDCLocation returns the tso allocator grpc client connection
// of the given dcLocation
func (c *tsoBaseClient) GetTSOAllocatorClientConnByDCLocation(dcLocation string) (*grpc.ClientConn, string) {
	return nil, ""
}

// GetServingEndpointAddr returns the grpc client connection of the serving endpoint
// which is the leader in a quorum-based cluster or the primary in a primary/secondy
// configured cluster.
func (c *tsoBaseClient) GetServingEndpointClientConn() *grpc.ClientConn {
	return nil
}

// GetServingEndpointAddr returns the serving endpoint which is the leader
// in a quorum-based cluster or the primary in a primary/secondy configured cluster.
func (c *tsoBaseClient) GetServingEndpointAddr() string {
	return ""
}

// GetBackupEndpointsAddrs gets the addresses of the current reachable and healthy
// backup service endpoints randomly. Backup service endpoints are followers in a
// quorum-based cluster or secondaries in a primary/secondary configured cluster.
func (c *tsoBaseClient) GetBackupEndpointsAddrs() []string {
	return make([]string, 0)
}

// GetOrCreateGRPCConn returns the corresponding grpc client connection of the given addr
func (c *tsoBaseClient) GetOrCreateGRPCConn(addr string) (*grpc.ClientConn, error) {
	return nil, nil
}

// ScheduleCheckIfMembershipChanged is used to trigger a check to see if there is any
// membership change among the leader/followers in a quorum-based cluster or among
// the primary/secondaries in a primary/secondy configured cluster.
func (c *tsoBaseClient) ScheduleCheckIfMembershipChanged() {

}

// Immediately checkif there is any membership change among the leader/followers in a
// quorum-based cluster or among the primary/secondaries in a primary/secondy configured cluster.
func (c *tsoBaseClient) CheckIfMembershipChanged() error {
	return nil
}

// AddServiceEndpointSwitchedCallback adds callbacks which will be called when the leader
// in a quorum-based cluster or the primary in a primary/secondary configured cluster
// is switched.
func (c *tsoBaseClient) AddServiceEndpointSwitchedCallback(callbacks ...func()) {

}

// AddServiceEndpointsChangedCallback adds callbacks which will be called when any leader/follower
// in a quorum-based cluster or the primary in a primary/secondary configured cluster is changed.
func (c *tsoBaseClient) AddServiceEndpointsChangedCallback(callbacks ...func()) {

}

// CreateTsoStream creates a TSO stream to send/recv timestamps
func (c *tsoBaseClient) createTsoStreamInternal(ctx context.Context, cancel context.CancelFunc, client tsopb.TSOClient) (interface{}, error) {
	done := make(chan struct{})
	// TODO: we need to handle a conner case that this goroutine is timeout while the stream is successfully created.
	go c.checkStreamTimeout(ctx, cancel, done)
	stream, err := client.Tso(ctx)
	done <- struct{}{}
	return stream, err
}

func (c *tsoBaseClient) checkStreamTimeout(ctx context.Context, cancel context.CancelFunc, done chan struct{}) {
	select {
	case <-done:
		return
	case <-time.After(c.option.timeout):
		cancel()
	case <-ctx.Done():
	}
	<-done
}

func (c *tsoBaseClient) requestHeader() *tsopb.RequestHeader {
	return &tsopb.RequestHeader{
		ClusterId: c.clusterID,
	}
}

// CreateTsoStream creates a TSO stream to send/recv timestamps
func (c *tsoBaseClient) CreateTsoStream(ctx context.Context, cancel context.CancelFunc, cc *grpc.ClientConn) (interface{}, error) {
	return c.createTsoStreamInternal(ctx, cancel, tsopb.NewTSOClient(cc))
}

// TryConnectToTSOWithProxy will create multiple streams to all the service endpoints to work as
// a TSO proxy to reduce the pressure of the main serving service endpoint.
func (c *tsoBaseClient) TryConnectToTSOWithProxy(dispatcherCtx context.Context, dc string, connectionCtxs *sync.Map) error {
	return nil
}

// ProcessTSORequests processes TSO requests in streaming mode to get timestamps
func (c *tsoBaseClient) ProcessTSORequests(stream interface{}, dcLocation string, requests []*tsoRequest,
	batchStartTime time.Time) (physical, logical int64, suffixBits uint32, err error) {
	tsoStream := stream.(tsopb.TSO_TsoClient)

	start := time.Now()
	count := int64(len(requests))
	req := &tsopb.TsoRequest{
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

// GetURLs returns the URLs of the servers.
// For testing use. It should only be called when the client is closed.
func (c *tsoBaseClient) GetURLs() []string {
	return make([]string, 0)
}

// GetTSOAllocatorLeaderURLs returns the urls of the tso allocator leaders
// For testing use.
func (c *tsoBaseClient) GetTSOAllocatorLeaderURLs() map[string]string {
	return make(map[string]string)
}
