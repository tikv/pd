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

package pd

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/grpcutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// Region contains information of a region's meta and its peers.
type Region struct {
	Meta         *metapb.Region
	Leader       *metapb.Peer
	DownPeers    []*metapb.Peer
	PendingPeers []*metapb.Peer
}

// Client is a PD (Placement Driver) client.
// It should not be used after calling Close().
type Client interface {
	// GetClusterID gets the cluster ID from PD.
	GetClusterID(ctx context.Context) uint64
	// GetAllMembers gets the members Info from PD
	GetAllMembers(ctx context.Context) ([]*pdpb.Member, error)
	// GetLeaderAddr returns current leader's address. It returns "" before
	// syncing leader from server.
	GetLeaderAddr() string
	// GetTS gets a timestamp from PD.
	GetTS(ctx context.Context) (int64, int64, error)
	// GetTSAsync gets a timestamp from PD, without block the caller.
	GetTSAsync(ctx context.Context) TSFuture
	// GetLocalTS gets a local timestamp from PD.
	GetLocalTS(ctx context.Context, dcLocation string) (int64, int64, error)
	// GetLocalTSAsync gets a local timestamp from PD, without block the caller.
	GetLocalTSAsync(ctx context.Context, dcLocation string) TSFuture
	// GetRegion gets a region and its leader Peer from PD by key.
	// The region may expire after split. Caller is responsible for caching and
	// taking care of region change.
	// Also it may return nil if PD finds no Region for the key temporarily,
	// client should retry later.
	GetRegion(ctx context.Context, key []byte) (*Region, error)
	// GetRegionFromMember gets a region from certain members.
	GetRegionFromMember(ctx context.Context, key []byte, memberURLs []string) (*Region, error)
	// GetPrevRegion gets the previous region and its leader Peer of the region where the key is located.
	GetPrevRegion(ctx context.Context, key []byte) (*Region, error)
	// GetRegionByID gets a region and its leader Peer from PD by id.
	GetRegionByID(ctx context.Context, regionID uint64) (*Region, error)
	// ScanRegion gets a list of regions, starts from the region that contains key.
	// Limit limits the maximum number of regions returned.
	// If a region has no leader, corresponding leader will be placed by a peer
	// with empty value (PeerID is 0).
	ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*Region, error)
	// GetStore gets a store from PD by store id.
	// The store may expire later. Caller is responsible for caching and taking care
	// of store change.
	GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error)
	// GetAllStores gets all stores from pd.
	// The store may expire later. Caller is responsible for caching and taking care
	// of store change.
	GetAllStores(ctx context.Context, opts ...GetStoreOption) ([]*metapb.Store, error)
	// Update GC safe point. TiKV will check it and do GC themselves if necessary.
	// If the given safePoint is less than the current one, it will not be updated.
	// Returns the new safePoint after updating.
	UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error)
	// UpdateServiceGCSafePoint updates the safepoint for specific service and
	// returns the minimum safepoint across all services, this value is used to
	// determine the safepoint for multiple services, it does not trigger a GC
	// job. Use UpdateGCSafePoint to trigger the GC job if needed.
	UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error)
	// ScatterRegion scatters the specified region. Should use it for a batch of regions,
	// and the distribution of these regions will be dispersed.
	// NOTICE: This method is the old version of ScatterRegions, you should use the later one as your first choice.
	ScatterRegion(ctx context.Context, regionID uint64) error
	// ScatterRegions scatters the specified regions. Should use it for a batch of regions,
	// and the distribution of these regions will be dispersed.
	ScatterRegions(ctx context.Context, regionsID []uint64, opts ...RegionsOption) (*pdpb.ScatterRegionResponse, error)
	// SplitRegions split regions by given split keys
	SplitRegions(ctx context.Context, splitKeys [][]byte, opts ...RegionsOption) (*pdpb.SplitRegionsResponse, error)
	// GetOperator gets the status of operator of the specified region.
	GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error)
	// UpdateOption updates the client option.
	UpdateOption(option DynamicOption, value interface{}) error
	// Close closes the client.
	Close()
}

// GetStoreOp represents available options when getting stores.
type GetStoreOp struct {
	excludeTombstone bool
}

// GetStoreOption configures GetStoreOp.
type GetStoreOption func(*GetStoreOp)

// WithExcludeTombstone excludes tombstone stores from the result.
func WithExcludeTombstone() GetStoreOption {
	return func(op *GetStoreOp) { op.excludeTombstone = true }
}

// RegionsOp represents available options when operate regions
type RegionsOp struct {
	group      string
	retryLimit uint64
}

// RegionsOption configures RegionsOp
type RegionsOption func(op *RegionsOp)

// WithGroup specify the group during Scatter/Split Regions
func WithGroup(group string) RegionsOption {
	return func(op *RegionsOp) { op.group = group }
}

// WithRetry specify the retry limit during Scatter/Split Regions
func WithRetry(retry uint64) RegionsOption {
	return func(op *RegionsOp) { op.retryLimit = retry }
}

type tsoRequest struct {
	start      time.Time
	clientCtx  context.Context
	requestCtx context.Context
	done       chan error
	physical   int64
	logical    int64
	dcLocation string
}

type tsoBatchController struct {
	maxBatchSize int
	// bestBatchSize is a dynamic size that changed based on the current batch effect.
	bestBatchSize int

	tsoRequestCh          chan *tsoRequest
	collectedRequests     []*tsoRequest
	collectedRequestCount int

	batchStartTime time.Time
}

func newTSOBatchController(tsoRequestCh chan *tsoRequest, maxBatchSize int) *tsoBatchController {
	return &tsoBatchController{
		maxBatchSize:          maxBatchSize,
		bestBatchSize:         8, /* Starting from a low value is necessary because we need to make sure it will be converged to (current_batch_size - 4) */
		tsoRequestCh:          tsoRequestCh,
		collectedRequests:     make([]*tsoRequest, maxBatchSize+1),
		collectedRequestCount: 0,
	}
}

// fetchPendingRequests will start a new round of the batch collecting from the channel.
// It returns true if everything goes well, otherwise false which means we should stop the service.
func (tbc *tsoBatchController) fetchPendingRequests(ctx context.Context, maxBatchWaitInterval time.Duration) error {
	var firstTSORequest *tsoRequest
	select {
	case <-ctx.Done():
		return ctx.Err()
	case firstTSORequest = <-tbc.tsoRequestCh:
	}
	// Start to batch when the first TSO request arrives.
	tbc.batchStartTime = time.Now()
	tbc.collectedRequestCount = 0
	tbc.pushRequest(firstTSORequest)

	// This loop is for trying best to collect more requests, so we use `tbc.maxBatchSize` here.
fetchPendingRequestsLoop:
	for tbc.collectedRequestCount < tbc.maxBatchSize {
		select {
		case tsoReq := <-tbc.tsoRequestCh:
			tbc.pushRequest(tsoReq)
		case <-ctx.Done():
			return ctx.Err()
		default:
			break fetchPendingRequestsLoop
		}
	}

	// Check whether we should fetch more pending TSO requests from the channel.
	// TODO: maybe consider the actual load that returns through a TSO response from PD server.
	if tbc.collectedRequestCount >= tbc.maxBatchSize || maxBatchWaitInterval <= 0 {
		return nil
	}

	// Fetches more pending TSO requests from the channel.
	// Try to collect `tbc.bestBatchSize` requests, or wait `maxBatchWaitInterval`
	// when `tbc.collectedRequestCount` is less than the `tbc.bestBatchSize`.
	if tbc.collectedRequestCount < tbc.bestBatchSize {
		after := time.NewTimer(maxBatchWaitInterval)
		defer after.Stop()
		for tbc.collectedRequestCount < tbc.bestBatchSize {
			select {
			case tsoReq := <-tbc.tsoRequestCh:
				tbc.pushRequest(tsoReq)
			case <-ctx.Done():
				return ctx.Err()
			case <-after.C:
				return nil
			}
		}
	}

	// Do an additional non-block try. Here we test the length with `tbc.maxBatchSize` instead
	// of `tbc.bestBatchSize` because trying best to fetch more requests is necessary so that
	// we can adjust the `tbc.bestBatchSize` dynamically later.
	for tbc.collectedRequestCount < tbc.maxBatchSize {
		select {
		case tsoReq := <-tbc.tsoRequestCh:
			tbc.pushRequest(tsoReq)
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	return nil
}

func (tbc *tsoBatchController) pushRequest(tsoReq *tsoRequest) {
	tbc.collectedRequests[tbc.collectedRequestCount] = tsoReq
	tbc.collectedRequestCount++
}

// adjustBestBatchSize stabilizes the latency with the AIAD algorithm.
func (tbc *tsoBatchController) adjustBestBatchSize() {
	tsoBestBatchSize.Observe(float64(tbc.bestBatchSize))
	length := tbc.collectedRequestCount
	if length < tbc.bestBatchSize && tbc.bestBatchSize > 1 {
		// Waits too long to collect requests, reduce the target batch size.
		tbc.bestBatchSize--
	} else if length > tbc.bestBatchSize+4 /* Hard-coded number, in order to make `tbc.bestBatchSize` stable */ &&
		tbc.bestBatchSize < tbc.maxBatchSize {
		tbc.bestBatchSize++
	}
}

type tsoDispatcher struct {
	dispatcherCtx      context.Context
	dispatcherCancel   context.CancelFunc
	tsoBatchController *tsoBatchController
}

type lastTSO struct {
	physical int64
	logical  int64
}

const (
	dialTimeout            = 3 * time.Second
	updateMemberTimeout    = time.Second // Use a shorter timeout to recover faster from network isolation.
	tsLoopDCCheckInterval  = time.Minute
	defaultMaxTSOBatchSize = 10000 // should be higher if client is sending requests in burst
	retryInterval          = 1 * time.Second
	maxRetryTimes          = 5
)

// LeaderHealthCheckInterval might be changed in the unit to shorten the testing time.
var LeaderHealthCheckInterval = time.Second

var (
	// errFailInitClusterID is returned when failed to load clusterID from all supplied PD addresses.
	errFailInitClusterID = errors.New("[pd] failed to get cluster id")
	// errClosing is returned when request is canceled when client is closing.
	errClosing = errors.New("[pd] closing")
	// errTSOLength is returned when the number of response timestamps is inconsistent with request.
	errTSOLength = errors.New("[pd] tso length in rpc response is incorrect")
)

// ClientOption configures client.
type ClientOption func(c *client)

// WithGRPCDialOptions configures the client with gRPC dial options.
func WithGRPCDialOptions(opts ...grpc.DialOption) ClientOption {
	return func(c *client) {
		c.option.gRPCDialOptions = append(c.option.gRPCDialOptions, opts...)
	}
}

// WithCustomTimeoutOption configures the client with timeout option.
func WithCustomTimeoutOption(timeout time.Duration) ClientOption {
	return func(c *client) {
		c.option.timeout = timeout
	}
}

// WithForwardingOption configures the client with forwarding option.
func WithForwardingOption(enableForwarding bool) ClientOption {
	return func(c *client) {
		c.option.enableForwarding = enableForwarding
	}
}

// WithMaxErrorRetry configures the client max retry times when connect meets error.
func WithMaxErrorRetry(count int) ClientOption {
	return func(c *client) {
		c.option.maxRetryTimes = count
	}
}

type client struct {
	*baseClient
	// tsoDispatcher is used to dispatch different TSO requests to
	// the corresponding dc-location TSO channel.
	tsoDispatcher sync.Map // Same as map[string]chan *tsoRequest
	// dc-location -> deadline
	tsDeadline sync.Map // Same as map[string]chan deadline
	// dc-location -> *lastTSO
	lastTSMap sync.Map // Same as map[string]*lastTSO

	// For internal usage.
	checkTSDeadlineCh    chan struct{}
	leaderNetworkFailure int32
}

// NewClient creates a PD client.
func NewClient(pdAddrs []string, security SecurityOption, opts ...ClientOption) (Client, error) {
	return NewClientWithContext(context.Background(), pdAddrs, security, opts...)
}

// NewClientWithContext creates a PD client with context.
func NewClientWithContext(ctx context.Context, pdAddrs []string, security SecurityOption, opts ...ClientOption) (Client, error) {
	log.Info("[pd] create pd client with endpoints", zap.Strings("pd-address", pdAddrs))
	c := &client{
		baseClient:        newBaseClient(ctx, addrsToUrls(pdAddrs), security),
		checkTSDeadlineCh: make(chan struct{}),
	}
	// Inject the client options.
	for _, opt := range opts {
		opt(c)
	}
	// Init the client base.
	if err := c.init(); err != nil {
		return nil, err
	}
	// Start the daemons.
	c.updateTSODispatcher()
	c.wg.Add(3)
	go c.tsLoop()
	go c.tsCancelLoop()
	go c.leaderCheckLoop()

	return c, nil
}

// UpdateOption updates the client option.
func (c *client) UpdateOption(option DynamicOption, value interface{}) error {
	switch option {
	case MaxTSOBatchWaitInterval:
		interval, ok := value.(time.Duration)
		if !ok {
			return errors.New("[pd] invalid value type for MaxTSOBatchWaitInterval option, it should be time.Duration")
		}
		if err := c.option.setMaxTSOBatchWaitInterval(interval); err != nil {
			return err
		}
	case EnableTSOFollowerProxy:
		enable, ok := value.(bool)
		if !ok {
			return errors.New("[pd] invalid value type for EnableTSOFollowerProxy option, it should be bool")
		}
		c.option.setEnableTSOFollowerProxy(enable)
	default:
		return errors.New("[pd] unsupported client option")
	}
	return nil
}

func (c *client) updateTSODispatcher() {
	// Set up the new TSO dispatcher and batch controller.
	c.allocators.Range(func(dcLocationKey, _ interface{}) bool {
		dcLocation := dcLocationKey.(string)
		if !c.checkTSODispatcher(dcLocation) {
			c.createTSODispatcher(dcLocation)
		}
		return true
	})
	// Clean up the unused TSO dispatcher
	c.tsoDispatcher.Range(func(dcLocationKey, _ interface{}) bool {
		dcLocation := dcLocationKey.(string)
		// Skip the Global TSO Allocator
		if dcLocation == globalDCLocation {
			return true
		}
		if dispatcher, exist := c.allocators.Load(dcLocation); !exist {
			log.Info("[pd] delete unused tso dispatcher", zap.String("dc-location", dcLocation))
			dispatcher.(*tsoDispatcher).dispatcherCancel()
			c.tsoDispatcher.Delete(dcLocation)
		}
		return true
	})
}

func (c *client) leaderCheckLoop() {
	defer c.wg.Done()

	leaderCheckLoopCtx, leaderCheckLoopCancel := context.WithCancel(c.ctx)
	defer leaderCheckLoopCancel()

	ticker := time.NewTicker(LeaderHealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.checkLeaderHealth(leaderCheckLoopCtx)
		}
	}
}

func (c *client) checkLeaderHealth(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	defer cancel()
	if cc, ok := c.clientConns.Load(c.GetLeaderAddr()); ok {
		healthCli := healthpb.NewHealthClient(cc.(*grpc.ClientConn))
		resp, err := healthCli.Check(ctx, &healthpb.HealthCheckRequest{Service: ""})
		rpcErr, ok := status.FromError(err)
		failpoint.Inject("unreachableNetwork1", func() {
			resp = nil
			err = status.New(codes.Unavailable, "unavailable").Err()
		})
		if (ok && isNetworkError(rpcErr.Code())) || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			atomic.StoreInt32(&(c.leaderNetworkFailure), int32(1))
		} else {
			atomic.StoreInt32(&(c.leaderNetworkFailure), int32(0))
		}
	}
}

type deadline struct {
	timer  <-chan time.Time
	done   chan struct{}
	cancel context.CancelFunc
}

func (c *client) tsCancelLoop() {
	defer c.wg.Done()

	tsCancelLoopCtx, tsCancelLoopCancel := context.WithCancel(c.ctx)
	defer tsCancelLoopCancel()

	ticker := time.NewTicker(tsLoopDCCheckInterval)
	defer ticker.Stop()
	for {
		// Watch every dc-location's tsDeadlineCh
		c.allocators.Range(func(dcLocation, _ interface{}) bool {
			c.watchTSDeadline(tsCancelLoopCtx, dcLocation.(string))
			return true
		})
		select {
		case <-c.checkTSDeadlineCh:
			continue
		case <-ticker.C:
			continue
		case <-tsCancelLoopCtx.Done():
			return
		}
	}
}

func (c *client) watchTSDeadline(ctx context.Context, dcLocation string) {
	if _, exist := c.tsDeadline.Load(dcLocation); !exist {
		tsDeadlineCh := make(chan deadline, 1)
		c.tsDeadline.Store(dcLocation, tsDeadlineCh)
		go func(dc string, tsDeadlineCh <-chan deadline) {
			for {
				select {
				case d := <-tsDeadlineCh:
					select {
					case <-d.timer:
						log.Error("[pd] tso request is canceled due to timeout", zap.String("dc-location", dc), errs.ZapError(errs.ErrClientGetTSOTimeout))
						d.cancel()
					case <-d.done:
						continue
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}(dcLocation, tsDeadlineCh)
	}
}

func (c *client) scheduleCheckTSDeadline() {
	select {
	case c.checkTSDeadlineCh <- struct{}{}:
	default:
	}
}

func (c *client) checkStreamTimeout(streamCtx context.Context, cancel context.CancelFunc, done chan struct{}) {
	select {
	case <-done:
		return
	case <-time.After(c.option.timeout):
		cancel()
	case <-streamCtx.Done():
	}
	<-done
}

func (c *client) GetAllMembers(ctx context.Context) ([]*pdpb.Member, error) {
	start := time.Now()
	defer func() { cmdDurationGetAllMembers.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &pdpb.GetMembersRequest{Header: c.requestHeader()}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	resp, err := c.getClient().GetMembers(ctx, req)
	cancel()
	if err != nil {
		cmdFailDurationGetAllMembers.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return nil, errors.WithStack(err)
	}
	return resp.GetMembers(), nil
}

func (c *client) tsLoop() {
	defer c.wg.Done()

	loopCtx, loopCancel := context.WithCancel(c.ctx)
	defer loopCancel()

	ticker := time.NewTicker(tsLoopDCCheckInterval)
	defer ticker.Stop()
	for {
		c.updateTSODispatcher()
		select {
		case <-ticker.C:
		case <-c.checkTSODispatcherCh:
		case <-loopCtx.Done():
			return
		}
	}
}

func (c *client) createTsoStream(ctx context.Context, cancel context.CancelFunc, client pdpb.PDClient) (pdpb.PD_TsoClient, error) {
	done := make(chan struct{})
	// TODO: we need to handle a conner case that this goroutine is timeout while the stream is successfully created.
	go c.checkStreamTimeout(ctx, cancel, done)
	stream, err := client.Tso(ctx)
	done <- struct{}{}
	return stream, err
}

func (c *client) checkAllocator(
	dispatcherCtx context.Context,
	forwardCancel context.CancelFunc,
	dc, forwardedHostTrim, addrTrim, url string,
	updateAndClear func(newAddr string, connectionCtx *connectionContext)) {
	defer func() {
		// cancel the forward stream
		forwardCancel()
		requestForwarded.WithLabelValues(forwardedHostTrim, addrTrim).Set(0)
	}()
	cc, u := c.getAllocatorClientConnByDCLocation(dc)
	healthCli := healthpb.NewHealthClient(cc)
	for {
		// the pd/allocator leader change, we need to re-establish the stream
		if u != url {
			log.Info("[pd] the leader of the allocator leader is changed", zap.String("dc", dc), zap.String("origin", url), zap.String("new", u))
			return
		}
		healthCtx, healthCancel := context.WithTimeout(dispatcherCtx, c.option.timeout)
		resp, err := healthCli.Check(healthCtx, &healthpb.HealthCheckRequest{Service: ""})
		failpoint.Inject("unreachableNetwork", func() {
			resp.Status = healthpb.HealthCheckResponse_UNKNOWN
		})
		healthCancel()
		if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			// create a stream of the original allocator
			cctx, cancel := context.WithCancel(dispatcherCtx)
			stream, err := c.createTsoStream(cctx, cancel, pdpb.NewPDClient(cc))
			if err == nil && stream != nil {
				log.Info("[pd] recover the original tso stream since the network has become normal", zap.String("dc", dc), zap.String("url", url))
				updateAndClear(url, &connectionContext{url, stream, cancel})
				return
			}
		}
		select {
		case <-dispatcherCtx.Done():
			return
		case <-time.After(time.Second):
			// To ensure we can get the latest allocator leader
			// and once the leader is changed, we can exit this function.
			_, u = c.getAllocatorClientConnByDCLocation(dc)
		}
	}
}

func (c *client) checkTSODispatcher(dcLocation string) bool {
	dispatcher, ok := c.tsoDispatcher.Load(dcLocation)
	if !ok || dispatcher == nil {
		return false
	}
	return true
}

func (c *client) createTSODispatcher(dcLocation string) {
	dispatcherCtx, dispatcherCancel := context.WithCancel(c.ctx)
	dispatcher := &tsoDispatcher{
		dispatcherCtx:    dispatcherCtx,
		dispatcherCancel: dispatcherCancel,
		tsoBatchController: newTSOBatchController(
			make(chan *tsoRequest, defaultMaxTSOBatchSize*2),
			defaultMaxTSOBatchSize),
	}
	// Each goroutine is responsible for handling the tso stream request for its dc-location.
	// The only case that will make the dispatcher goroutine exit
	// is that the loopCtx is done, otherwise there is no circumstance
	// this goroutine should exit.
	go c.handleDispatcher(dispatcherCtx, dcLocation, dispatcher.tsoBatchController)
	c.tsoDispatcher.Store(dcLocation, dispatcher)
	log.Info("[pd] tso dispatcher created", zap.String("dc-location", dcLocation))
}

func (c *client) handleDispatcher(
	dispatcherCtx context.Context,
	dc string,
	tbc *tsoBatchController) {
	var (
		retryTimeConsuming time.Duration
		err                error
		streamAddr         string
		stream             pdpb.PD_TsoClient
		cancel             context.CancelFunc
		// addr -> connectionContext
		connectionCtxs sync.Map
		opts           []opentracing.StartSpanOption
	)
	defer func() {
		log.Info("[pd] exit tso dispatcher", zap.String("dc-location", dc))
		// Cancel all connections.
		connectionCtxs.Range(func(_, cc interface{}) bool {
			cc.(*connectionContext).cancel()
			return true
		})
	}()
	// Call updateConnectionCtxs once to init the connectionCtxs first.
	c.updateConnectionCtxs(dispatcherCtx, dc, &connectionCtxs)
	// Only the Global TSO needs to watch the updateConnectionCtxsCh to sense the
	// change of the cluster when TSO Follower Proxy is enabled.
	// TODO: support TSO Follower Proxy for the Local TSO.
	if dc == globalDCLocation {
		go func() {
			var updateTicker = &time.Ticker{}
			setNewUpdateTicker := func(ticker *time.Ticker) {
				if updateTicker.C != nil {
					updateTicker.Stop()
				}
				updateTicker = ticker
			}
			// Set to nil before returning to ensure that the existing ticker can be GC.
			defer setNewUpdateTicker(nil)

			for {
				select {
				case <-dispatcherCtx.Done():
					return
				case <-c.option.enableTSOFollowerProxyCh:
					enableTSOFollowerProxy := c.option.getEnableTSOFollowerProxy()
					if enableTSOFollowerProxy && updateTicker.C == nil {
						// Because the TSO Follower Proxy is enabled,
						// the periodic check needs to be performed.
						setNewUpdateTicker(time.NewTicker(memberUpdateInterval))
					} else if !enableTSOFollowerProxy && updateTicker.C != nil {
						// Because the TSO Follower Proxy is disabled,
						// the periodic check needs to be turned off.
						setNewUpdateTicker(&time.Ticker{})
					} else {
						// The status of TSO Follower Proxy does not change, and updateConnectionCtxs is not triggered
						continue
					}
				case <-updateTicker.C:
				case <-c.updateConnectionCtxsCh:
				}
				c.updateConnectionCtxs(dispatcherCtx, dc, &connectionCtxs)
			}
		}()
	}

	// Loop through each batch of TSO requests and send them for processing.
	for {
		select {
		case <-dispatcherCtx.Done():
			return
		default:
		}
		// Choose a stream to send the TSO gRPC request.
		connectionCtx := c.chooseStream(&connectionCtxs)
		if connectionCtx != nil {
			streamAddr, stream, cancel = connectionCtx.streamAddr, connectionCtx.stream, connectionCtx.cancel
		}
		// Check stream and retry if necessary.
		if stream == nil {
			log.Info("[pd] tso stream is not ready", zap.String("dc", dc))
			c.updateConnectionCtxs(dispatcherCtx, dc, &connectionCtxs)
			if retryTimeConsuming >= c.option.timeout {
				err = errs.ErrClientCreateTSOStream.FastGenByArgs()
				log.Error("[pd] create tso stream error", zap.String("dc-location", dc), errs.ZapError(err))
				c.ScheduleCheckLeader()
				c.revokeTSORequest(errors.WithStack(err), tbc.tsoRequestCh)
				retryTimeConsuming = 0
				continue
			}
			select {
			case <-dispatcherCtx.Done():
				return
			case <-time.After(time.Second):
				retryTimeConsuming += time.Second
				continue
			}
		}
		retryTimeConsuming = 0
		// Start to collect the TSO requests.
		maxBatchWaitInterval := c.option.getMaxTSOBatchWaitInterval()
		if err = tbc.fetchPendingRequests(dispatcherCtx, maxBatchWaitInterval); err != nil {
			log.Error("[pd] fetch pending tso requests error", zap.String("dc-location", dc), errs.ZapError(errs.ErrClientGetTSO, err))
			return
		}
		if maxBatchWaitInterval >= 0 {
			tbc.adjustBestBatchSize()
		}
		done := make(chan struct{})
		dl := deadline{
			timer:  time.After(c.option.timeout),
			done:   done,
			cancel: cancel,
		}
		tsDeadlineCh, ok := c.tsDeadline.Load(dc)
		for !ok || tsDeadlineCh == nil {
			c.scheduleCheckTSDeadline()
			time.Sleep(time.Millisecond * 100)
			tsDeadlineCh, ok = c.tsDeadline.Load(dc)
		}
		select {
		case <-dispatcherCtx.Done():
			return
		case tsDeadlineCh.(chan deadline) <- dl:
		}
		opts = extractSpanReference(tbc, opts[:0])
		err = c.processTSORequests(stream, dc, tbc, opts)
		close(done)
		// If error happens during tso stream handling, reset stream and run the next trial.
		if err != nil {
			select {
			case <-dispatcherCtx.Done():
				return
			default:
			}
			c.ScheduleCheckLeader()
			log.Error("[pd] getTS error", zap.String("dc-location", dc), errs.ZapError(errs.ErrClientGetTSO, err))
			cancel()
			// Set `stream` to nil and remove this stream from the `connectionCtxs`.
			stream = nil
			connectionCtxs.Delete(streamAddr)
			// Because ScheduleCheckLeader is asynchronous, if the leader changes, we better call `updateMember` ASAP.
			if IsLeaderChange(err) {
				if err := c.updateMember(); err != nil {
					select {
					case <-dispatcherCtx.Done():
						return
					default:
					}
				}
				// Because the TSO Follower Proxy could be configured online,
				// If we change it from on -> off, background updateConnectionCtxs
				// will cancel the current stream, then the EOF error caused by cancel()
				// should not trigger the updateConnectionCtxs here.
				// So we should only call it when the leader changes.
				c.updateConnectionCtxs(dispatcherCtx, dc, &connectionCtxs)
			}
		}
	}
}

// TSO Follower Proxy only supports the Global TSO proxy now.
func (c *client) allowTSOFollowerProxy(dc string) bool {
	return dc == globalDCLocation && c.option.getEnableTSOFollowerProxy()
}

// chooseStream uses the reservoir sampling algorithm to randomly choose a connection.
// connectionCtxs will only have only one stream to choose when the TSO Follower Proxy is off.
func (c *client) chooseStream(connectionCtxs *sync.Map) (connectionCtx *connectionContext) {
	idx := 0
	connectionCtxs.Range(func(addr, cc interface{}) bool {
		j := rand.Intn(idx + 1)
		if j < 1 {
			connectionCtx = cc.(*connectionContext)
		}
		idx++
		return true
	})
	return connectionCtx
}

type connectionContext struct {
	streamAddr string
	// Current stream to send gRPC requests, maybe a leader or a follower.
	stream pdpb.PD_TsoClient
	cancel context.CancelFunc
}

func (c *client) updateConnectionCtxs(updaterCtx context.Context, dc string, connectionCtxs *sync.Map) {
	// Normal connection creating, it will be affected by the `enableForwarding`.
	createTSOConnection := c.tryConnect
	if c.allowTSOFollowerProxy(dc) {
		createTSOConnection = c.tryConnectWithProxy
	}
	if err := createTSOConnection(updaterCtx, dc, connectionCtxs); err != nil {
		log.Error("[pd] update connection contexts failed", zap.String("dc", dc), errs.ZapError(err))
	}
}

// tryConnect will try to connect to the TSO allocator leader. If the connection becomes unreachable
// and enableForwarding is true, it will create a new connection to a follower to do the forwarding,
// while a new daemon will be created also to switch back to a normal leader connection ASAP the
// connection comes back to normal.
func (c *client) tryConnect(
	dispatcherCtx context.Context,
	dc string,
	connectionCtxs *sync.Map,
) error {
	var (
		networkErrNum uint64
		err           error
		stream        pdpb.PD_TsoClient
	)
	updateAndClear := func(newAddr string, connectionCtx *connectionContext) {
		if cc, loaded := connectionCtxs.LoadOrStore(newAddr, connectionCtx); loaded {
			// If the previous connection still exists, we should close it first.
			cc.(*connectionContext).cancel()
			connectionCtxs.Store(newAddr, connectionCtx)
		}
		connectionCtxs.Range(func(addr, cc interface{}) bool {
			if addr.(string) != newAddr {
				cc.(*connectionContext).cancel()
				connectionCtxs.Delete(addr)
			}
			return true
		})
	}
	cc, url := c.getAllocatorClientConnByDCLocation(dc)
	// retry several times before falling back to the follower when the network problem happens
	for i := 0; i < maxRetryTimes; i++ {
		cctx, cancel := context.WithCancel(dispatcherCtx)
		stream, err = c.createTsoStream(cctx, cancel, pdpb.NewPDClient(cc))
		failpoint.Inject("unreachableNetwork", func() {
			stream = nil
			err = status.New(codes.Unavailable, "unavailable").Err()
		})
		if stream != nil && err == nil {
			updateAndClear(url, &connectionContext{url, stream, cancel})
			return nil
		}

		if err != nil && c.option.enableForwarding {
			// The reason we need to judge if the error code is equal to "Canceled" here is that
			// when we create a stream we use a goroutine to manually control the timeout of the connection.
			// There is no need to wait for the transport layer timeout which can reduce the time of unavailability.
			// But it conflicts with the retry mechanism since we use the error code to decide if it is caused by network error.
			// And actually the `Canceled` error can be regarded as a kind of network error in some way.
			if rpcErr, ok := status.FromError(err); ok && (isNetworkError(rpcErr.Code()) || rpcErr.Code() == codes.Canceled) {
				networkErrNum++
			}
		}

		cancel()
		select {
		case <-dispatcherCtx.Done():
			return err
		case <-time.After(retryInterval):
		}
	}

	if networkErrNum == maxRetryTimes {
		// encounter the network error
		followerClient, addr := c.followerClient()
		if followerClient != nil {
			log.Info("[pd] fall back to use follower to forward tso stream", zap.String("dc", dc), zap.String("addr", addr))
			forwardedHost, ok := c.getAllocatorLeaderAddrByDCLocation(dc)
			if !ok {
				return errors.Errorf("cannot find the allocator leader in %s", dc)
			}

			// create the follower stream
			cctx, cancel := context.WithCancel(dispatcherCtx)
			cctx = grpcutil.BuildForwardContext(cctx, forwardedHost)
			stream, err = c.createTsoStream(cctx, cancel, followerClient)
			if err == nil {
				forwardedHostTrim := trimHTTPPrefix(forwardedHost)
				addrTrim := trimHTTPPrefix(addr)
				// the goroutine is used to check the network and change back to the original stream
				go c.checkAllocator(dispatcherCtx, cancel, dc, forwardedHostTrim, addrTrim, url, updateAndClear)
				requestForwarded.WithLabelValues(forwardedHostTrim, addrTrim).Set(1)
				updateAndClear(addr, &connectionContext{addr, stream, cancel})
				return nil
			}
			cancel()
		}
	}
	return err
}

// tryConnectWithProxy will create multiple streams to all the PD servers to work as a TSO proxy to reduce
// the pressure of PD leader.
func (c *client) tryConnectWithProxy(
	dispatcherCtx context.Context,
	dc string,
	connectionCtxs *sync.Map,
) error {
	clients := c.getAllClients()
	leaderAddr := c.GetLeaderAddr()
	forwardedHost, ok := c.getAllocatorLeaderAddrByDCLocation(dc)
	if !ok {
		return errors.Errorf("cannot find the allocator leader in %s", dc)
	}
	// GC the stale one.
	connectionCtxs.Range(func(addr, cc interface{}) bool {
		if _, ok := clients[addr.(string)]; !ok {
			cc.(*connectionContext).cancel()
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
		stream, err := c.createTsoStream(cctx, cancel, client)
		if err == nil {
			if addr != leaderAddr {
				forwardedHostTrim := trimHTTPPrefix(forwardedHost)
				addrTrim := trimHTTPPrefix(addr)
				requestForwarded.WithLabelValues(forwardedHostTrim, addrTrim).Set(1)
			}
			connectionCtxs.Store(addr, &connectionContext{addr, stream, cancel})
			continue
		}
		log.Error("[pd] create the tso stream failed", zap.String("dc", dc), zap.String("addr", addr), errs.ZapError(err))
		cancel()
	}
	return nil
}

func extractSpanReference(tbc *tsoBatchController, opts []opentracing.StartSpanOption) []opentracing.StartSpanOption {
	for _, req := range tbc.collectedRequests[:tbc.collectedRequestCount] {
		if span := opentracing.SpanFromContext(req.requestCtx); span != nil {
			opts = append(opts, opentracing.ChildOf(span.Context()))
		}
	}
	return opts
}

func (c *client) processTSORequests(stream pdpb.PD_TsoClient, dcLocation string, tbc *tsoBatchController, opts []opentracing.StartSpanOption) error {
	if len(opts) > 0 {
		span := opentracing.StartSpan("pdclient.processTSORequests", opts...)
		defer span.Finish()
	}
	start := time.Now()
	requests := tbc.collectedRequests[:tbc.collectedRequestCount]
	count := int64(len(requests))
	req := &pdpb.TsoRequest{
		Header:     c.requestHeader(),
		Count:      uint32(count),
		DcLocation: dcLocation,
	}

	if err := stream.Send(req); err != nil {
		err = errors.WithStack(err)
		c.finishTSORequest(requests, 0, 0, 0, err)
		return err
	}
	tsoBatchSendLatency.Observe(float64(time.Since(tbc.batchStartTime)))
	resp, err := stream.Recv()
	if err != nil {
		err = errors.WithStack(err)
		c.finishTSORequest(requests, 0, 0, 0, err)
		return err
	}
	requestDurationTSO.Observe(time.Since(start).Seconds())
	tsoBatchSize.Observe(float64(count))

	if resp.GetCount() != uint32(count) {
		err = errors.WithStack(errTSOLength)
		c.finishTSORequest(requests, 0, 0, 0, err)
		return err
	}

	physical, logical, suffixBits := resp.GetTimestamp().GetPhysical(), resp.GetTimestamp().GetLogical(), resp.GetTimestamp().GetSuffixBits()
	// `logical` is the largest ts's logical part here, we need to do the subtracting before we finish each TSO request.
	firstLogical := addLogical(logical, -count+1, suffixBits)
	c.compareAndSwapTS(dcLocation, physical, firstLogical, suffixBits, count)
	c.finishTSORequest(requests, physical, firstLogical, suffixBits, nil)
	return nil
}

// Because of the suffix, we need to shift the count before we add it to the logical part.
func addLogical(logical, count int64, suffixBits uint32) int64 {
	return logical + count<<suffixBits
}

func (c *client) compareAndSwapTS(dcLocation string, physical, firstLogical int64, suffixBits uint32, count int64) {
	largestLogical := addLogical(firstLogical, count-1, suffixBits)
	lastTSOInterface, loaded := c.lastTSMap.LoadOrStore(dcLocation, &lastTSO{
		physical: physical,
		// Save the largest logical part here
		logical: largestLogical,
	})
	if !loaded {
		return
	}
	lastTSOPointer := lastTSOInterface.(*lastTSO)
	lastPhysical := lastTSOPointer.physical
	lastLogical := lastTSOPointer.logical
	// The TSO we get is a range like [largestLogical-count+1, largestLogical], so we save the last TSO's largest logical to compare with the new TSO's first logical.
	// For example, if we have a TSO resp with logical 10, count 5, then all TSOs we get will be [6, 7, 8, 9, 10].
	if tsLessEqual(physical, firstLogical, lastPhysical, lastLogical) {
		panic(errors.Errorf("%s timestamp fallback, newly acquired ts (%d, %d) is less or equal to last one (%d, %d)",
			dcLocation, physical, firstLogical, lastPhysical, lastLogical))
	}
	lastTSOPointer.physical = physical
	// Same as above, we save the largest logical part here.
	lastTSOPointer.logical = largestLogical
}

func tsLessEqual(physical, logical, thatPhysical, thatLogical int64) bool {
	if physical == thatPhysical {
		return logical <= thatLogical
	}
	return physical < thatPhysical
}

func (c *client) finishTSORequest(requests []*tsoRequest, physical, firstLogical int64, suffixBits uint32, err error) {
	for i := 0; i < len(requests); i++ {
		if span := opentracing.SpanFromContext(requests[i].requestCtx); span != nil {
			span.Finish()
		}
		requests[i].physical, requests[i].logical = physical, addLogical(firstLogical, int64(i), suffixBits)
		requests[i].done <- err
	}
}

func (c *client) revokeTSORequest(err error, tsoDispatcher <-chan *tsoRequest) {
	for i := 0; i < len(tsoDispatcher); i++ {
		req := <-tsoDispatcher
		req.done <- err
	}
}

func (c *client) Close() {
	c.cancel()
	c.wg.Wait()

	c.tsoDispatcher.Range(func(_, dispatcher interface{}) bool {
		if dispatcher != nil {
			c.revokeTSORequest(errors.WithStack(errClosing), dispatcher.(*tsoDispatcher).tsoBatchController.tsoRequestCh)
			dispatcher.(*tsoDispatcher).dispatcherCancel()
		}
		return true
	})

	c.clientConns.Range(func(_, cc interface{}) bool {
		if err := cc.(*grpc.ClientConn).Close(); err != nil {
			log.Error("[pd] failed to close gRPC clientConn", errs.ZapError(errs.ErrCloseGRPCConn, err))
		}
		return true
	})
}

// leaderClient gets the client of current PD leader.
func (c *client) leaderClient() pdpb.PDClient {
	if cc, ok := c.clientConns.Load(c.GetLeaderAddr()); ok {
		return pdpb.NewPDClient(cc.(*grpc.ClientConn))
	}
	return nil
}

// followerClient gets a client of the current reachable and healthy PD follower randomly.
func (c *client) followerClient() (pdpb.PDClient, string) {
	addrs := c.GetFollowerAddrs()
	if len(addrs) < 1 {
		return nil, ""
	}
	var (
		cc  *grpc.ClientConn
		err error
	)
	for i := 0; i < len(addrs); i++ {
		addr := addrs[rand.Intn(len(addrs))]
		if cc, err = c.getOrCreateGRPCConn(addr); err != nil {
			continue
		}
		healthCtx, healthCancel := context.WithTimeout(c.ctx, c.option.timeout)
		resp, err := healthpb.NewHealthClient(cc).Check(healthCtx, &healthpb.HealthCheckRequest{Service: ""})
		healthCancel()
		if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			return pdpb.NewPDClient(cc), addr
		}
	}
	return nil, ""
}

func (c *client) getClient() pdpb.PDClient {
	if c.option.enableForwarding && atomic.LoadInt32(&c.leaderNetworkFailure) == 1 {
		followerClient, addr := c.followerClient()
		if followerClient != nil {
			log.Debug("[pd] use follower client", zap.String("addr", addr))
			return followerClient
		}
	}
	return c.leaderClient()
}

func (c *client) getAllClients() map[string]pdpb.PDClient {
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
		if cc, err = c.getOrCreateGRPCConn(addr); err != nil {
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

var tsoReqPool = sync.Pool{
	New: func() interface{} {
		return &tsoRequest{
			done:     make(chan error, 1),
			physical: 0,
			logical:  0,
		}
	},
}

func (c *client) GetTSAsync(ctx context.Context) TSFuture {
	return c.GetLocalTSAsync(ctx, globalDCLocation)
}

func (c *client) GetLocalTSAsync(ctx context.Context, dcLocation string) TSFuture {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("GetLocalTSAsync", opentracing.ChildOf(span.Context()))
		ctx = opentracing.ContextWithSpan(ctx, span)
	}
	req := tsoReqPool.Get().(*tsoRequest)
	req.requestCtx = ctx
	req.clientCtx = c.ctx
	req.start = time.Now()
	req.dcLocation = dcLocation
	if err := c.dispatchRequest(dcLocation, req); err != nil {
		// Wait for a while and try again
		time.Sleep(50 * time.Millisecond)
		if err = c.dispatchRequest(dcLocation, req); err != nil {
			req.done <- err
		}
	}
	return req
}

func (c *client) dispatchRequest(dcLocation string, request *tsoRequest) error {
	dispatcher, ok := c.tsoDispatcher.Load(dcLocation)
	if !ok {
		err := errs.ErrClientGetTSO.FastGenByArgs(fmt.Sprintf("unknown dc-location %s to the client", dcLocation))
		log.Error("[pd] dispatch tso request error", zap.String("dc-location", dcLocation), errs.ZapError(err))
		c.ScheduleCheckLeader()
		return err
	}
	dispatcher.(*tsoDispatcher).tsoBatchController.tsoRequestCh <- request
	return nil
}

// TSFuture is a future which promises to return a TSO.
type TSFuture interface {
	// Wait gets the physical and logical time, it would block caller if data is not available yet.
	Wait() (int64, int64, error)
}

func (req *tsoRequest) Wait() (physical int64, logical int64, err error) {
	// If tso command duration is observed very high, the reason could be it
	// takes too long for Wait() be called.
	start := time.Now()
	cmdDurationTSOAsyncWait.Observe(start.Sub(req.start).Seconds())
	select {
	case err = <-req.done:
		err = errors.WithStack(err)
		defer tsoReqPool.Put(req)
		if err != nil {
			cmdFailDurationTSO.Observe(time.Since(req.start).Seconds())
			return 0, 0, err
		}
		physical, logical = req.physical, req.logical
		now := time.Now()
		cmdDurationWait.Observe(now.Sub(start).Seconds())
		cmdDurationTSO.Observe(now.Sub(req.start).Seconds())
		return
	case <-req.requestCtx.Done():
		return 0, 0, errors.WithStack(req.requestCtx.Err())
	case <-req.clientCtx.Done():
		return 0, 0, errors.WithStack(req.clientCtx.Err())
	}
}

func (c *client) GetTS(ctx context.Context) (physical int64, logical int64, err error) {
	resp := c.GetTSAsync(ctx)
	return resp.Wait()
}

func (c *client) GetLocalTS(ctx context.Context, dcLocation string) (physical int64, logical int64, err error) {
	resp := c.GetLocalTSAsync(ctx, dcLocation)
	return resp.Wait()
}

func handleRegionResponse(res *pdpb.GetRegionResponse) *Region {
	if res.Region == nil {
		return nil
	}

	r := &Region{
		Meta:         res.Region,
		Leader:       res.Leader,
		PendingPeers: res.PendingPeers,
	}
	for _, s := range res.DownPeers {
		r.DownPeers = append(r.DownPeers, s.Peer)
	}
	return r
}

func (c *client) GetRegion(ctx context.Context, key []byte) (*Region, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.GetRegion", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationGetRegion.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &pdpb.GetRegionRequest{
		Header:    c.requestHeader(),
		RegionKey: key,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	resp, err := c.getClient().GetRegion(ctx, req)
	cancel()

	if err != nil {
		cmdFailDurationGetRegion.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return nil, errors.WithStack(err)
	}
	return handleRegionResponse(resp), nil
}

func isNetworkError(code codes.Code) bool {
	return code == codes.Unavailable || code == codes.DeadlineExceeded
}

func (c *client) GetRegionFromMember(ctx context.Context, key []byte, memberURLs []string) (*Region, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.GetRegionFromMember", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationGetRegion.Observe(time.Since(start).Seconds()) }()

	var resp *pdpb.GetRegionResponse
	for _, url := range memberURLs {
		conn, err := c.getOrCreateGRPCConn(url)
		if err != nil {
			log.Error("[pd] can't get grpc connection", zap.String("member-URL", url), errs.ZapError(err))
			continue
		}
		cc := pdpb.NewPDClient(conn)
		resp, err = cc.GetRegion(ctx, &pdpb.GetRegionRequest{
			Header:    c.requestHeader(),
			RegionKey: key,
		})
		if err != nil {
			log.Error("[pd] can't get region info", zap.String("member-URL", url), errs.ZapError(err))
			continue
		}
		if resp != nil {
			break
		}
	}

	if resp == nil {
		cmdFailDurationGetRegion.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		errorMsg := fmt.Sprintf("[pd] can't get region info from member URLs: %+v", memberURLs)
		return nil, errors.WithStack(errors.New(errorMsg))
	}
	return handleRegionResponse(resp), nil
}

func (c *client) GetPrevRegion(ctx context.Context, key []byte) (*Region, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.GetPrevRegion", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationGetPrevRegion.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &pdpb.GetRegionRequest{
		Header:    c.requestHeader(),
		RegionKey: key,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	resp, err := c.getClient().GetPrevRegion(ctx, req)
	cancel()

	if err != nil {
		cmdFailDurationGetPrevRegion.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return nil, errors.WithStack(err)
	}
	return handleRegionResponse(resp), nil
}

func (c *client) GetRegionByID(ctx context.Context, regionID uint64) (*Region, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.GetRegionByID", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationGetRegionByID.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &pdpb.GetRegionByIDRequest{
		Header:   c.requestHeader(),
		RegionId: regionID,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	resp, err := c.getClient().GetRegionByID(ctx, req)
	cancel()

	if err != nil {
		cmdFailedDurationGetRegionByID.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return nil, errors.WithStack(err)
	}
	return handleRegionResponse(resp), nil
}

func (c *client) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*Region, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.ScanRegions", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer cmdDurationScanRegions.Observe(time.Since(start).Seconds())

	var cancel context.CancelFunc
	scanCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		scanCtx, cancel = context.WithTimeout(ctx, c.option.timeout)
		defer cancel()
	}
	req := &pdpb.ScanRegionsRequest{
		Header:   c.requestHeader(),
		StartKey: key,
		EndKey:   endKey,
		Limit:    int32(limit),
	}
	scanCtx = grpcutil.BuildForwardContext(scanCtx, c.GetLeaderAddr())
	resp, err := c.getClient().ScanRegions(scanCtx, req)

	if err != nil {
		cmdFailedDurationScanRegions.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return nil, errors.WithStack(err)
	}

	return handleRegionsResponse(resp), nil
}

func handleRegionsResponse(resp *pdpb.ScanRegionsResponse) []*Region {
	var regions []*Region
	if len(resp.GetRegions()) == 0 {
		// Make it compatible with old server.
		metas, leaders := resp.GetRegionMetas(), resp.GetLeaders()
		for i := range metas {
			r := &Region{Meta: metas[i]}
			if i < len(leaders) {
				r.Leader = leaders[i]
			}
			regions = append(regions, r)
		}
	} else {
		for _, r := range resp.GetRegions() {
			region := &Region{
				Meta:         r.Region,
				Leader:       r.Leader,
				PendingPeers: r.PendingPeers,
			}
			for _, p := range r.DownPeers {
				region.DownPeers = append(region.DownPeers, p.Peer)
			}
			regions = append(regions, region)
		}
	}
	return regions
}

func (c *client) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.GetStore", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationGetStore.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &pdpb.GetStoreRequest{
		Header:  c.requestHeader(),
		StoreId: storeID,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	resp, err := c.getClient().GetStore(ctx, req)
	cancel()

	if err != nil {
		cmdFailedDurationGetStore.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return nil, errors.WithStack(err)
	}
	return handleStoreResponse(resp)
}

func handleStoreResponse(resp *pdpb.GetStoreResponse) (*metapb.Store, error) {
	store := resp.GetStore()
	if store == nil {
		return nil, errors.New("[pd] store field in rpc response not set")
	}
	if store.GetState() == metapb.StoreState_Tombstone {
		return nil, nil
	}
	return store, nil
}

func (c *client) GetAllStores(ctx context.Context, opts ...GetStoreOption) ([]*metapb.Store, error) {
	// Applies options
	options := &GetStoreOp{}
	for _, opt := range opts {
		opt(options)
	}

	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.GetAllStores", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationGetAllStores.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &pdpb.GetAllStoresRequest{
		Header:                 c.requestHeader(),
		ExcludeTombstoneStores: options.excludeTombstone,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	resp, err := c.getClient().GetAllStores(ctx, req)
	cancel()

	if err != nil {
		cmdFailedDurationGetAllStores.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return nil, errors.WithStack(err)
	}
	return resp.GetStores(), nil
}

func (c *client) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.UpdateGCSafePoint", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationUpdateGCSafePoint.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &pdpb.UpdateGCSafePointRequest{
		Header:    c.requestHeader(),
		SafePoint: safePoint,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	resp, err := c.getClient().UpdateGCSafePoint(ctx, req)
	cancel()

	if err != nil {
		cmdFailedDurationUpdateGCSafePoint.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return 0, errors.WithStack(err)
	}
	return resp.GetNewSafePoint(), nil
}

// UpdateServiceGCSafePoint updates the safepoint for specific service and
// returns the minimum safepoint across all services, this value is used to
// determine the safepoint for multiple services, it does not trigger a GC
// job. Use UpdateGCSafePoint to trigger the GC job if needed.
func (c *client) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.UpdateServiceGCSafePoint", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}

	start := time.Now()
	defer func() { cmdDurationUpdateServiceGCSafePoint.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &pdpb.UpdateServiceGCSafePointRequest{
		Header:    c.requestHeader(),
		ServiceId: []byte(serviceID),
		TTL:       ttl,
		SafePoint: safePoint,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	resp, err := c.getClient().UpdateServiceGCSafePoint(ctx, req)
	cancel()

	if err != nil {
		cmdFailedDurationUpdateServiceGCSafePoint.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return 0, errors.WithStack(err)
	}
	return resp.GetMinSafePoint(), nil
}

func (c *client) ScatterRegion(ctx context.Context, regionID uint64) error {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.ScatterRegion", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	return c.scatterRegionsWithGroup(ctx, regionID, "")
}

func (c *client) scatterRegionsWithGroup(ctx context.Context, regionID uint64, group string) error {
	start := time.Now()
	defer func() { cmdDurationScatterRegion.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &pdpb.ScatterRegionRequest{
		Header:   c.requestHeader(),
		RegionId: regionID,
		Group:    group,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	resp, err := c.getClient().ScatterRegion(ctx, req)
	cancel()
	if err != nil {
		return err
	}
	if resp.Header.GetError() != nil {
		return errors.Errorf("scatter region %d failed: %s", regionID, resp.Header.GetError().String())
	}
	return nil
}

func (c *client) ScatterRegions(ctx context.Context, regionsID []uint64, opts ...RegionsOption) (*pdpb.ScatterRegionResponse, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.ScatterRegions", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	return c.scatterRegionsWithOptions(ctx, regionsID, opts...)
}

func (c *client) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.GetOperator", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationGetOperator.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	defer cancel()
	req := &pdpb.GetOperatorRequest{
		Header:   c.requestHeader(),
		RegionId: regionID,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	return c.getClient().GetOperator(ctx, req)
}

// SplitRegions split regions by given split keys
func (c *client) SplitRegions(ctx context.Context, splitKeys [][]byte, opts ...RegionsOption) (*pdpb.SplitRegionsResponse, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.SplitRegions", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationSplitRegions.Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	defer cancel()
	options := &RegionsOp{}
	for _, opt := range opts {
		opt(options)
	}
	req := &pdpb.SplitRegionsRequest{
		Header:     c.requestHeader(),
		SplitKeys:  splitKeys,
		RetryLimit: options.retryLimit,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	return c.getClient().SplitRegions(ctx, req)
}

func (c *client) requestHeader() *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: c.clusterID,
	}
}

func (c *client) scatterRegionsWithOptions(ctx context.Context, regionsID []uint64, opts ...RegionsOption) (*pdpb.ScatterRegionResponse, error) {
	start := time.Now()
	defer func() { cmdDurationScatterRegions.Observe(time.Since(start).Seconds()) }()
	options := &RegionsOp{}
	for _, opt := range opts {
		opt(options)
	}
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &pdpb.ScatterRegionRequest{
		Header:     c.requestHeader(),
		Group:      options.group,
		RegionsId:  regionsID,
		RetryLimit: options.retryLimit,
	}

	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	resp, err := c.getClient().ScatterRegion(ctx, req)
	cancel()

	if err != nil {
		return nil, err
	}
	if resp.Header.GetError() != nil {
		return nil, errors.Errorf("scatter regions %v failed: %s", regionsID, resp.Header.GetError().String())
	}
	return resp, nil
}

func addrsToUrls(addrs []string) []string {
	// Add default schema "http://" to addrs.
	urls := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if strings.Contains(addr, "://") {
			urls = append(urls, addr)
		} else {
			urls = append(urls, "http://"+addr)
		}
	}
	return urls
}

// IsLeaderChange will determine whether there is a leader change.
func IsLeaderChange(err error) bool {
	errMsg := err.Error()
	return strings.Contains(errMsg, errs.NotLeaderErr) || strings.Contains(errMsg, errs.MismatchLeaderErr)
}

func trimHTTPPrefix(str string) string {
	str = strings.TrimPrefix(str, "http://")
	str = strings.TrimPrefix(str, "https://")
	return str
}
