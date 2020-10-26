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
// See the License for the specific language governing permissions and
// limitations under the License.

package pd

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"go.uber.org/zap"
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
	ScatterRegion(ctx context.Context, regionID uint64) error
	// ScatterRegionWithOption scatters the specified region with the given options, should use it
	// for a batch of regions.
	ScatterRegionWithOption(ctx context.Context, regionID uint64, opts ...ScatterRegionOption) error
	// GetOperator gets the status of operator of the specified region.
	GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error)
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

// ScatterRegionOp represents available options when scatter regions
type ScatterRegionOp struct {
	group string
}

// ScatterRegionOption configures ScatterRegionOp
type ScatterRegionOption func(op *ScatterRegionOp)

// WithGroup specify the group during ScatterRegion
func WithGroup(group string) ScatterRegionOption {
	return func(op *ScatterRegionOp) { op.group = group }
}

type tsoRequest struct {
	start    time.Time
	ctx      context.Context
	done     chan error
	physical int64
	logical  int64
}

const (
	defaultPDTimeout      = 3 * time.Second
	dialTimeout           = 3 * time.Second
	updateLeaderTimeout   = time.Second // Use a shorter timeout to recover faster from network isolation.
	maxMergeTSORequests   = 10000       // should be higher if client is sending requests in burst
	maxInitClusterRetries = 100
)

var (
	// errFailInitClusterID is returned when failed to load clusterID from all supplied PD addresses.
	errFailInitClusterID = errors.New("[pd] failed to get cluster id")
	// errClosing is returned when request is canceled when client is closing.
	errClosing = errors.New("[pd] closing")
	// errTSOLength is returned when the number of response timestamps is inconsistent with request.
	errTSOLength = errors.New("[pd] tso length in rpc response is incorrect")
)

type client struct {
	*baseClient
	tsoRequests chan *tsoRequest

	lastPhysical int64
	lastLogical  int64

	tsDeadlineCh chan deadline
}

// NewClient creates a PD client.
func NewClient(pdAddrs []string, security SecurityOption, opts ...ClientOption) (Client, error) {
	return NewClientWithContext(context.Background(), pdAddrs, security, opts...)
}

// NewClientWithContext creates a PD client with context.
func NewClientWithContext(ctx context.Context, pdAddrs []string, security SecurityOption, opts ...ClientOption) (Client, error) {
	log.Info("[pd] create pd client with endpoints", zap.Strings("pd-address", pdAddrs))
	base, err := newBaseClient(ctx, addrsToUrls(pdAddrs), security, opts...)
	if err != nil {
		return nil, err
	}
	c := &client{
		baseClient:   base,
		tsoRequests:  make(chan *tsoRequest, maxMergeTSORequests),
		tsDeadlineCh: make(chan deadline, 1),
	}

	c.wg.Add(2)
	go c.tsLoop()
	go c.tsCancelLoop()

	return c, nil
}

type deadline struct {
	timer  <-chan time.Time
	done   chan struct{}
	cancel context.CancelFunc
}

func (c *client) tsCancelLoop() {
	defer c.wg.Done()

	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()

	for {
		select {
		case d := <-c.tsDeadlineCh:
			select {
			case <-d.timer:
				log.Error("tso request is canceled due to timeout", errs.ZapError(errs.ErrClientGetTSOTimeout))
				d.cancel()
			case <-d.done:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *client) checkStreamTimeout(loopCtx context.Context, cancel context.CancelFunc, createdCh chan struct{}) {
	select {
	case <-time.After(c.timeout):
		cancel()
	case <-createdCh:
		return
	case <-loopCtx.Done():
		return
	}
}

func (c *client) GetAllMembers(ctx context.Context) ([]*pdpb.Member, error) {
	start := time.Now()
	defer func() { cmdDurationGetAllMembers.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	resp, err := c.leaderClient().GetMembers(ctx, &pdpb.GetMembersRequest{
		Header: c.requestHeader(),
	})
	cancel()
	if err != nil {
		cmdFailDurationGetAllMembers.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return nil, errors.WithStack(err)
	}
	members := resp.GetMembers()
	return members, nil
}

func (c *client) tsLoop() {
	defer c.wg.Done()

	loopCtx, loopCancel := context.WithCancel(c.ctx)
	defer loopCancel()

	defaultSize := maxMergeTSORequests + 1
	requests := make([]*tsoRequest, defaultSize)
	createdCh := make(chan struct{})

	var opts []opentracing.StartSpanOption
	var stream pdpb.PD_TsoClient
	var cancel context.CancelFunc

	for {
		var err error

		if stream == nil {
			var ctx context.Context
			ctx, cancel = context.WithCancel(loopCtx)
			go c.checkStreamTimeout(loopCtx, cancel, createdCh)
			stream, err = c.leaderClient().Tso(ctx)
			if stream != nil {
				createdCh <- struct{}{}
			}
			if err != nil {
				select {
				case <-loopCtx.Done():
					cancel()
					return
				default:
				}
				log.Error("[pd] create tso stream error", errs.ZapError(errs.ErrClientCreateTSOStream, err))
				c.ScheduleCheckLeader()
				cancel()
				c.revokeTSORequest(errors.WithStack(err))
				select {
				case <-time.After(time.Second):
				case <-loopCtx.Done():
					return
				}
				continue
			}
		}

		select {
		case first := <-c.tsoRequests:
			pendingPlus1 := len(c.tsoRequests) + 1
			requests[0] = first
			for i := 1; i < pendingPlus1; i++ {
				requests[i] = <-c.tsoRequests
			}
			done := make(chan struct{})
			dl := deadline{
				timer:  time.After(c.timeout),
				done:   done,
				cancel: cancel,
			}
			select {
			case c.tsDeadlineCh <- dl:
			case <-loopCtx.Done():
				cancel()
				return
			}
			opts = extractSpanReference(requests[:pendingPlus1], opts[:0])
			err = c.processTSORequests(stream, requests[:pendingPlus1], opts)
			close(done)
		case <-loopCtx.Done():
			cancel()
			return
		}

		if err != nil {
			select {
			case <-loopCtx.Done():
				cancel()
				return
			default:
			}
			log.Error("[pd] getTS error", errs.ZapError(errs.ErrClientGetTSO, err))
			c.ScheduleCheckLeader()
			cancel()
			stream, cancel = nil, nil
		}
	}
}

func extractSpanReference(requests []*tsoRequest, opts []opentracing.StartSpanOption) []opentracing.StartSpanOption {
	for _, req := range requests {
		if span := opentracing.SpanFromContext(req.ctx); span != nil {
			opts = append(opts, opentracing.ChildOf(span.Context()))
		}
	}
	return opts
}

func (c *client) processTSORequests(stream pdpb.PD_TsoClient, requests []*tsoRequest, opts []opentracing.StartSpanOption) error {
	if len(opts) > 0 {
		span := opentracing.StartSpan("pdclient.processTSORequests", opts...)
		defer span.Finish()
	}
	count := len(requests)
	start := time.Now()
	req := &pdpb.TsoRequest{
		Header: c.requestHeader(),
		Count:  uint32(count),
	}

	if err := stream.Send(req); err != nil {
		err = errors.WithStack(err)
		c.finishTSORequest(requests, 0, 0, err)
		return err
	}
	resp, err := stream.Recv()
	if err != nil {
		err = errors.WithStack(err)
		c.finishTSORequest(requests, 0, 0, err)
		return err
	}
	requestDurationTSO.Observe(time.Since(start).Seconds())
	tsoBatchSize.Observe(float64(count))

	if resp.GetCount() != uint32(len(requests)) {
		err = errors.WithStack(errTSOLength)
		c.finishTSORequest(requests, 0, 0, err)
		return err
	}

	physical, logical := resp.GetTimestamp().GetPhysical(), resp.GetTimestamp().GetLogical()
	// Server returns the highest ts.
	logical -= int64(resp.GetCount() - 1)
	if tsLessEqual(physical, logical, c.lastPhysical, c.lastLogical) {
		panic(errors.Errorf("timestamp fallback, newly acquired ts (%d, %d) is less or equal to last one (%d, %d)",
			physical, logical, c.lastPhysical, c.lastLogical))
	}
	c.lastPhysical = physical
	c.lastLogical = logical + int64(len(requests)) - 1
	c.finishTSORequest(requests, physical, logical, nil)
	return nil
}

func tsLessEqual(physical, logical, thatPhysical, thatLogical int64) bool {
	if physical == thatPhysical {
		return logical <= thatLogical
	}
	return physical < thatPhysical
}

func (c *client) finishTSORequest(requests []*tsoRequest, physical, firstLogical int64, err error) {
	for i := 0; i < len(requests); i++ {
		if span := opentracing.SpanFromContext(requests[i].ctx); span != nil {
			span.Finish()
		}
		requests[i].physical, requests[i].logical = physical, firstLogical+int64(i)
		requests[i].done <- err
	}
}

func (c *client) revokeTSORequest(err error) {
	n := len(c.tsoRequests)
	for i := 0; i < n; i++ {
		req := <-c.tsoRequests
		req.done <- err
	}
}

func (c *client) Close() {
	c.cancel()
	c.wg.Wait()

	c.revokeTSORequest(errors.WithStack(errClosing))

	c.connMu.Lock()
	defer c.connMu.Unlock()
	for _, cc := range c.connMu.clientConns {
		if err := cc.Close(); err != nil {
			log.Error("[pd] failed to close gRPC clientConn", errs.ZapError(errs.ErrCloseGRPCConn, err))
		}
	}
}

// leaderClient gets the client of current PD leader.
func (c *client) leaderClient() pdpb.PDClient {
	c.connMu.RLock()
	defer c.connMu.RUnlock()

	return pdpb.NewPDClient(c.connMu.clientConns[c.connMu.leader])
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
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("GetTSAsync", opentracing.ChildOf(span.Context()))
		ctx = opentracing.ContextWithSpan(ctx, span)
	}
	req := tsoReqPool.Get().(*tsoRequest)
	req.ctx = ctx
	req.start = time.Now()
	c.tsoRequests <- req

	return req
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
	case <-req.ctx.Done():
		return 0, 0, errors.WithStack(req.ctx.Err())
	}
}

func (c *client) GetTS(ctx context.Context) (physical int64, logical int64, err error) {
	resp := c.GetTSAsync(ctx)
	return resp.Wait()
}

func (c *client) parseRegionResponse(res *pdpb.GetRegionResponse) *Region {
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

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	resp, err := c.leaderClient().GetRegion(ctx, &pdpb.GetRegionRequest{
		Header:    c.requestHeader(),
		RegionKey: key,
	})
	cancel()

	if err != nil {
		cmdFailDurationGetRegion.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return nil, errors.WithStack(err)
	}
	return c.parseRegionResponse(resp), nil
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
			log.Error("[pd] can't get grpc connection", zap.String("memberURL", url), errs.ZapError(err))
			continue
		}
		cc := pdpb.NewPDClient(conn)
		resp, err = cc.GetRegion(ctx, &pdpb.GetRegionRequest{
			Header:    c.requestHeader(),
			RegionKey: key,
		})
		if err != nil {
			log.Error("[pd] can't get region info", zap.String("memberURL", url), errs.ZapError(err))
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
	return c.parseRegionResponse(resp), nil
}

func (c *client) GetPrevRegion(ctx context.Context, key []byte) (*Region, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.GetPrevRegion", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationGetPrevRegion.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	resp, err := c.leaderClient().GetPrevRegion(ctx, &pdpb.GetRegionRequest{
		Header:    c.requestHeader(),
		RegionKey: key,
	})
	cancel()

	if err != nil {
		cmdFailDurationGetPrevRegion.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return nil, errors.WithStack(err)
	}
	return c.parseRegionResponse(resp), nil
}

func (c *client) GetRegionByID(ctx context.Context, regionID uint64) (*Region, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.GetRegionByID", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationGetRegionByID.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	resp, err := c.leaderClient().GetRegionByID(ctx, &pdpb.GetRegionByIDRequest{
		Header:   c.requestHeader(),
		RegionId: regionID,
	})
	cancel()

	if err != nil {
		cmdFailedDurationGetRegionByID.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return nil, errors.WithStack(err)
	}
	return c.parseRegionResponse(resp), nil
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
		scanCtx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	resp, err := c.leaderClient().ScanRegions(scanCtx, &pdpb.ScanRegionsRequest{
		Header:   c.requestHeader(),
		StartKey: key,
		EndKey:   endKey,
		Limit:    int32(limit),
	})
	if err != nil {
		cmdFailedDurationScanRegions.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return nil, errors.WithStack(err)
	}

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
	return regions, nil
}

func (c *client) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.GetStore", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationGetStore.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	resp, err := c.leaderClient().GetStore(ctx, &pdpb.GetStoreRequest{
		Header:  c.requestHeader(),
		StoreId: storeID,
	})
	cancel()

	if err != nil {
		cmdFailedDurationGetStore.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return nil, errors.WithStack(err)
	}
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

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	resp, err := c.leaderClient().GetAllStores(ctx, &pdpb.GetAllStoresRequest{
		Header:                 c.requestHeader(),
		ExcludeTombstoneStores: options.excludeTombstone,
	})
	cancel()

	if err != nil {
		cmdFailedDurationGetAllStores.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return nil, errors.WithStack(err)
	}
	stores := resp.GetStores()
	return stores, nil
}

func (c *client) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.UpdateGCSafePoint", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationUpdateGCSafePoint.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	resp, err := c.leaderClient().UpdateGCSafePoint(ctx, &pdpb.UpdateGCSafePointRequest{
		Header:    c.requestHeader(),
		SafePoint: safePoint,
	})
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

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	resp, err := c.leaderClient().UpdateServiceGCSafePoint(ctx, &pdpb.UpdateServiceGCSafePointRequest{
		Header:    c.requestHeader(),
		ServiceId: []byte(serviceID),
		TTL:       ttl,
		SafePoint: safePoint,
	})
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

func (c *client) ScatterRegionWithOption(ctx context.Context, regionID uint64, opts ...ScatterRegionOption) error {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.ScatterRegionWithOption", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	options := &ScatterRegionOp{}
	for _, opt := range opts {
		opt(options)
	}
	return c.scatterRegionsWithGroup(ctx, regionID, options.group)
}

func (c *client) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.GetOperator", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationGetOperator.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.leaderClient().GetOperator(ctx, &pdpb.GetOperatorRequest{
		Header:   c.requestHeader(),
		RegionId: regionID,
	})
}

func (c *client) requestHeader() *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: c.clusterID,
	}
}

func (c *client) scatterRegionsWithGroup(ctx context.Context, regionID uint64, group string) error {
	start := time.Now()
	defer func() { cmdDurationScatterRegion.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	resp, err := c.leaderClient().ScatterRegion(ctx, &pdpb.ScatterRegionRequest{
		Header:   c.requestHeader(),
		RegionId: regionID,
		Group:    group,
	})
	cancel()
	if err != nil {
		return err
	}
	if resp.Header.GetError() != nil {
		return errors.Errorf("scatter region %d failed: %s", regionID, resp.Header.GetError().String())
	}
	return nil
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
