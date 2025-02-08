// Copyright 2024 TiKV Project Authors.
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

package router

import (
	"context"
	"encoding/hex"
	"net/url"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/batch"
	cctx "github.com/tikv/pd/client/pkg/connectionctx"
	"github.com/tikv/pd/client/pkg/retry"
	sd "github.com/tikv/pd/client/servicediscovery"
)

// defaultMaxRouterRequestBatchSize is the default max size of the router request batch.
const defaultMaxRouterRequestBatchSize = 10000

// Region contains information of a region's meta and its peers.
type Region struct {
	Meta         *metapb.Region
	Leader       *metapb.Peer
	DownPeers    []*metapb.Peer
	PendingPeers []*metapb.Peer
	Buckets      *metapb.Buckets
}

type regionResponse interface {
	GetRegion() *metapb.Region
	GetLeader() *metapb.Peer
	GetDownPeers() []*pdpb.PeerStats
	GetPendingPeers() []*metapb.Peer
	GetBuckets() *metapb.Buckets
}

// ConvertToRegion converts the region response to the region.
func ConvertToRegion(res regionResponse) *Region {
	region := res.GetRegion()
	if region == nil {
		return nil
	}

	r := &Region{
		Meta:         region,
		Leader:       res.GetLeader(),
		PendingPeers: res.GetPendingPeers(),
		Buckets:      res.GetBuckets(),
	}
	for _, s := range res.GetDownPeers() {
		r.DownPeers = append(r.DownPeers, s.Peer)
	}
	return r
}

// KeyRange defines a range of keys in bytes.
type KeyRange struct {
	StartKey []byte
	EndKey   []byte
}

// NewKeyRange creates a new key range structure with the given start key and end key bytes.
// Notice: the actual encoding of the key range is not specified here. It should be either UTF-8 or hex.
//   - UTF-8 means the key has already been encoded into a string with UTF-8 encoding, like:
//     []byte{52 56 54 53 54 99 54 99 54 102 50 48 53 55 54 102 55 50 54 99 54 52}, which will later be converted to "48656c6c6f20576f726c64"
//     by using `string()` method.
//   - Hex means the key is just a raw hex bytes without encoding to a UTF-8 string, like:
//     []byte{72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100}, which will later be converted to "48656c6c6f20576f726c64"
//     by using `hex.EncodeToString()` method.
func NewKeyRange(startKey, endKey []byte) *KeyRange {
	return &KeyRange{startKey, endKey}
}

// EscapeAsUTF8Str returns the URL escaped key strings as they are UTF-8 encoded.
func (r *KeyRange) EscapeAsUTF8Str() (startKeyStr, endKeyStr string) {
	startKeyStr = url.QueryEscape(string(r.StartKey))
	endKeyStr = url.QueryEscape(string(r.EndKey))
	return
}

// EscapeAsHexStr returns the URL escaped key strings as they are hex encoded.
func (r *KeyRange) EscapeAsHexStr() (startKeyStr, endKeyStr string) {
	startKeyStr = url.QueryEscape(hex.EncodeToString(r.StartKey))
	endKeyStr = url.QueryEscape(hex.EncodeToString(r.EndKey))
	return
}

// Client defines the interface of a router client, which includes the methods for obtaining the routing information.
type Client interface {
	// GetRegion gets a region and its leader Peer from PD by key.
	// The region may expire after split. Caller is responsible for caching and
	// taking care of region change.
	// Also, it may return nil if PD finds no Region for the key temporarily,
	// client should retry later.
	GetRegion(ctx context.Context, key []byte, opts ...opt.GetRegionOption) (*Region, error)
	// GetRegionFromMember gets a region from certain members.
	GetRegionFromMember(ctx context.Context, key []byte, memberURLs []string, opts ...opt.GetRegionOption) (*Region, error)
	// GetPrevRegion gets the previous region and its leader Peer of the region where the key is located.
	GetPrevRegion(ctx context.Context, key []byte, opts ...opt.GetRegionOption) (*Region, error)
	// GetRegionByID gets a region and its leader Peer from PD by id.
	GetRegionByID(ctx context.Context, regionID uint64, opts ...opt.GetRegionOption) (*Region, error)
	// Deprecated: use BatchScanRegions instead.
	// ScanRegions gets a list of regions, starts from the region that contains key.
	// Limit limits the maximum number of regions returned. It returns all the regions in the given range if limit <= 0.
	// If a region has no leader, corresponding leader will be placed by a peer
	// with empty value (PeerID is 0).
	ScanRegions(ctx context.Context, key, endKey []byte, limit int, opts ...opt.GetRegionOption) ([]*Region, error)
	// BatchScanRegions gets a list of regions, starts from the region that contains key.
	// Limit limits the maximum number of regions returned. It returns all the regions in the given ranges if limit <= 0.
	// If a region has no leader, corresponding leader will be placed by a peer
	// with empty value (PeerID is 0).
	// The returned regions are flattened, even there are key ranges located in the same region, only one region will be returned.
	BatchScanRegions(ctx context.Context, keyRanges []KeyRange, limit int, opts ...opt.GetRegionOption) ([]*Region, error)
}

// Cli is the implementation of the router client.
type Cli struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	option *opt.Option

	svcDiscovery sd.ServiceDiscovery
	// leaderURL is the URL of the router leader.
	leaderURL atomic.Value
	// conCtxMgr is used to store the context of the router stream connection(s).
	conCtxMgr *cctx.Manager[pdpb.PD_QueryRegionClient]
	// updateConnectionCh is used to trigger the connection update actively.
	updateConnectionCh chan struct{}
	// bo is the backoffer for the router client.
	bo *retry.Backoffer

	reqPool         *sync.Pool
	requestCh       chan *Request
	batchController *batch.Controller[*Request]
}

// NewClient returns a new router client.
func NewClient(
	ctx context.Context,
	svcDiscovery sd.ServiceDiscovery,
	option *opt.Option,
) *Cli {
	ctx, cancel := context.WithCancel(ctx)
	c := &Cli{
		ctx:                ctx,
		cancel:             cancel,
		svcDiscovery:       svcDiscovery,
		option:             option,
		conCtxMgr:          cctx.NewManager[pdpb.PD_QueryRegionClient](),
		updateConnectionCh: make(chan struct{}, 1),
		bo: retry.InitialBackoffer(
			sd.UpdateMemberBackOffBaseTime,
			sd.UpdateMemberMaxBackoffTime,
			sd.UpdateMemberTimeout,
		),
		reqPool: &sync.Pool{
			New: func() any {
				return &Request{
					done: make(chan error, 1),
				}
			},
		},
		requestCh:       make(chan *Request, defaultMaxRouterRequestBatchSize*2),
		batchController: batch.NewController(defaultMaxRouterRequestBatchSize, requestFinisher(nil), nil),
	}
	c.leaderURL.Store(svcDiscovery.GetServingURL())
	c.svcDiscovery.ExecAndAddLeaderSwitchedCallback(c.updateLeaderURL)
	c.svcDiscovery.AddMembersChangedCallback(c.scheduleUpdateConnection)

	c.wg.Add(2)
	go c.connectionDaemon()
	go c.dispatcher()

	return c
}

func (c *Cli) newRequest(ctx context.Context) *Request {
	req := c.reqPool.Get().(*Request)
	req.requestCtx = ctx
	req.clientCtx = c.ctx
	req.pool = c.reqPool

	return req
}

func requestFinisher(resp *pdpb.QueryRegionResponse) batch.FinisherFunc[*Request] {
	var keyIdx, prevKeyIdx int
	return func(_ int, req *Request, err error) {
		requestCtx := req.requestCtx
		defer trace.StartRegion(requestCtx, "pdclient.regionReqDone").End()

		if err != nil {
			req.tryDone(err)
			return
		}

		var id uint64
		if req.key != nil {
			id = resp.KeyIdMap[keyIdx]
			keyIdx++
		} else if req.prevKey != nil {
			id = resp.PrevKeyIdMap[prevKeyIdx]
			prevKeyIdx++
		} else if req.id != 0 {
			id = req.id
		}
		if region, ok := resp.RegionsById[id]; ok {
			req.region = ConvertToRegion(region)
		}
		req.tryDone(err)
	}
}

func (c *Cli) cancelCollectedRequests(err error) {
	c.batchController.FinishCollectedRequests(requestFinisher(nil), err)
}

func (c *Cli) doneCollectedRequests(resp *pdpb.QueryRegionResponse) {
	c.batchController.FinishCollectedRequests(requestFinisher(resp), nil)
}

// Close closes the router client.
func (c *Cli) Close() {
	if c == nil {
		return
	}
	log.Info("[router] closing router client")

	c.cancel()
	c.wg.Wait()

	log.Info("[router] router client is closed")
}

func (c *Cli) getLeaderURL() string {
	url := c.leaderURL.Load()
	if url == nil {
		return ""
	}
	return url.(string)
}

func (c *Cli) updateLeaderURL(url string) error {
	oldURL := c.getLeaderURL()
	if oldURL == url {
		return nil
	}
	c.leaderURL.Store(url)
	c.scheduleUpdateConnection()

	log.Info("[router] switch the router leader serving url",
		zap.String("old-url", oldURL), zap.String("new-url", url))
	return nil
}

// getLeaderClientConn returns the leader gRPC client connection.
func (c *Cli) getLeaderClientConn() (*grpc.ClientConn, string) {
	url := c.getLeaderURL()
	if len(url) == 0 {
		c.svcDiscovery.ScheduleCheckMemberChanged()
		return nil, ""
	}
	cc, ok := c.svcDiscovery.GetClientConns().Load(url)
	if !ok {
		return nil, url
	}
	return cc.(*grpc.ClientConn), url
}

// scheduleUpdateConnection is used to schedule an update to the connection(s).
func (c *Cli) scheduleUpdateConnection() {
	select {
	case c.updateConnectionCh <- struct{}{}:
	default:
	}
}

// connectionDaemon is used to update the router leader/primary/backup connection(s) in background.
// It aims to provide a seamless connection updating for the router client to keep providing the
// router service without interruption.
func (c *Cli) connectionDaemon() {
	defer c.wg.Done()
	updaterCtx, updaterCancel := context.WithCancel(c.ctx)
	defer updaterCancel()
	updateTicker := time.NewTicker(sd.MemberUpdateInterval)
	defer updateTicker.Stop()

	log.Info("[router] connection daemon is started")
	for {
		c.updateConnection(updaterCtx)
		select {
		case <-updaterCtx.Done():
			log.Info("[router] connection daemon is exiting")
			return
		case <-updateTicker.C:
		case <-c.updateConnectionCh:
		}
	}
}

// updateConnection is used to get the leader client connection and update the connection context if it does not exist before.
func (c *Cli) updateConnection(ctx context.Context) {
	cc, url := c.getLeaderClientConn()
	if cc == nil || len(url) == 0 {
		log.Warn("[router] got an invalid leader client connection", zap.String("url", url))
		return
	}
	if c.conCtxMgr.Exist(url) {
		log.Debug("[router] the router leader remains unchanged", zap.String("url", url))
		return
	}
	stream, err := pdpb.NewPDClient(cc).QueryRegion(ctx)
	if err != nil {
		log.Error("[router] failed to create the router stream connection", errs.ZapError(err))
	}
	c.conCtxMgr.Store(ctx, url, stream)
	// TODO: support the forwarding mechanism for the router client.
	// TODO: support sending the router requests to the follower nodes.
}

func (c *Cli) dispatcher() {
	defer c.wg.Done()

	var (
		stream            pdpb.PD_QueryRegionClient
		streamURL         string
		streamCtx         context.Context
		timeoutTimer      *time.Timer
		resetTimeoutTimer = func() {
			if timeoutTimer == nil {
				timeoutTimer = time.NewTimer(c.option.Timeout)
			} else {
				timeoutTimer.Reset(c.option.Timeout)
			}
		}
		ctx, cancel = context.WithCancel(c.ctx)
	)

	log.Info("[router] dispatcher is started")
	defer func() {
		log.Info("[router] dispatcher is exiting")
		cancel()
		if timeoutTimer != nil {
			timeoutTimer.Stop()
		}
		log.Info("[router] dispatcher exited")
	}()
batchLoop:
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Step 1: Fetch the pending router requests in batch.
		err := c.batchController.FetchPendingRequests(ctx, c.requestCh, nil, 0)
		if err != nil {
			if err == context.Canceled {
				log.Info("[router] stop fetching the pending router requests due to context canceled")
			} else {
				log.Error("[router] failed to fetch the pending router requests", errs.ZapError(err))
			}
			return
		}

		// Step 2: Choose a stream connection to send the router request.
		resetTimeoutTimer()
	connectionCtxChoosingLoop:
		for {
			// Check if the dispatcher is canceled or the timeout timer is triggered.
			select {
			case <-ctx.Done():
				return
			case <-timeoutTimer.C:
				log.Error("[router] router stream connection is not ready until timeout, abort the batch")
				c.svcDiscovery.ScheduleCheckMemberChanged()
				c.batchController.FinishCollectedRequests(requestFinisher(nil), err)
				continue batchLoop
			default:
			}
			// Choose a stream connection to send the router request later.
			connectionCtx := c.conCtxMgr.GetConnectionCtx()
			if connectionCtx == nil {
				log.Info("[router] router stream connection is not ready")
				c.updateConnection(ctx)
				continue connectionCtxChoosingLoop
			}
			streamCtx, streamURL, stream = connectionCtx.Ctx, connectionCtx.StreamURL, connectionCtx.Stream
			// Check if the stream connection is canceled.
			select {
			case <-streamCtx.Done():
				log.Info("[router] router stream connection is canceled", zap.String("stream-url", streamURL))
				c.conCtxMgr.Release(streamURL)
				continue connectionCtxChoosingLoop
			default:
			}
			// The stream connection is ready, break the loop.
			break connectionCtxChoosingLoop
		}

		// Step 3: Dispatch the router requests to the stream connection.
		// TODO: timeout handling if the stream takes too long to process the requests.
		err = c.processRequests(stream)
		if err != nil {
			if !c.handleProcessRequestError(ctx, streamURL, err) {
				return
			}
		}
	}
}

func (c *Cli) processRequests(stream pdpb.PD_QueryRegionClient) error {
	var (
		requests     = c.batchController.GetCollectedRequests()
		traceRegions = make([]*trace.Region, 0, len(requests))
		spans        = make([]opentracing.Span, 0, len(requests))
	)
	for _, req := range requests {
		traceRegions = append(traceRegions, trace.StartRegion(req.requestCtx, "pdclient.regionReqSend"))
		if span := opentracing.SpanFromContext(req.requestCtx); span != nil && span.Tracer() != nil {
			spans = append(spans, span.Tracer().StartSpan("pdclient.processRegionRequests", opentracing.ChildOf(span.Context())))
		}
	}
	defer func() {
		for i := range spans {
			spans[i].Finish()
		}
		for i := range traceRegions {
			traceRegions[i].End()
		}
	}()

	queryReq := &pdpb.QueryRegionRequest{
		Header: &pdpb.RequestHeader{
			ClusterId: c.svcDiscovery.GetClusterID(),
		},
		Keys:     make([][]byte, 0, len(requests)),
		PrevKeys: make([][]byte, 0, len(requests)),
		Ids:      make([]uint64, 0, len(requests)),
	}
	for _, req := range requests {
		if !queryReq.NeedBuckets && req.needBuckets {
			queryReq.NeedBuckets = true
		}
		if req.key != nil {
			queryReq.Keys = append(queryReq.Keys, req.key)
		} else if req.prevKey != nil {
			queryReq.PrevKeys = append(queryReq.PrevKeys, req.prevKey)
		} else if req.id != 0 {
			queryReq.Ids = append(queryReq.Ids, req.id)
		} else {
			panic("invalid region query request received")
		}
	}
	err := stream.Send(queryReq)
	if err != nil {
		return err
	}
	resp, err := stream.Recv()
	if err != nil {
		return err
	}
	c.doneCollectedRequests(resp)
	return nil
}

func (c *Cli) handleProcessRequestError(
	ctx context.Context,
	streamURL string,
	err error,
) bool {
	log.Error("[router] failed to process the router requests",
		zap.String("stream-url", streamURL),
		errs.ZapError(err))
	c.cancelCollectedRequests(err)

	select {
	case <-ctx.Done():
		return false
	default:
	}

	// Delete the stream connection context.
	c.conCtxMgr.Release(streamURL)
	if errs.IsLeaderChange(err) {
		// If the leader changes, we better call `CheckMemberChanged` blockingly to
		// ensure the next round of router requests can be sent to the new leader.
		if err := c.bo.Exec(ctx, c.svcDiscovery.CheckMemberChanged); err != nil {
			select {
			case <-ctx.Done():
				return false
			default:
			}
		}
	} else {
		// For other errors, we can just schedule a member change check asynchronously.
		c.svcDiscovery.ScheduleCheckMemberChanged()
	}

	return true
}
