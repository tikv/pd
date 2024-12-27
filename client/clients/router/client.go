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
	"github.com/tikv/pd/client/pkg/retry"
	"github.com/tikv/pd/client/pkg/utils/timerutil"
	sd "github.com/tikv/pd/client/servicediscovery"
)

const (
	// defaultMaxRouterRequestBatchSize is the default max size of the router request batch.
	defaultMaxRouterRequestBatchSize = 10000
)

// Region contains information of a region's meta and its peers.
type Region struct {
	Meta         *metapb.Region
	Leader       *metapb.Peer
	DownPeers    []*metapb.Peer
	PendingPeers []*metapb.Peer
	Buckets      *metapb.Buckets
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
	// connManger is used to store the context of the router stream connection(s).
	connManger *connectionManager
	// updateConnectionCh is used to trigger the connection update actively.
	updateConnectionCh chan struct{}

	reqPool         *sync.Pool
	requestCh       chan *Request
	batchController *batch.Controller[*Request]
}

// NewClient returns a new router client.
func NewClient(
	ctx context.Context,
	svcDiscovery sd.ServiceDiscovery,
) *Cli {
	ctx, cancel := context.WithCancel(ctx)
	c := &Cli{
		ctx:                ctx,
		cancel:             cancel,
		svcDiscovery:       svcDiscovery,
		connManger:         newConnectionManager(),
		updateConnectionCh: make(chan struct{}, 1),
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

	eventSrc := svcDiscovery.(sd.EventSource)
	eventSrc.SetLeaderURLUpdatedCallback(c.updateLeaderURL)

	c.wg.Add(2)
	go c.connectionDaemon()
	go c.dispatcher()

	return c
}

func requestFinisher(region *Region) batch.FinisherFunc[*Request] {
	return func(_ int, req *Request, err error) {
		requestCtx := req.requestCtx
		req.region = region
		req.TryDone(err)
		trace.StartRegion(requestCtx, "pdclient.regionReqDone").End()
	}
}

func (c *Cli) cancelCollectedRequests(err error) {
	c.batchController.FinishCollectedRequests(requestFinisher(nil), err)
}

func (c *Cli) doneCollectedRequests(_ *pdpb.QueryRegionResponse) {
	// TODO: dispatch the regions to the request finisher.
	c.batchController.FinishCollectedRequests(requestFinisher(&Region{}), nil)
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
	oldURL := c.leaderURL.Load()
	if oldURL == nil {
		return ""
	}
	return oldURL.(string)
}

func (c *Cli) updateLeaderURL(url string) error {
	oldURL := c.getLeaderURL()
	if !c.leaderURL.CompareAndSwap(oldURL, url) {
		return nil
	}
	log.Info("[router] switch the router leader serving url",
		zap.String("old-url", oldURL), zap.String("new-url", url))

	c.scheduleUpdateConnection()
	return nil
}

// getLeaderClientConn returns the leader gRPC client connection.
func (c *Cli) getLeaderClientConn() (*grpc.ClientConn, string) {
	url := c.getLeaderURL()
	if len(url) == 0 {
		log.Fatal("[router] the router leader should exist")
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
	err := c.connManger.storeIfNotExist(ctx, url, cc)
	if err != nil {
		log.Error("[router] failed to update the router stream connection", errs.ZapError(err))
	}
	// TODO: support the forwarding mechanism for the router client.
}

func (c *Cli) dispatcher() {
	defer c.wg.Done()

	var (
		connectionCtx     *connectionCtx
		timeoutTimer      *time.Timer
		resetTimeoutTimer = func() {
			if timeoutTimer == nil {
				timeoutTimer = time.NewTimer(c.option.Timeout)
			} else {
				timerutil.SafeResetTimer(timeoutTimer, c.option.Timeout)
			}
		}
		bo = retry.InitialBackoffer(
			sd.UpdateMemberBackOffBaseTime, sd.UpdateMemberTimeout, sd.UpdateMemberBackOffBaseTime)
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
			connectionCtx = c.connManger.chooseConnectionCtx()
			if connectionCtx == nil {
				log.Info("[router] router stream connection is not ready")
				c.scheduleUpdateConnection()
				continue connectionCtxChoosingLoop
			}
			// Check if the stream connection is canceled.
			select {
			case <-connectionCtx.ctx.Done():
				log.Info("[router] router stream connection is canceled", zap.String("stream-url", connectionCtx.streamURL))
				c.connManger.deleteConnectionCtx(connectionCtx.streamURL)
				connectionCtx = nil
				continue connectionCtxChoosingLoop
			default:
			}
			// The stream connection is ready, break the loop.
			break connectionCtxChoosingLoop
		}

		// Step 3: Dispatch the router requests to the stream connection.
		err = c.processRequests(connectionCtx)
		if err != nil {
			if !c.handleProcessRequestError(ctx, connectionCtx, err, bo) {
				return
			}
		}
	}
}

func (c *Cli) processRequests(cctx *connectionCtx) error {
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

	req := &pdpb.QueryRegionRequest{
		// TODO: build the request.
	}
	err := cctx.stream.Send(req)
	if err != nil {
		return err
	}
	resp, err := cctx.stream.Recv()
	if err != nil {
		return err
	}
	c.doneCollectedRequests(resp)
	return nil
}

func (c *Cli) handleProcessRequestError(ctx context.Context, cctx *connectionCtx, err error, bo *retry.Backoffer) bool {
	log.Error("[router] failed to process the router requests",
		zap.String("stream-url", cctx.streamURL),
		errs.ZapError(err))
	c.cancelCollectedRequests(err)

	select {
	case <-ctx.Done():
		return false
	default:
	}

	// Delete the stream connection context.
	c.connManger.deleteConnectionCtx(cctx.streamURL)
	if errs.IsLeaderChange(err) {
		// If the leader changes, we better call `CheckMemberChanged` blockingly to
		// ensure the next round of router requests can be sent to the new leader.
		if err := bo.Exec(ctx, c.svcDiscovery.CheckMemberChanged); err != nil {
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
