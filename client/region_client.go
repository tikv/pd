// Copyright 2026 TiKV Project Authors.
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
	"runtime/trace"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/metrics"
	"github.com/tikv/pd/client/pkg/batch"
	cctx "github.com/tikv/pd/client/pkg/connectionctx"
	"github.com/tikv/pd/client/retry"
)

const defaultMaxRegionRequestBatchSize = 10000

type regionClient struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	option *option

	svcDiscovery *pdServiceDiscovery
	conCtxMgr    *cctx.Manager[pdpb.PD_QueryRegionClient]
	bo           *retry.Backoffer

	updateConnectionCh chan struct{}

	reqPool         *sync.Pool
	requestCh       chan *regionRequest
	batchController *batch.Controller[*regionRequest]
}

func newRegionClient(ctx context.Context, option *option, svcDiscovery *pdServiceDiscovery) *regionClient {
	ctx, cancel := context.WithCancel(ctx)
	c := &regionClient{
		ctx:                ctx,
		cancel:             cancel,
		option:             option,
		svcDiscovery:       svcDiscovery,
		conCtxMgr:          cctx.NewManager[pdpb.PD_QueryRegionClient](),
		bo:                 retry.InitialBackoffer(updateMemberBackOffBaseTime, updateMemberMaxBackoffTime, updateMemberTimeout),
		updateConnectionCh: make(chan struct{}, 1),
		reqPool: &sync.Pool{
			New: func() any {
				return &regionRequest{
					done: make(chan error, 1),
				}
			},
		},
		requestCh: make(chan *regionRequest, defaultMaxRegionRequestBatchSize*2),
		batchController: batch.NewController(
			defaultMaxRegionRequestBatchSize,
			regionRequestFinisher(nil),
			metrics.QueryRegionBestBatchSize,
		),
	}
	svcDiscovery.AddServingURLSwitchedCallback(c.scheduleUpdateConnection)
	svcDiscovery.AddServiceURLsSwitchedCallback(c.scheduleUpdateConnection)

	c.wg.Add(2)
	go c.connectionDaemon()
	go c.dispatcher()

	return c
}

func (c *regionClient) newRequest(ctx context.Context, opts ...GetRegionOption) *regionRequest {
	req := c.reqPool.Get().(*regionRequest)
	req.requestCtx = ctx
	req.clientCtx = c.ctx
	req.key = nil
	req.prevKey = nil
	req.id = 0
	req.options = nil
	req.region = nil
	req.start = time.Now()
	req.pool = c.reqPool
	req.options = &GetRegionOp{}
	for _, opt := range opts {
		opt(req.options)
	}

	return req
}

func (c *regionClient) GetRegion(ctx context.Context, key []byte, opts ...GetRegionOption) (*Region, error) {
	req := c.newRequest(ctx, opts...)
	if key == nil {
		key = []byte{}
	}
	req.key = key

	c.requestCh <- req
	return req.wait()
}

func (c *regionClient) GetPrevRegion(ctx context.Context, key []byte, opts ...GetRegionOption) (*Region, error) {
	req := c.newRequest(ctx, opts...)
	if key == nil {
		key = []byte{}
	}
	req.prevKey = key

	c.requestCh <- req
	return req.wait()
}

func (c *regionClient) GetRegionByID(ctx context.Context, regionID uint64, opts ...GetRegionOption) (*Region, error) {
	req := c.newRequest(ctx, opts...)
	req.id = regionID

	c.requestCh <- req
	return req.wait()
}

func (c *regionClient) close() {
	if c == nil {
		return
	}
	log.Info("[region] closing query region client")

	c.cancel()
	c.wg.Wait()
	c.conCtxMgr.ReleaseAll()

	log.Info("[region] query region client is closed")
}

func (c *regionClient) scheduleUpdateConnection() {
	select {
	case c.updateConnectionCh <- struct{}{}:
	default:
	}
}

func (c *regionClient) getLeaderClientConn() (pdpb.PDClient, string) {
	url := c.svcDiscovery.GetServingURL()
	if len(url) == 0 {
		c.svcDiscovery.ScheduleCheckMemberChanged()
		return nil, ""
	}
	cc := c.svcDiscovery.GetServingEndpointClientConn()
	if cc == nil {
		return nil, url
	}
	return pdpb.NewPDClient(cc), url
}

func (c *regionClient) getAllClientConns() map[string]*grpc.ClientConn {
	conns := make(map[string]*grpc.ClientConn)
	c.svcDiscovery.GetClientConns().Range(func(key, value any) bool {
		url, ok := key.(string)
		if !ok || len(url) == 0 {
			return true
		}
		conn, ok := value.(*grpc.ClientConn)
		if !ok || conn == nil {
			return true
		}
		conns[url] = conn
		return true
	})
	return conns
}

func (c *regionClient) connectionDaemon() {
	defer c.wg.Done()
	updaterCtx, updaterCancel := context.WithCancel(c.ctx)
	defer updaterCancel()
	updateTicker := time.NewTicker(memberUpdateInterval)
	defer updateTicker.Stop()

	log.Info("[region] connection daemon is started")
	for {
		c.updateConnection(updaterCtx)
		select {
		case <-updaterCtx.Done():
			log.Info("[region] connection daemon is exiting")
			return
		case <-c.option.enableFollowerHandleCh:
			log.Info("[region] follower handle status changed",
				zap.Bool("enable", c.option.getEnableFollowerHandle()))
		case <-updateTicker.C:
		case <-c.updateConnectionCh:
		}
	}
}

func (c *regionClient) updateConnection(ctx context.Context) {
	cc, url := c.getLeaderClientConn()
	if cc == nil || len(url) == 0 {
		log.Warn("[region] got an invalid leader client connection", zap.String("url", url))
	} else if c.conCtxMgr.Exist(url) {
		log.Debug("[region] the leader remains unchanged", zap.String("url", url))
	} else {
		cctx, cancel := context.WithCancel(ctx)
		stream, err := cc.QueryRegion(cctx)
		if err != nil {
			log.Error("[region] failed to create the leader query region stream connection", errs.ZapError(err))
		}
		if stream != nil {
			c.conCtxMgr.Store(cctx, cancel, url, stream)
			log.Info("[region] successfully established the leader query region stream connection", zap.String("url", url))
		} else {
			log.Warn("[region] failed to create the leader query region stream connection")
			cancel()
		}
	}

	if c.option.getEnableFollowerHandle() {
		conns := c.getAllClientConns()
		if len(conns) == 0 {
			log.Warn("[region] no query region node found")
			return
		}
		for url, conn := range conns {
			if !c.conCtxMgr.Exist(url) {
				cctx, cancel := context.WithCancel(ctx)
				stream, err := pdpb.NewPDClient(conn).QueryRegion(cctx)
				if err != nil {
					log.Error("[region] failed to create the follower query region stream connection", errs.ZapError(err))
					cancel()
				} else if stream != nil {
					c.conCtxMgr.Store(cctx, cancel, url, stream)
					log.Info("[region] successfully established the follower query region stream connection", zap.String("url", url))
				} else {
					log.Warn("[region] failed to create the follower query region stream connection")
					cancel()
				}
			}
		}
		c.conCtxMgr.GC(func(url string) bool {
			_, ok := conns[url]
			if !ok {
				log.Info("[region] release the stale query region stream connection", zap.String("url", url))
				return true
			}
			return false
		})
	} else {
		leaderURL := c.svcDiscovery.GetServingURL()
		c.conCtxMgr.GC(func(url string) bool {
			return url != leaderURL
		})
	}
}

func (c *regionClient) dispatcher() {
	defer c.wg.Done()

	var (
		streamURL         string
		timeoutTimer      *time.Timer
		resetTimeoutTimer = func() {
			if timeoutTimer == nil {
				timeoutTimer = time.NewTimer(c.option.timeout)
			} else {
				timeoutTimer.Reset(c.option.timeout)
			}
		}
		ctx, cancel = context.WithCancel(c.ctx)
	)

	log.Info("[region] dispatcher is started")
	defer func() {
		log.Info("[region] dispatcher is exiting")
		cancel()
		if timeoutTimer != nil {
			timeoutTimer.Stop()
		}
		log.Info("[region] dispatcher exited")
	}()
batchLoop:
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		err := c.batchController.FetchPendingRequests(ctx, c.requestCh, nil, 0)
		if err != nil {
			if err == context.Canceled {
				log.Info("[region] stop fetching pending query region requests due to context canceled")
			} else {
				log.Error("[region] failed to fetch pending query region requests", errs.ZapError(err))
			}
			return
		}

		resetTimeoutTimer()
		var (
			processQueryFunc processRegionFn
			retry            bool
		)
	connectionCtxChoosingLoop:
		for {
			select {
			case <-ctx.Done():
				return
			case <-timeoutTimer.C:
				log.Error("[region] query region stream connection is not ready until timeout, abort the batch")
				c.svcDiscovery.ScheduleCheckMemberChanged()
				c.batchController.FinishCollectedRequests(regionRequestFinisher(nil), errs.ErrClientRouterConnectionTimeout)
				continue batchLoop
			default:
			}
			processQueryFunc, streamURL, retry = c.sendToPD(ctx)
			if retry {
				continue connectionCtxChoosingLoop
			}
			break connectionCtxChoosingLoop
		}

		err = processQueryFunc()
		if err != nil && !c.handleProcessRequestError(ctx, streamURL, err) {
			return
		}
	}
}

func (c *regionClient) sendToPD(ctx context.Context) (processRegionFn, string, bool) {
	allowFollowerHandle := c.option.getEnableFollowerHandle()
	if allowFollowerHandle {
		c.batchController.IterCollectedRequests(func(req *regionRequest) bool {
			if !req.options.allowFollowerHandle {
				allowFollowerHandle = false
				return false
			}
			return true
		})
	}

	var connectionCtx *cctx.ConnectionCtx[pdpb.PD_QueryRegionClient]
	if allowFollowerHandle {
		connectionCtx = c.conCtxMgr.RandomlyPick()
	} else {
		connectionCtx = c.conCtxMgr.GetConnectionCtx(c.svcDiscovery.GetServingURL())
	}
	if connectionCtx == nil {
		log.Info("[region] query region stream connection is not ready")
		c.updateConnection(ctx)
		return nil, "", true
	}
	select {
	case <-connectionCtx.Ctx.Done():
		log.Info("[region] query region stream connection is canceled", zap.String("stream-url", connectionCtx.StreamURL))
		c.conCtxMgr.Release(connectionCtx.StreamURL)
		return nil, "", true
	default:
	}
	return func() error {
		return c.processRequestsInner(connectionCtx.Stream.Send, connectionCtx.Stream.Recv)
	}, connectionCtx.StreamURL, false
}

type processRegionFn func() error
type recvRegionFn func() (*pdpb.QueryRegionResponse, error)
type sendRegionFn func(*pdpb.QueryRegionRequest) error

func (c *regionClient) processRequestsInner(send sendRegionFn, recv recvRegionFn) error {
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
		if !queryReq.NeedBuckets && req.options.needBuckets {
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
	start := time.Now()
	err := send(queryReq)
	if err != nil {
		metrics.RequestFailedDurationQueryRegion.Observe(time.Since(start).Seconds())
		return err
	}
	metrics.QueryRegionBatchSendLatency.Observe(time.Since(c.batchController.GetExtraBatchingStartTime()).Seconds())
	resp, err := recv()
	if err != nil {
		metrics.RequestFailedDurationQueryRegion.Observe(time.Since(start).Seconds())
		return err
	}
	metrics.RequestDurationQueryRegion.Observe(time.Since(start).Seconds())
	metrics.QueryRegionBatchSizeTotal.Observe(float64(len(requests)))
	if headerErr := resp.GetHeader().GetError(); headerErr != nil {
		return errors.New(headerErr.String())
	}
	if keysLen := len(queryReq.Keys); keysLen > 0 {
		metrics.QueryRegionBatchSizeByKeys.Observe(float64(keysLen))
	}
	if prevKeysLen := len(queryReq.PrevKeys); prevKeysLen > 0 {
		metrics.QueryRegionBatchSizeByPrevKeys.Observe(float64(prevKeysLen))
	}
	if idsLen := len(queryReq.Ids); idsLen > 0 {
		metrics.QueryRegionBatchSizeByIDs.Observe(float64(idsLen))
	}
	c.doneCollectedRequests(resp)
	return nil
}

func (c *regionClient) cancelCollectedRequests(err error) {
	c.batchController.FinishCollectedRequests(regionRequestFinisher(nil), err)
}

func (c *regionClient) doneCollectedRequests(resp *pdpb.QueryRegionResponse) {
	c.batchController.FinishCollectedRequests(regionRequestFinisher(resp), nil)
}

func (c *regionClient) handleProcessRequestError(ctx context.Context, streamURL string, err error) bool {
	log.Error("[region] failed to process the query region requests",
		zap.String("stream-url", streamURL),
		errs.ZapError(err))
	c.cancelCollectedRequests(err)

	select {
	case <-ctx.Done():
		return false
	default:
	}

	c.conCtxMgr.Release(streamURL)

	if errs.IsLeaderChange(err) {
		if err := c.bo.Exec(ctx, c.svcDiscovery.CheckMemberChanged); err != nil {
			select {
			case <-ctx.Done():
				return false
			default:
			}
		}
	} else {
		c.svcDiscovery.ScheduleCheckMemberChanged()
	}

	return true
}
