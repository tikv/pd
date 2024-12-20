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

package tso

import (
	"context"
	"math/rand"
	"runtime/trace"
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
	"github.com/pingcap/log"

	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/metrics"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/utils/grpcutil"
	"github.com/tikv/pd/client/pkg/utils/tlsutil"
	sd "github.com/tikv/pd/client/servicediscovery"
)

const (
	// defaultMaxTSOBatchSize is the default max size of the TSO request batch.
	defaultMaxTSOBatchSize = 10000
	dispatchRetryDelay     = 50 * time.Millisecond
	dispatchRetryCount     = 2
)

// Client defines the interface of a TSO client.
type Client interface {
	// GetTS gets a timestamp from PD or TSO microservice.
	GetTS(ctx context.Context) (int64, int64, error)
	// GetTSAsync gets a timestamp from PD or TSO microservice, without block the caller.
	GetTSAsync(ctx context.Context) TSFuture
	// GetMinTS gets a timestamp from PD or the minimal timestamp across all keyspace groups from
	// the TSO microservice.
	GetMinTS(ctx context.Context) (int64, int64, error)

	// Deprecated: the Local TSO feature has been deprecated. Regardless of the
	// parameters passed, the behavior of this interface will be equivalent to
	// `GetTS`. If you want to use a separately deployed TSO service,
	// please refer to the deployment of the TSO microservice.
	GetLocalTS(ctx context.Context, _ string) (int64, int64, error)
	// Deprecated: the Local TSO feature has been deprecated. Regardless of the
	// parameters passed, the behavior of this interface will be equivalent to
	// `GetTSAsync`. If you want to use a separately deployed TSO service,
	// please refer to the deployment of the TSO microservice.
	GetLocalTSAsync(ctx context.Context, _ string) TSFuture
}

// Cli is the implementation of the TSO client.
type Cli struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	option *opt.Option

	svcDiscovery sd.ServiceDiscovery
	tsoStreamBuilderFactory
	// leaderURL is the URL of the TSO leader.
	leaderURL atomic.Value

	// tsoReqPool is the pool to recycle `*tsoRequest`.
	tsoReqPool *sync.Pool
	// dispatcher is used to dispatch the TSO requests to the channel.
	dispatcher atomic.Pointer[tsoDispatcher]
}

// NewClient returns a new TSO client.
func NewClient(
	ctx context.Context, option *opt.Option,
	svcDiscovery sd.ServiceDiscovery, factory tsoStreamBuilderFactory,
) *Cli {
	ctx, cancel := context.WithCancel(ctx)
	c := &Cli{
		ctx:                     ctx,
		cancel:                  cancel,
		option:                  option,
		svcDiscovery:            svcDiscovery,
		tsoStreamBuilderFactory: factory,
		tsoReqPool: &sync.Pool{
			New: func() any {
				return &Request{
					done:     make(chan error, 1),
					physical: 0,
					logical:  0,
				}
			},
		},
	}

	eventSrc := svcDiscovery.(sd.TSOEventSource)
	eventSrc.SetTSOLeaderURLUpdatedCallback(c.updateTSOLeaderURL)
	c.svcDiscovery.AddServiceURLsSwitchedCallback(c.scheduleUpdateTSOConnectionCtxs)

	return c
}

func (c *Cli) getOption() *opt.Option { return c.option }

func (c *Cli) getServiceDiscovery() sd.ServiceDiscovery { return c.svcDiscovery }

func (c *Cli) getDispatcher() *tsoDispatcher {
	return c.dispatcher.Load()
}

// GetRequestPool gets the request pool of the TSO client.
func (c *Cli) GetRequestPool() *sync.Pool {
	return c.tsoReqPool
}

// Setup initializes the TSO client.
func (c *Cli) Setup() {
	if err := c.svcDiscovery.CheckMemberChanged(); err != nil {
		log.Warn("[tso] failed to check member changed", errs.ZapError(err))
	}
	c.tryCreateTSODispatcher()
}

// Close closes the TSO client
func (c *Cli) Close() {
	if c == nil {
		return
	}
	log.Info("[tso] closing tso client")

	c.cancel()
	c.wg.Wait()

	log.Info("[tso] close tso client")
	c.getDispatcher().close()
	log.Info("[tso] tso client is closed")
}

// scheduleUpdateTSOConnectionCtxs update the TSO connection contexts.
func (c *Cli) scheduleUpdateTSOConnectionCtxs() {
	c.getDispatcher().scheduleUpdateConnectionCtxs()
}

// GetTSORequest gets a TSO request from the pool.
func (c *Cli) GetTSORequest(ctx context.Context) *Request {
	req := c.tsoReqPool.Get().(*Request)
	// Set needed fields in the request before using it.
	req.start = time.Now()
	req.pool = c.tsoReqPool
	req.requestCtx = ctx
	req.clientCtx = c.ctx
	req.physical = 0
	req.logical = 0
	req.streamID = ""
	return req
}

func (c *Cli) getLeaderURL() string {
	url := c.leaderURL.Load()
	if url == nil {
		return ""
	}
	return url.(string)
}

// getTSOLeaderClientConn returns the TSO leader gRPC client connection.
func (c *Cli) getTSOLeaderClientConn() (*grpc.ClientConn, string) {
	url := c.getLeaderURL()
	if len(url) == 0 {
		log.Fatal("[tso] the tso leader should exist")
	}
	cc, ok := c.svcDiscovery.GetClientConns().Load(url)
	if !ok {
		return nil, url
	}
	return cc.(*grpc.ClientConn), url
}

func (c *Cli) updateTSOLeaderURL(url string) error {
	c.leaderURL.Store(url)
	log.Info("[tso] switch the tso leader serving url", zap.String("new-url", url))
	// Try to create the TSO dispatcher if it is not created yet.
	c.tryCreateTSODispatcher()
	// Update the TSO connection contexts after the dispatcher is ready.
	c.scheduleUpdateTSOConnectionCtxs()
	return nil
}

// backupClientConn gets a grpc client connection of the current reachable and healthy
// backup service endpoints randomly. Backup service endpoints are followers in a
// quorum-based cluster or secondaries in a primary/secondary configured cluster.
func (c *Cli) backupClientConn() (*grpc.ClientConn, string) {
	urls := c.svcDiscovery.GetBackupURLs()
	if len(urls) < 1 {
		return nil, ""
	}
	var (
		cc  *grpc.ClientConn
		err error
	)
	for range urls {
		url := urls[rand.Intn(len(urls))]
		if cc, err = c.svcDiscovery.GetOrCreateGRPCConn(url); err != nil {
			continue
		}
		healthCtx, healthCancel := context.WithTimeout(c.ctx, c.option.Timeout)
		resp, err := healthpb.NewHealthClient(cc).Check(healthCtx, &healthpb.HealthCheckRequest{Service: ""})
		healthCancel()
		if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			return cc, url
		}
	}
	return nil, ""
}

// tsoConnectionContext is used to store the context of a TSO stream connection.
type tsoConnectionContext struct {
	ctx    context.Context
	cancel context.CancelFunc
	// Current URL of the stream connection.
	streamURL string
	// Current stream to send gRPC requests.
	stream *tsoStream
}

// updateConnectionCtxs will choose the proper way to update the connections.
// It will return a bool to indicate whether the update is successful.
func (c *Cli) updateConnectionCtxs(ctx context.Context, connectionCtxs *sync.Map) bool {
	// Normal connection creating, it will be affected by the `enableForwarding`.
	createTSOConnection := c.tryConnectToTSO
	if c.option.GetEnableTSOFollowerProxy() {
		createTSOConnection = c.tryConnectToTSOWithProxy
	}
	if err := createTSOConnection(ctx, connectionCtxs); err != nil {
		log.Error("[tso] update connection contexts failed", errs.ZapError(err))
		return false
	}
	return true
}

// tryConnectToTSO will try to connect to the TSO leader. If the connection becomes unreachable
// and enableForwarding is true, it will create a new connection to a follower to do the forwarding,
// while a new daemon will be created also to switch back to a normal leader connection ASAP the
// connection comes back to normal.
func (c *Cli) tryConnectToTSO(
	ctx context.Context,
	connectionCtxs *sync.Map,
) error {
	var (
		networkErrNum  uint64
		err            error
		stream         *tsoStream
		url            string
		cc             *grpc.ClientConn
		updateAndClear = func(newURL string, connectionCtx *tsoConnectionContext) {
			// Only store the `connectionCtx` if it does not exist before.
			if connectionCtx != nil {
				connectionCtxs.LoadOrStore(newURL, connectionCtx)
			}
			// Remove all other `connectionCtx`s.
			connectionCtxs.Range(func(url, cc any) bool {
				if url.(string) != newURL {
					cc.(*tsoConnectionContext).cancel()
					connectionCtxs.Delete(url)
				}
				return true
			})
		}
	)

	ticker := time.NewTicker(constants.RetryInterval)
	defer ticker.Stop()
	// Retry several times before falling back to the follower when the network problem happens
	for range constants.MaxRetryTimes {
		c.svcDiscovery.ScheduleCheckMemberChanged()
		cc, url = c.getTSOLeaderClientConn()
		if _, ok := connectionCtxs.Load(url); ok {
			// Just trigger the clean up of the stale connection contexts.
			updateAndClear(url, nil)
			return nil
		}
		if cc != nil {
			cctx, cancel := context.WithCancel(ctx)
			stream, err = c.tsoStreamBuilderFactory.makeBuilder(cc).build(cctx, cancel, c.option.Timeout)
			failpoint.Inject("unreachableNetwork", func() {
				stream = nil
				err = status.New(codes.Unavailable, "unavailable").Err()
			})
			if stream != nil && err == nil {
				updateAndClear(url, &tsoConnectionContext{cctx, cancel, url, stream})
				return nil
			}

			if err != nil && c.option.EnableForwarding {
				// The reason we need to judge if the error code is equal to "Canceled" here is that
				// when we create a stream we use a goroutine to manually control the timeout of the connection.
				// There is no need to wait for the transport layer timeout which can reduce the time of unavailability.
				// But it conflicts with the retry mechanism since we use the error code to decide if it is caused by network error.
				// And actually the `Canceled` error can be regarded as a kind of network error in some way.
				if rpcErr, ok := status.FromError(err); ok && (errs.IsNetworkError(rpcErr.Code()) || rpcErr.Code() == codes.Canceled) {
					networkErrNum++
				}
			}
			cancel()
		} else {
			networkErrNum++
		}
		select {
		case <-ctx.Done():
			return err
		case <-ticker.C:
		}
	}

	if networkErrNum == constants.MaxRetryTimes {
		// encounter the network error
		backupClientConn, backupURL := c.backupClientConn()
		if backupClientConn != nil {
			log.Info("[tso] fall back to use follower to forward tso stream", zap.String("follower-url", backupURL))
			forwardedHost := c.getLeaderURL()
			if len(forwardedHost) == 0 {
				return errors.Errorf("cannot find the tso leader")
			}

			// create the follower stream
			cctx, cancel := context.WithCancel(ctx)
			cctx = grpcutil.BuildForwardContext(cctx, forwardedHost)
			stream, err = c.tsoStreamBuilderFactory.makeBuilder(backupClientConn).build(cctx, cancel, c.option.Timeout)
			if err == nil {
				forwardedHostTrim := tlsutil.TrimHTTPPrefix(forwardedHost)
				addr := tlsutil.TrimHTTPPrefix(backupURL)
				// the goroutine is used to check the network and change back to the original stream
				go c.checkLeader(ctx, cancel, forwardedHostTrim, addr, url, updateAndClear)
				metrics.RequestForwarded.WithLabelValues(forwardedHostTrim, addr).Set(1)
				updateAndClear(backupURL, &tsoConnectionContext{cctx, cancel, backupURL, stream})
				return nil
			}
			cancel()
		}
	}
	return err
}

func (c *Cli) checkLeader(
	ctx context.Context,
	forwardCancel context.CancelFunc,
	forwardedHostTrim, addr, url string,
	updateAndClear func(newAddr string, connectionCtx *tsoConnectionContext),
) {
	defer func() {
		// cancel the forward stream
		forwardCancel()
		metrics.RequestForwarded.WithLabelValues(forwardedHostTrim, addr).Set(0)
	}()
	cc, u := c.getTSOLeaderClientConn()
	var healthCli healthpb.HealthClient
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		// the tso leader change, we need to re-establish the stream
		if u != url {
			log.Info("[tso] the tso leader is changed", zap.String("origin", url), zap.String("new", u))
			return
		}
		if healthCli == nil && cc != nil {
			healthCli = healthpb.NewHealthClient(cc)
		}
		if healthCli != nil {
			healthCtx, healthCancel := context.WithTimeout(ctx, c.option.Timeout)
			resp, err := healthCli.Check(healthCtx, &healthpb.HealthCheckRequest{Service: ""})
			failpoint.Inject("unreachableNetwork", func() {
				resp.Status = healthpb.HealthCheckResponse_UNKNOWN
			})
			healthCancel()
			if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
				// create a stream of the original tso leader
				cctx, cancel := context.WithCancel(ctx)
				stream, err := c.tsoStreamBuilderFactory.makeBuilder(cc).build(cctx, cancel, c.option.Timeout)
				if err == nil && stream != nil {
					log.Info("[tso] recover the original tso stream since the network has become normal", zap.String("url", url))
					updateAndClear(url, &tsoConnectionContext{cctx, cancel, url, stream})
					return
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// To ensure we can get the latest tso leader and once it's changed, we can exit this function.
			cc, u = c.getTSOLeaderClientConn()
		}
	}
}

// tryConnectToTSOWithProxy will create multiple streams to all the service endpoints to work as
// a TSO proxy to reduce the pressure of the main serving service endpoint.
func (c *Cli) tryConnectToTSOWithProxy(
	ctx context.Context,
	connectionCtxs *sync.Map,
) error {
	tsoStreamBuilders := c.getAllTSOStreamBuilders()
	leaderAddr := c.svcDiscovery.GetServingURL()
	forwardedHost := c.getLeaderURL()
	if len(forwardedHost) == 0 {
		return errors.Errorf("cannot find the tso leader")
	}
	// GC the stale one.
	connectionCtxs.Range(func(addr, cc any) bool {
		addrStr := addr.(string)
		if _, ok := tsoStreamBuilders[addrStr]; !ok {
			log.Info("[tso] remove the stale tso stream",
				zap.String("addr", addrStr))
			cc.(*tsoConnectionContext).cancel()
			connectionCtxs.Delete(addr)
		}
		return true
	})
	// Update the missing one.
	for addr, tsoStreamBuilder := range tsoStreamBuilders {
		_, ok := connectionCtxs.Load(addr)
		if ok {
			continue
		}
		log.Info("[tso] try to create tso stream", zap.String("addr", addr))
		cctx, cancel := context.WithCancel(ctx)
		// Do not proxy the leader client.
		if addr != leaderAddr {
			log.Info("[tso] use follower to forward tso stream to do the proxy",
				zap.String("addr", addr))
			cctx = grpcutil.BuildForwardContext(cctx, forwardedHost)
		}
		// Create the TSO stream.
		stream, err := tsoStreamBuilder.build(cctx, cancel, c.option.Timeout)
		if err == nil {
			if addr != leaderAddr {
				forwardedHostTrim := tlsutil.TrimHTTPPrefix(forwardedHost)
				addrTrim := tlsutil.TrimHTTPPrefix(addr)
				metrics.RequestForwarded.WithLabelValues(forwardedHostTrim, addrTrim).Set(1)
			}
			connectionCtxs.Store(addr, &tsoConnectionContext{cctx, cancel, addr, stream})
			continue
		}
		log.Error("[tso] create the tso stream failed",
			zap.String("addr", addr), errs.ZapError(err))
		cancel()
	}
	return nil
}

// getAllTSOStreamBuilders returns a TSO stream builder for every service endpoint of TSO leader/followers
// or of keyspace group primary/secondaries.
func (c *Cli) getAllTSOStreamBuilders() map[string]tsoStreamBuilder {
	var (
		addrs          = c.svcDiscovery.GetServiceURLs()
		streamBuilders = make(map[string]tsoStreamBuilder, len(addrs))
		cc             *grpc.ClientConn
		err            error
	)
	for _, addr := range addrs {
		if len(addrs) == 0 {
			continue
		}
		if cc, err = c.svcDiscovery.GetOrCreateGRPCConn(addr); err != nil {
			continue
		}
		healthCtx, healthCancel := context.WithTimeout(c.ctx, c.option.Timeout)
		resp, err := healthpb.NewHealthClient(cc).Check(healthCtx, &healthpb.HealthCheckRequest{Service: ""})
		healthCancel()
		if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			streamBuilders[addr] = c.tsoStreamBuilderFactory.makeBuilder(cc)
		}
	}
	return streamBuilders
}

// tryCreateTSODispatcher will try to create the TSO dispatcher if it is not created yet.
func (c *Cli) tryCreateTSODispatcher() {
	// The dispatcher is already created.
	if c.getDispatcher() != nil {
		return
	}
	// The TSO leader is not ready.
	url := c.getLeaderURL()
	if len(url) == 0 {
		return
	}
	dispatcher := newTSODispatcher(c.ctx, defaultMaxTSOBatchSize, c)
	c.wg.Add(1)
	go dispatcher.handleDispatcher(&c.wg)
	// Try to set the dispatcher atomically.
	if swapped := c.dispatcher.CompareAndSwap(nil, dispatcher); !swapped {
		dispatcher.close()
	}
}

// DispatchRequest will send the TSO request to the corresponding TSO dispatcher.
func (c *Cli) DispatchRequest(request *Request) (bool, error) {
	if c.getDispatcher() == nil {
		err := errs.ErrClientGetTSO.FastGenByArgs("tso dispatcher is not ready")
		log.Error("[tso] dispatch tso request error", errs.ZapError(err))
		c.svcDiscovery.ScheduleCheckMemberChanged()
		// New dispatcher could be created in the meantime, which is retryable.
		return true, err
	}

	defer trace.StartRegion(request.requestCtx, "pdclient.tsoReqEnqueue").End()
	select {
	case <-request.requestCtx.Done():
		// Caller cancelled the request, no need to retry.
		return false, request.requestCtx.Err()
	case <-request.clientCtx.Done():
		// Client is closed, no need to retry.
		return false, request.clientCtx.Err()
	case <-c.ctx.Done():
		// tsoClient is closed due to the PD service mode switch, which is retryable.
		return true, c.ctx.Err()
	default:
		// This failpoint will increase the possibility that the request is sent to a closed dispatcher.
		failpoint.Inject("delayDispatchTSORequest", func() {
			time.Sleep(time.Second)
		})
		c.getDispatcher().push(request)
	}
	// Check the contexts again to make sure the request is not been sent to a closed dispatcher.
	// Never retry on these conditions to prevent unexpected data race.
	select {
	case <-request.requestCtx.Done():
		return false, request.requestCtx.Err()
	case <-request.clientCtx.Done():
		return false, request.clientCtx.Err()
	case <-c.ctx.Done():
		return false, c.ctx.Err()
	default:
	}
	return false, nil
}
