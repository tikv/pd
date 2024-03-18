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
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/client/errs"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// TSOClient is the client used to get timestamps.
type TSOClient interface {
	// GetTS gets a timestamp from PD or TSO microservice.
	GetTS(ctx context.Context) (int64, int64, error)
	// GetTSAsync gets a timestamp from PD or TSO microservice, without block the caller.
	GetTSAsync(ctx context.Context) TSFuture
	// GetLocalTS gets a local timestamp from PD or TSO microservice.
	GetLocalTS(ctx context.Context, dcLocation string) (int64, int64, error)
	// GetLocalTSAsync gets a local timestamp from PD or TSO microservice, without block the caller.
	GetLocalTSAsync(ctx context.Context, dcLocation string) TSFuture
	// GetMinTS gets a timestamp from PD or the minimal timestamp across all keyspace groups from
	// the TSO microservice.
	GetMinTS(ctx context.Context) (int64, int64, error)
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

var tsoReqPool = sync.Pool{
	New: func() any {
		return &tsoRequest{
			done:     make(chan error, 1),
			physical: 0,
			logical:  0,
		}
	},
}

type tsoClient struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	option *option

	svcDiscovery ServiceDiscovery
	tsoStreamBuilderFactory
	// tsoAllocators defines the mapping {dc-location -> TSO allocator leader URL}
	tsoAllocators sync.Map // Store as map[string]string
	// tsoAllocServingURLSwitchedCallback will be called when any global/local
	// tso allocator leader is switched.
	tsoAllocServingURLSwitchedCallback []func()

	// tsoDispatcher is used to dispatch different TSO requests to
	// the corresponding dc-location TSO channel.
	tsoDispatcher sync.Map // Same as map[string]chan *tsoDispatcher
	// dc-location -> deadline
	tsDeadline sync.Map // Same as map[string]chan deadline
	// dc-location -> *tsoInfo while the tsoInfo is the last TSO info
	lastTSOInfoMap sync.Map // Same as map[string]*tsoInfo

	checkTSDeadlineCh         chan struct{}
	checkTSODispatcherCh      chan struct{}
	updateTSOConnectionCtxsCh chan struct{}
}

// newTSOClient returns a new TSO client.
func newTSOClient(
	ctx context.Context, option *option,
	svcDiscovery ServiceDiscovery, factory tsoStreamBuilderFactory,
) *tsoClient {
	ctx, cancel := context.WithCancel(ctx)
	c := &tsoClient{
		ctx:                       ctx,
		cancel:                    cancel,
		option:                    option,
		svcDiscovery:              svcDiscovery,
		tsoStreamBuilderFactory:   factory,
		checkTSDeadlineCh:         make(chan struct{}),
		checkTSODispatcherCh:      make(chan struct{}, 1),
		updateTSOConnectionCtxsCh: make(chan struct{}, 1),
	}

	eventSrc := svcDiscovery.(tsoAllocatorEventSource)
	eventSrc.SetTSOLocalServURLsUpdatedCallback(c.updateTSOLocalServURLs)
	eventSrc.SetTSOGlobalServURLUpdatedCallback(c.updateTSOGlobalServURL)
	c.svcDiscovery.AddServiceURLsSwitchedCallback(c.scheduleUpdateTSOConnectionCtxs)

	return c
}

func (c *tsoClient) Setup() {
	c.svcDiscovery.CheckMemberChanged()
	c.updateTSODispatcher()

	// Start the daemons.
	c.wg.Add(2)
	go c.tsoDispatcherCheckLoop()
	go c.tsCancelLoop()
}

// Close closes the TSO client
func (c *tsoClient) Close() {
	if c == nil {
		return
	}
	log.Info("closing tso client")

	c.cancel()
	c.wg.Wait()

	log.Info("close tso client")
	c.tsoDispatcher.Range(func(_, dispatcherInterface any) bool {
		if dispatcherInterface != nil {
			dispatcher := dispatcherInterface.(*tsoDispatcher)
			tsoErr := errors.WithStack(errClosing)
			dispatcher.tsoBatchController.revokePendingRequest(tsoErr)
			dispatcher.dispatcherCancel()
		}
		return true
	})

	log.Info("tso client is closed")
}

// GetTSOAllocators returns {dc-location -> TSO allocator leader URL} connection map
func (c *tsoClient) GetTSOAllocators() *sync.Map {
	return &c.tsoAllocators
}

// GetTSOAllocatorServingURLByDCLocation returns the tso allocator of the given dcLocation
func (c *tsoClient) GetTSOAllocatorServingURLByDCLocation(dcLocation string) (string, bool) {
	url, exist := c.tsoAllocators.Load(dcLocation)
	if !exist {
		return "", false
	}
	return url.(string), true
}

// GetTSOAllocatorClientConnByDCLocation returns the tso allocator grpc client connection
// of the given dcLocation
func (c *tsoClient) GetTSOAllocatorClientConnByDCLocation(dcLocation string) (*grpc.ClientConn, string) {
	url, ok := c.tsoAllocators.Load(dcLocation)
	if !ok {
		panic(fmt.Sprintf("the allocator leader in %s should exist", dcLocation))
	}
	// todo: if we support local tso forward, we should get or create client conns.
	cc, ok := c.svcDiscovery.GetClientConns().Load(url)
	if !ok {
		return nil, url.(string)
	}
	return cc.(*grpc.ClientConn), url.(string)
}

// AddTSOAllocatorServingURLSwitchedCallback adds callbacks which will be called
// when any global/local tso allocator service endpoint is switched.
func (c *tsoClient) AddTSOAllocatorServingURLSwitchedCallback(callbacks ...func()) {
	c.tsoAllocServingURLSwitchedCallback = append(c.tsoAllocServingURLSwitchedCallback, callbacks...)
}

func (c *tsoClient) updateTSOLocalServURLs(allocatorMap map[string]string) error {
	if len(allocatorMap) == 0 {
		return nil
	}

	updated := false

	// Switch to the new one
	for dcLocation, url := range allocatorMap {
		if len(url) == 0 {
			continue
		}
		oldURL, exist := c.GetTSOAllocatorServingURLByDCLocation(dcLocation)
		if exist && url == oldURL {
			continue
		}
		updated = true
		if _, err := c.svcDiscovery.GetOrCreateGRPCConn(url); err != nil {
			log.Warn("[tso] failed to connect dc tso allocator serving url",
				zap.String("dc-location", dcLocation),
				zap.String("serving-url", url),
				errs.ZapError(err))
			return err
		}
		c.tsoAllocators.Store(dcLocation, url)
		log.Info("[tso] switch dc tso local allocator serving url",
			zap.String("dc-location", dcLocation),
			zap.String("new-url", url),
			zap.String("old-url", oldURL))
	}

	// Garbage collection of the old TSO allocator primaries
	c.gcAllocatorServingURL(allocatorMap)

	if updated {
		c.scheduleCheckTSODispatcher()
	}

	return nil
}

func (c *tsoClient) updateTSOGlobalServURL(url string) error {
	c.tsoAllocators.Store(globalDCLocation, url)
	log.Info("[tso] switch dc tso global allocator serving url",
		zap.String("dc-location", globalDCLocation),
		zap.String("new-url", url))
	c.scheduleCheckTSODispatcher()
	return nil
}

func (c *tsoClient) gcAllocatorServingURL(curAllocatorMap map[string]string) {
	// Clean up the old TSO allocators
	c.tsoAllocators.Range(func(dcLocationKey, _ any) bool {
		dcLocation := dcLocationKey.(string)
		// Skip the Global TSO Allocator
		if dcLocation == globalDCLocation {
			return true
		}
		if _, exist := curAllocatorMap[dcLocation]; !exist {
			log.Info("[tso] delete unused tso allocator", zap.String("dc-location", dcLocation))
			c.tsoAllocators.Delete(dcLocation)
		}
		return true
	})
}

// backupClientConn gets a grpc client connection of the current reachable and healthy
// backup service endpoints randomly. Backup service endpoints are followers in a
// quorum-based cluster or secondaries in a primary/secondary configured cluster.
func (c *tsoClient) backupClientConn() (*grpc.ClientConn, string) {
	urls := c.svcDiscovery.GetBackupURLs()
	if len(urls) < 1 {
		return nil, ""
	}
	var (
		cc  *grpc.ClientConn
		err error
	)
	for i := 0; i < len(urls); i++ {
		url := urls[rand.Intn(len(urls))]
		if cc, err = c.svcDiscovery.GetOrCreateGRPCConn(url); err != nil {
			continue
		}
		healthCtx, healthCancel := context.WithTimeout(c.ctx, c.option.timeout)
		resp, err := healthpb.NewHealthClient(cc).Check(healthCtx, &healthpb.HealthCheckRequest{Service: ""})
		healthCancel()
		if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			return cc, url
		}
	}
	return nil, ""
}
