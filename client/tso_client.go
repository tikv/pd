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

// TSOClient defines basic interface of the TSO client
// For test only
type TSOClient interface {
	// GetTSOAllocators returns {dc-location -> TSO allocator leader URL} connection map
	GetTSOAllocators() *sync.Map
}

type tsoRequest struct {
	start      time.Time
	clientCtx  context.Context
	requestCtx context.Context
	done       chan error
	physical   int64
	logical    int64
	keyspaceID uint32
	dcLocation string
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

type tsoClient struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup
	option *option

	keyspaceID   uint32
	svcDiscovery ServiceDiscovery
	tsoStreamBuilderFactory
	// tsoAllocators defines the mapping {dc-location -> TSO allocator leader URL}
	tsoAllocators sync.Map // Store as map[string]string
	// tsoAllocServingAddrSwitchedCallback will be called when any global/local
	// tso allocator leader is switched.
	tsoAllocServingAddrSwitchedCallback []func()

	// tsoDispatcher is used to dispatch different TSO requests to
	// the corresponding dc-location TSO channel.
	tsoDispatcher sync.Map // Same as map[string]chan *tsoRequest
	// dc-location -> deadline
	tsDeadline sync.Map // Same as map[string]chan deadline
	// dc-location -> *lastTSO
	lastTSMap sync.Map // Same as map[string]*lastTSO

	checkTSDeadlineCh         chan struct{}
	checkTSODispatcherCh      chan struct{}
	updateTSOConnectionCtxsCh chan struct{}
}

// newTSOClient returns a new TSO client.
func newTSOClient(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, option *option,
	keyspaceID uint32, svcDiscovery ServiceDiscovery, eventSrc tsoAllocatorEventSource, factory tsoStreamBuilderFactory) *tsoClient {
	c := &tsoClient{
		ctx:                       ctx,
		cancel:                    cancel,
		wg:                        wg,
		option:                    option,
		keyspaceID:                keyspaceID,
		svcDiscovery:              svcDiscovery,
		tsoStreamBuilderFactory:   factory,
		checkTSDeadlineCh:         make(chan struct{}),
		checkTSODispatcherCh:      make(chan struct{}, 1),
		updateTSOConnectionCtxsCh: make(chan struct{}, 1),
	}

	eventSrc.AddTSOServAddrsUpdatedCallback(c.updateTSOAllocatorServingAddrs)
	c.svcDiscovery.AddServiceAddrsSwitchedCallback(c.scheduleUpdateTSOConnectionCtxs)

	return c
}

func (c *tsoClient) setup() error {
	// Start the daemons.
	c.wg.Add(2)
	go c.tsLoop()
	go c.tsCancelLoop()

	if err := c.svcDiscovery.Init(); err != nil {
		return err
	}
	c.updateTSODispatcher()
	return nil
}

// Close closes the TSO client
func (c *tsoClient) Close() {
	c.tsoDispatcher.Range(func(_, dispatcherInterface interface{}) bool {
		if dispatcherInterface != nil {
			dispatcher := dispatcherInterface.(*tsoDispatcher)
			tsoErr := errors.WithStack(errClosing)
			dispatcher.tsoBatchController.revokePendingTSORequest(tsoErr)
			dispatcher.dispatcherCancel()
		}
		return true
	})
}

// GetTSOAllocators returns {dc-location -> TSO allocator leader URL} connection map
func (c *tsoClient) GetTSOAllocators() *sync.Map {
	return &c.tsoAllocators
}

// GetTSOAllocatorServingAddrByDCLocation returns the tso allocator of the given dcLocation
func (c *tsoClient) GetTSOAllocatorServingAddrByDCLocation(dcLocation string) (string, bool) {
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
	cc, ok := c.svcDiscovery.GetClientConns().Load(url)
	if !ok {
		panic(fmt.Sprintf("the client connection of %s in %s should exist", url, dcLocation))
	}
	return cc.(*grpc.ClientConn), url.(string)
}

// AddTSOAllocatorServingAddrSwitchedCallback adds callbacks which will be called
// when any global/local tso allocator service endpoint is switched.
func (c *tsoClient) AddTSOAllocatorServingAddrSwitchedCallback(callbacks ...func()) {
	c.tsoAllocServingAddrSwitchedCallback = append(c.tsoAllocServingAddrSwitchedCallback, callbacks...)
}

func (c *tsoClient) updateTSOAllocatorServingAddrs(allocatorMap map[string]string) error {
	if len(allocatorMap) == 0 {
		return nil
	}

	updated := false

	// Switch to the new one
	for dcLocation, addr := range allocatorMap {
		if len(addr) == 0 {
			continue
		}
		oldAddr, exist := c.GetTSOAllocatorServingAddrByDCLocation(dcLocation)
		if exist && addr == oldAddr {
			continue
		}
		updated = true
		if _, err := c.svcDiscovery.GetOrCreateGRPCConn(addr); err != nil {
			log.Warn("[pd[tso]] failed to connect dc tso allocator serving address",
				zap.String("dc-location", dcLocation),
				zap.String("serving-address", addr),
				errs.ZapError(err))
			return err
		}
		c.tsoAllocators.Store(dcLocation, addr)
		log.Info("[pd(tso)] switch dc tso allocatorimary",
			zap.String("dc-location", dcLocation),
			zap.String("new-address", addr),
			zap.String("old-address", oldAddr))
	}

	// Garbage collection of the old TSO allocator primaries
	c.gcAllocatorServingAddr(allocatorMap)

	if updated {
		c.scheduleCheckTSODispatcher()
	}

	return nil
}

func (c *tsoClient) gcAllocatorServingAddr(curAllocatorMap map[string]string) {
	// Clean up the old TSO allocators
	c.tsoAllocators.Range(func(dcLocationKey, _ interface{}) bool {
		dcLocation := dcLocationKey.(string)
		// Skip the Global TSO Allocator
		if dcLocation == globalDCLocation {
			return true
		}
		if _, exist := curAllocatorMap[dcLocation]; !exist {
			log.Info("[pd(tso)] delete unused tso allocator", zap.String("dc-location", dcLocation))
			c.tsoAllocators.Delete(dcLocation)
		}
		return true
	})
}

// backupClientConn gets a grpc client connection of the current reachable and healthy
// backup service endpoints randomly. Backup service endpoints are followers in a
// quorum-based cluster or secondaries in a primary/secondary configured cluster.
func (c *tsoClient) backupClientConn() (*grpc.ClientConn, string) {
	addrs := c.svcDiscovery.GetBackupAddrs()
	if len(addrs) < 1 {
		return nil, ""
	}
	var (
		cc  *grpc.ClientConn
		err error
	)
	for i := 0; i < len(addrs); i++ {
		addr := addrs[rand.Intn(len(addrs))]
		if cc, err = c.svcDiscovery.GetOrCreateGRPCConn(addr); err != nil {
			continue
		}
		healthCtx, healthCancel := context.WithTimeout(c.ctx, c.option.timeout)
		resp, err := healthpb.NewHealthClient(cc).Check(healthCtx, &healthpb.HealthCheckRequest{Service: ""})
		healthCancel()
		if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			return cc, addr
		}
	}
	return nil, ""
}
