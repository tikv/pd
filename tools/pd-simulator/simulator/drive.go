// Copyright 2017 TiKV Project Authors.
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

package simulator

import (
	"context"
	"math/rand"
	"net/http"
	"net/http/pprof"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	pdHttp "github.com/tikv/pd/client/http"
	sd "github.com/tikv/pd/client/servicediscovery"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/tools/pd-simulator/simulator/cases"
	"github.com/tikv/pd/tools/pd-simulator/simulator/config"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
)

// Driver promotes the cluster status change.
type Driver struct {
	wg            sync.WaitGroup
	pdAddr        string
	statusAddress string
	simCase       *cases.Case
	eventRunner   *EventRunner
	raftEngine    *RaftEngine
	conn          *Connection
	simConfig     *config.SimConfig
	pdConfig      *config.PDConfig

	tick struct {
		count      int64
		region     chan int64
		store      chan int64
		stepRegion chan int64
	}
}

// NewDriver returns a driver.
func NewDriver(pdAddr, statusAddress, caseName string, simConfig *config.SimConfig) (*Driver, error) {
	simCase := cases.NewCase(caseName, simConfig)
	if simCase == nil {
		return nil, errors.Errorf("failed to create case %s", caseName)
	}
	pdConfig := &config.PDConfig{}
	pdConfig.PlacementRules = simCase.Rules
	pdConfig.LocationLabels = simCase.Labels
	driver := Driver{
		pdAddr:        pdAddr,
		statusAddress: statusAddress,
		simCase:       simCase,
		simConfig:     simConfig,
		pdConfig:      pdConfig,
	}
	driver.tick.stepRegion = make(chan int64, 1)
	driver.tick.region = make(chan int64, 1)
	driver.tick.store = make(chan int64, 1)
	return &driver, nil
}

// Prepare initializes cluster information, bootstraps cluster and starts nodes.
func (d *Driver) Prepare() error {
	simutil.Logger.Info("prepare cluster")
	conn, err := NewConnection(d.simCase, d.simConfig)
	if err != nil {
		return err
	}
	d.conn = conn

	d.raftEngine = NewRaftEngine(d.simCase, d.conn, d.simConfig)
	d.eventRunner = NewEventRunner(d.simCase.Events, d.raftEngine)
	d.updateNodeAvailable()

	if d.statusAddress != "" {
		go d.runHTTPServer()
	}

	if err = d.allocID(); err != nil {
		return err
	}

	return d.Start()
}

func (d *Driver) allocID() error {
	// Bootstrap.
	store, region, err := d.GetBootstrapInfo(d.raftEngine)
	if err != nil {
		return err
	}

	leaderURL, pdCli, err := Bootstrap(context.Background(), d.pdAddr, store, region)
	if err != nil {
		simutil.Logger.Fatal("bootstrap error", zap.Error(err))
	} else {
		simutil.Logger.Debug("bootstrap success")
	}

	// Setup alloc id.
	// TODO: This is a hack way. Once we have reset alloc ID API, we need to replace it.
	maxID := simutil.IDAllocator.GetID()
	requestTimeout := 10 * time.Second
	etcdTimeout := 3 * time.Second
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{leaderURL},
		DialTimeout: etcdTimeout,
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	rootPath := path.Join("/pd", strconv.FormatUint(clusterID.Load(), 10))
	allocIDPath := path.Join(rootPath, "alloc_id")
	_, err = etcdClient.Put(ctx, allocIDPath, string(typeutil.Uint64ToBytes(maxID+1000)))
	if err != nil {
		cancel()
		return err
	}
	cancel()

	for {
		var resp *pdpb.AllocIDResponse
		resp, err = pdCli.AllocID(context.Background(), &pdpb.AllocIDRequest{
			Header: requestHeader(),
		})
		if err != nil {
			return errors.WithStack(err)
		}
		if resp.Id > maxID {
			simutil.IDAllocator.ResetID()
			break
		}
	}
	return nil
}

func (d *Driver) updateNodesClient() error {
	urls := strings.Split(d.pdAddr, ",")
	ctx, cancel := context.WithCancel(context.Background())
	SD = sd.NewDefaultPDServiceDiscovery(ctx, cancel, urls, nil)
	if err := SD.Init(); err != nil {
		return err
	}
	// Init PD HTTP client.
	PDHTTPClient = pdHttp.NewClientWithServiceDiscovery("pd-simulator", SD)

	for _, node := range d.conn.Nodes {
		node.client = newRetryClient(node)
	}
	return nil
}

// Tick invokes nodes' Tick.
func (d *Driver) Tick() {
	d.tick.count++
	curTick := d.tick.count
	go func() {
		d.tick.stepRegion <- curTick
	}()
	go func() {
		d.tick.region <- curTick
	}()
	go func() {
		d.tick.store <- curTick
	}()
}

// StepRegions steps regions.
func (d *Driver) StepRegions(ctx context.Context) {
	for {
		select {
		case tick := <-d.tick.stepRegion:
			d.raftEngine.stepRegions()
			d.eventRunner.Tick(tick)
			for _, n := range d.conn.Nodes {
				n.reportRegionChange()
				d.wg.Add(1)
				go n.Tick(&d.wg)
			}
			d.wg.Wait()
		case <-ctx.Done():
			return
		}
	}
}

// StoresHeartbeat sends heartbeat to all stores.
func (d *Driver) StoresHeartbeat(ctx context.Context) {
	config := d.raftEngine.storeConfig
	storeInterval := uint64(config.RaftStore.StoreHeartBeatInterval.Duration / config.SimTickInterval.Duration)
	var wg sync.WaitGroup
	for {
		select {
		case tick := <-d.tick.store:
			if uint64(tick)%storeInterval == 0 {
				for _, n := range d.conn.Nodes {
					wg.Add(1)
					go n.storeHeartBeat(&wg)
				}
				wg.Wait()
			}
		case <-ctx.Done():
			return
		}
	}
}

// RegionsHeartbeat sends heartbeat to all regions.
func (d *Driver) RegionsHeartbeat(ctx context.Context) {
	// ensure only wait for the first time heartbeat done
	firstReport := true
	config := d.raftEngine.storeConfig
	regionInterval := uint64(config.RaftStore.RegionHeartBeatInterval.Duration / config.SimTickInterval.Duration)
	nodesChannel := make(map[uint64]chan *core.RegionInfo, len(d.conn.Nodes))
	for _, n := range d.conn.Nodes {
		nodesChannel[n.Store.GetId()] = make(chan *core.RegionInfo, d.simConfig.TotalRegion)
		go func(storeID uint64, ch chan *core.RegionInfo) {
			for {
				select {
				case region := <-ch:
					d.conn.Nodes[storeID].regionHeartBeat(region)
				case <-ctx.Done():
					close(ch)
					return
				}
			}
		}(n.Store.GetId(), nodesChannel[n.Store.GetId()])
	}

	for {
		select {
		case tick := <-d.tick.region:
			if uint64(tick)%regionInterval == 0 {
				regions := d.raftEngine.GetRegions()
				healthyNodes := make(map[uint64]bool)
				for _, n := range d.conn.Nodes {
					if n.GetNodeState() != metapb.NodeState_Preparing && n.GetNodeState() != metapb.NodeState_Serving {
						healthyNodes[n.Store.GetId()] = false
					} else {
						healthyNodes[n.Store.GetId()] = true
					}
				}
				for _, region := range regions {
					hibernatePercent := d.simConfig.HibernatePercent
					// using rand(0,100) to meet hibernatePercent
					if !firstReport && rand.Intn(100) < hibernatePercent {
						continue
					}

					if region.GetLeader() != nil {
						storeID := region.GetLeader().GetStoreId()
						if healthy, ok := healthyNodes[storeID]; !ok || !healthy {
							continue
						}
						nodesChannel[storeID] <- region.Clone()
					}
				}

				// Only set haltSchedule to false when the leader count is 80% of the total region count.
				// using firstReport to avoid the haltSchedule set to true manually.
				if haltSchedule.Load() && firstReport {
					storeInterval := uint64(config.RaftStore.StoreHeartBeatInterval.Duration / config.SimTickInterval.Duration)
					ticker := time.NewTicker(time.Duration(storeInterval))
					for range ticker.C {
						// need to wait for first time heartbeat done
						stores, _ := PDHTTPClient.GetStores(ctx)
						var leaderCount int64
						for _, store := range stores.Stores {
							leaderCount += store.Status.LeaderCount
						}
						// Add halt schedule check to avoid the situation that the leader count is always less than 80%.
						if leaderCount > int64(float64(d.simConfig.TotalRegion)*0.8) || !haltSchedule.Load() {
							ChooseToHaltPDSchedule(false)
							firstReport = false
							ticker.Stop()
							simutil.Logger.Info("first region heartbeat done", zap.Int64("leaderCount", leaderCount), zap.Int("checkRegions", len(regions)))
							break
						}
					}
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

var haltSchedule atomic.Bool

// Check checks if the simulation is completed.
func (d *Driver) Check() bool {
	if !haltSchedule.Load() {
		return false
	}
	var stats []info.StoreStats
	var stores []*metapb.Store
	for _, s := range d.conn.Nodes {
		s.statsMutex.RLock()
		stores = append(stores, s.Store)
		stats = append(stats, *s.stats)
		s.statsMutex.RUnlock()
	}
	return d.simCase.Checker(stores, d.raftEngine.regionsInfo, stats)
}

// Start starts all nodes.
func (d *Driver) Start() error {
	simutil.Logger.Info("init nodes")
	if err := d.updateNodesClient(); err != nil {
		return err
	}

	for _, n := range d.conn.Nodes {
		err := n.Start()
		if err != nil {
			return err
		}
	}

	PutPDConfig(d.pdConfig)
	return nil
}

// Stop stops all nodes.
func (d *Driver) Stop() {
	for _, n := range d.conn.Nodes {
		n.Stop()
	}
}

// TickCount returns the simulation's tick count.
func (d *Driver) TickCount() int64 {
	return d.tick.count
}

// GetBootstrapInfo returns a valid bootstrap store and region.
func (d *Driver) GetBootstrapInfo(r *RaftEngine) (*metapb.Store, *metapb.Region, error) {
	origin := r.BootstrapRegion()
	if origin == nil {
		return nil, nil, errors.New("no region found for bootstrap")
	}
	region := origin.Clone(
		core.WithStartKey([]byte("")),
		core.WithEndKey([]byte("")),
		core.SetRegionConfVer(1),
		core.SetRegionVersion(1),
		core.SetPeers([]*metapb.Peer{origin.GetLeader()}),
	)
	if region.GetLeader() == nil {
		return nil, nil, errors.New("bootstrap region has no leader")
	}
	store := d.conn.Nodes[region.GetLeader().GetStoreId()]
	if store == nil {
		return nil, nil, errors.Errorf("bootstrap store %v not found", region.GetLeader().GetStoreId())
	}
	return store.Store, region.GetMeta(), nil
}

func (d *Driver) updateNodeAvailable() {
	for storeID, n := range d.conn.Nodes {
		n.statsMutex.Lock()
		if n.hasExtraUsedSpace {
			n.stats.StoreStats.Available = n.stats.StoreStats.Capacity - uint64(d.raftEngine.regionsInfo.GetStoreRegionSize(storeID)) - uint64(d.simConfig.RaftStore.ExtraUsedSpace)
		} else {
			n.stats.StoreStats.Available = n.stats.StoreStats.Capacity - uint64(d.raftEngine.regionsInfo.GetStoreRegionSize(storeID))
		}
		n.statsMutex.Unlock()
	}
}

func (d *Driver) runHTTPServer() {
	http.Handle("/metrics", promhttp.Handler())
	// profile API
	http.HandleFunc("/pprof/profile", pprof.Profile)
	http.HandleFunc("/pprof/trace", pprof.Trace)
	http.HandleFunc("/pprof/symbol", pprof.Symbol)
	http.Handle("/pprof/heap", pprof.Handler("heap"))
	http.Handle("/pprof/mutex", pprof.Handler("mutex"))
	http.Handle("/pprof/allocs", pprof.Handler("allocs"))
	http.Handle("/pprof/block", pprof.Handler("block"))
	http.Handle("/pprof/goroutine", pprof.Handler("goroutine"))
	eventHandler := newEventHandler(d.eventRunner)
	http.HandleFunc("/event", eventHandler.createEvent)
	// nolint
	http.ListenAndServe(d.statusAddress, nil)
}
