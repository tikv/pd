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
	"net/http"
	"net/http/pprof"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/tools/pd-simulator/simulator/cases"
	"github.com/tikv/pd/tools/pd-simulator/simulator/config"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// Driver promotes the cluster status change.
type Driver struct {
	wg            sync.WaitGroup
	pdAddr        string
	statusAddress string
	simCase       *cases.Case
	client        Client
	tickCount     int64
	eventRunner   *EventRunner
	raftEngine    *RaftEngine
	conn          *Connection
	simConfig     *config.SimConfig
	pdConfig      *config.PDConfig

	regionTickc chan int64
	storeTickc  chan int64
	tickc       chan struct{}
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
	return &Driver{
		pdAddr:        pdAddr,
		statusAddress: statusAddress,
		simCase:       simCase,
		simConfig:     simConfig,
		pdConfig:      pdConfig,
		tickc:         make(chan struct{}, 1),
		regionTickc:   make(chan int64, 1),
		storeTickc:    make(chan int64, 1),
	}, nil
}

// Prepare initializes cluster information, bootstraps cluster and starts nodes.
func (d *Driver) Prepare() error {
	conn, err := NewConnection(d.simCase, d.pdAddr, d.simConfig)
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
	// Bootstrap.
	store, region, err := d.GetBootstrapInfo(d.raftEngine)
	if err != nil {
		return err
	}
	d.client = d.conn.Nodes[store.GetId()].client

	ctx, cancel := context.WithTimeout(context.Background(), pdTimeout)
	err = d.client.Bootstrap(ctx, store, region)
	cancel()
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
		Endpoints:   []string{d.pdAddr},
		DialTimeout: etcdTimeout,
	})
	if err != nil {
		return err
	}
	ctx, cancel = context.WithTimeout(context.Background(), requestTimeout)
	clusterID := d.client.GetClusterID(ctx)
	rootPath := path.Join("/pd", strconv.FormatUint(clusterID, 10))
	allocIDPath := path.Join(rootPath, "alloc_id")
	_, err = etcdClient.Put(ctx, allocIDPath, string(typeutil.Uint64ToBytes(maxID+1000)))
	if err != nil {
		cancel()
		return err
	}
	cancel()

	for {
		var id uint64
		id, err = d.client.AllocID(context.Background())
		if err != nil {
			return errors.WithStack(err)
		}
		if id > maxID {
			simutil.IDAllocator.ResetID()
			break
		}
	}

	err = d.Start()
	if err != nil {
		return err
	}

	return nil
}

// Tick invokes nodes' Tick.
func (d *Driver) Tick() {
	d.tickCount++
	go func() {
		d.tickc <- struct{}{}
	}()
	go func() {
		d.regionTickc <- d.tickCount
	}()
	go func() {
		d.storeTickc <- d.tickCount
	}()
}

func (d *Driver) StepRegions(ctx context.Context) {
	for {
		select {
		case <-d.tickc:
			d.raftEngine.stepRegions()
			d.eventRunner.Tick(d.tickCount)
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

func (d *Driver) StoresHeartbeat(ctx context.Context) {
	config := d.raftEngine.storeConfig
	storeInterval := uint64(config.RaftStore.StoreHeartBeatInterval.Duration / config.SimTickInterval.Duration)
	var wg sync.WaitGroup
	for {
		select {
		case tick := <-d.storeTickc:
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

var schedule sync.Once

func (d *Driver) RegionsHeartbeat(ctx context.Context) {
	config := d.raftEngine.storeConfig
	regionInterval := uint64(config.RaftStore.RegionHeartBeatInterval.Duration / config.SimTickInterval.Duration)
	var wg sync.WaitGroup
	// simulator don't need any schedulers util all regions send their heartbeat.
	ChooseToHaltPDSchedule(true)
	for {
		select {
		case tick := <-d.regionTickc:
			if uint64(tick)%regionInterval == 0 {
				for _, n := range d.conn.Nodes {
					wg.Add(1)
					go n.regionHeartBeat(&wg)
				}
				wg.Wait()
				schedule.Do(func() {
					ChooseToHaltPDSchedule(false)
				})
			}
		case <-ctx.Done():
			return
		}
	}
}

// Check checks if the simulation is completed.
func (d *Driver) Check() bool {
	var stats []info.StoreStats
	for _, n := range d.conn.Nodes {
		stats = append(stats, *n.stats)
	}

	return d.simCase.Checker(d.raftEngine.regionsInfo, stats)
}

// Start starts all nodes.
func (d *Driver) Start() error {
	for _, n := range d.conn.Nodes {
		err := n.Start()
		if err != nil {
			return err
		}
	}
	d.ChangePDConfig()
	return nil
}

// ChangePDConfig changes pd config
func (d *Driver) ChangePDConfig() error {
	d.client.PutPDConfig(d.pdConfig)
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
	return d.tickCount
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
		if n.hasExtraUsedSpace {
			n.stats.StoreStats.Available = n.stats.StoreStats.Capacity - uint64(d.raftEngine.regionsInfo.GetStoreRegionSize(storeID)) - uint64(d.simConfig.RaftStore.ExtraUsedSpace)
		} else {
			n.stats.StoreStats.Available = n.stats.StoreStats.Capacity - uint64(d.raftEngine.regionsInfo.GetStoreRegionSize(storeID))
		}
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
