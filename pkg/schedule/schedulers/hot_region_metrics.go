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

package schedulers

import (
	"math"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const hotRegionTopN = 10

// Keep immutable metric snapshots, rather than a GaugeVec retaining labels for
// every region ever seen. Scrapes read at most 30 metrics and never scan regions.
type hotRegionCollector struct {
	syncutil.RWMutex
	desc    *prometheus.Desc
	metrics [utils.RWTypeLen][]prometheus.Metric
}

func newHotRegionCollector() *hotRegionCollector {
	return &hotRegionCollector{desc: prometheus.NewDesc(
		"pd_hotspot_top_region_load",
		"Smoothed load of the top 10 distinct sustained hot regions per scheduling type; dim specifies the unit (byte/s, key/s, query/s, or native CPU usage). Replicated peer loads are represented by the maximum, not summed.",
		[]string{"type", "dim", "region", "store"}, nil)}
}

var topHotRegionMetrics = newHotRegionCollector()

func init() {
	prometheus.MustRegister(topHotRegionMetrics)
}

// Describe implements prometheus.Collector.
func (c *hotRegionCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect implements prometheus.Collector.
func (c *hotRegionCollector) Collect(ch chan<- prometheus.Metric) {
	// The slices and metrics are immutable after publication. Release the lock
	// before sending so a slow scrape cannot block the periodic producer.
	c.RLock()
	snapshot := c.metrics
	c.RUnlock()
	for _, metrics := range snapshot {
		for _, metric := range metrics {
			ch <- metric
		}
	}
}

func (c *hotRegionCollector) set(rw utils.RWType, metrics []prometheus.Metric) {
	c.Lock()
	c.metrics[rw] = metrics
	c.Unlock()
}

// ResetHotRegionMetrics removes all Top N samples, including on leadership loss.
func ResetHotRegionMetrics() {
	topHotRegionMetrics.Lock()
	topHotRegionMetrics.metrics = [utils.RWTypeLen][]prometheus.Metric{}
	topHotRegionMetrics.Unlock()
}

type hotRegionEntry struct {
	regionID uint64
	storeID  uint64
	load     float64
}

// hotter gives deterministic ordering, including replicas with equal load.
func (e hotRegionEntry) hotter(other hotRegionEntry) bool {
	if e.load != other.load {
		return e.load > other.load
	}
	if e.regionID != other.regionID {
		return e.regionID < other.regionID
	}
	return e.storeID < other.storeID
}

type hotRegionTop struct {
	entries [hotRegionTopN]hotRegionEntry
	count   int
}

// add keeps only ten entries, ordered hottest first. Deduplication also uses
// only these ten entries: a discarded replica cannot exceed the current cutoff,
// so retaining a cluster-sized region map is unnecessary. The cutoff rejects
// most candidates with a single comparison; insertion costs at most ten moves.
func (t *hotRegionTop) add(e hotRegionEntry) {
	if e.load <= 0 || math.IsNaN(e.load) || math.IsInf(e.load, 0) {
		return
	}
	if t.count == hotRegionTopN && !e.hotter(t.entries[t.count-1]) {
		return
	}
	for i := 0; i < t.count; i++ {
		if t.entries[i].regionID == e.regionID {
			if !e.hotter(t.entries[i]) {
				return
			}
			copy(t.entries[i:], t.entries[i+1:t.count])
			t.count--
			break
		}
	}
	pos := 0
	for pos < t.count && !e.hotter(t.entries[pos]) {
		pos++
	}
	if t.count < hotRegionTopN {
		t.count++
	}
	copy(t.entries[pos+1:t.count], t.entries[pos:t.count-1])
	t.entries[pos] = e
}

func selectHotRegions(cluster sche.SchedulerCluster, stats map[uint64][]*statistics.HotPeerStat, dim int, leaderOnly bool) hotRegionTop {
	var top hotRegionTop
	for storeID, peers := range stats {
		store := cluster.GetStore(storeID)
		if store == nil || store.IsRemoved() || store.IsUnhealthy() {
			continue
		}
		for _, peer := range peers {
			if leaderOnly && !peer.IsLeader() {
				continue
			}
			top.add(hotRegionEntry{regionID: peer.RegionID, storeID: storeID, load: peer.GetLoad(dim)})
		}
	}
	return top
}

// CollectHotRegionMetrics reuses the already filtered, denoised hot-cache
// snapshot used by hotspot monitoring. It is independent of scheduling being
// paused, hitting an operator limit, or producing an operator.
func (c *Controller) CollectHotRegionMetrics(rw utils.RWType, stats map[uint64][]*statistics.HotPeerStat) {
	c.RLock()
	defer c.RUnlock()
	var scheduler *hotScheduler
	if controller := c.schedulers[types.BalanceHotRegionScheduler.String()]; controller != nil {
		scheduler, _ = controller.Scheduler.(*hotScheduler)
	} else {
		scheduler, _ = c.schedulerHandlers[types.BalanceHotRegionScheduler.String()].(*hotScheduler)
	}
	if scheduler == nil {
		topHotRegionMetrics.set(rw, nil)
		return
	}
	metrics := make([]prometheus.Metric, 0, 2*hotRegionTopN)
	appendTop := func(ty resourceType, leaderOnly bool) {
		priorities := scheduler.getPriorities(c.cluster, ty)
		if len(priorities) == 0 {
			return
		}
		dim := utils.StringToDim(priorities[0])
		top := selectHotRegions(c.cluster, stats, dim, leaderOnly)
		for _, entry := range top.entries[:top.count] {
			metrics = append(metrics, prometheus.MustNewConstMetric(topHotRegionMetrics.desc, prometheus.GaugeValue, entry.load,
				ty.String(), utils.DimToString(dim), strconv.FormatUint(entry.regionID, 10), strconv.FormatUint(entry.storeID, 10)))
		}
	}
	if rw == utils.Read {
		appendTop(readPeer, false)
	} else {
		appendTop(writeLeader, true)
		appendTop(writePeer, false)
	}
	topHotRegionMetrics.set(rw, metrics)
}
