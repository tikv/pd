// Copyright 2018 TiKV Project Authors.
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

package cases

import (
	"bytes"
	"fmt"
	"os"

	"github.com/docker/go-units"
	"github.com/go-echarts/go-echarts/charts"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	sc "github.com/tikv/pd/tools/pd-simulator/simulator/config"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
	"go.uber.org/zap"
)

func newImportData(config *sc.SimConfig) *Case {
	var simCase Case
	totalStore := config.TotalStore
	totalRegion := config.TotalRegion
	replica := int(config.ServerConfig.Replication.MaxReplicas)
	allStores := make(map[uint64]struct{}, totalStore)
	// Initialize the cluster
	for range totalStore {
		id := simutil.IDAllocator.NextID()
		simCase.Stores = append(simCase.Stores, &Store{
			ID:     id,
			Status: metapb.StoreState_Up,
		})
		allStores[id] = struct{}{}
	}

	for i := range totalRegion {
		peers := make([]*metapb.Peer, 0, replica)
		for j := range replica {
			peers = append(peers, &metapb.Peer{
				Id:      IDAllocator.nextID(),
				StoreId: uint64((i+j)%totalStore + 1),
			})
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:     IDAllocator.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   32 * units.MiB,
			Keys:   320000,
		})
	}

	simCase.RegionSplitSize = 64 * units.MiB
	simCase.RegionSplitKeys = 640000
	simCase.TableNumber = 10
	// Events description
	e := &WriteFlowOnSpotDescriptor{}
	table12 := string(codec.EncodeBytes(codec.GenerateTableKey(12)))
	table13 := string(codec.EncodeBytes(codec.GenerateTableKey(13)))
	e.Step = func(tick int64) map[string]int64 {
		if tick > int64(totalRegion)/10 {
			return nil
		}
		return map[string]int64{
			table12: 32 * units.MiB,
		}
	}
	simCase.Events = []EventDescriptor{e}

	// Checker description
	checkCount := uint64(0)
	var newRegionCount [][3]int
	var allRegionCount [][3]int
	simCase.Checker = func(stores []*metapb.Store, regions *core.RegionsInfo, _ []info.StoreStats) bool {
		for _, store := range stores {
			if store.NodeState == metapb.NodeState_Removed {
				delete(allStores, store.GetId())
			}
		}

		leaderDist := make(map[uint64]int)
		peerDist := make(map[uint64]int)
		leaderTotal := 0
		peerTotal := 0
		res := make([]*core.RegionInfo, 0, 100)
		regions.ScanRegionWithIterator([]byte(table12), func(region *core.RegionInfo) bool {
			if bytes.Compare(region.GetEndKey(), []byte(table13)) < 0 {
				res = append(res, regions.GetRegion(region.GetID()))
				return true
			}
			return false
		})

		for _, r := range res {
			leaderTotal++
			leaderDist[r.GetLeader().GetStoreId()]++
			for _, p := range r.GetPeers() {
				peerDist[p.GetStoreId()]++
				peerTotal++
			}
		}
		if leaderTotal == 0 || peerTotal == 0 {
			return false
		}
		tableLeaderLog := fmt.Sprintf("%d leader:", leaderTotal)
		tablePeerLog := fmt.Sprintf("%d peer: ", peerTotal)
		for storeID := 1; storeID <= 10; storeID++ {
			if leaderCount, ok := leaderDist[uint64(storeID)]; ok {
				tableLeaderLog = fmt.Sprintf("%s [store %d]:%.2f%%", tableLeaderLog, storeID, float64(leaderCount)/float64(leaderTotal)*100)
			}
		}
		for storeID := range allStores {
			if peerCount, ok := peerDist[storeID]; ok {
				newRegionCount = append(newRegionCount, [3]int{int(storeID), int(checkCount), peerCount})
				tablePeerLog = fmt.Sprintf("%s [store %d]:%.2f%%", tablePeerLog, storeID, float64(peerCount)/float64(peerTotal)*100)
			}
		}
		regionTotal := regions.GetTotalRegionCount()
		totalLeaderLog := fmt.Sprintf("%d leader:", regionTotal)
		totalPeerLog := fmt.Sprintf("%d peer:", regionTotal*3)
		isEnd := false
		var regionProps []float64
		for storeID := range allStores {
			totalLeaderLog = fmt.Sprintf("%s [store %d]:%.2f%%", totalLeaderLog, storeID, float64(regions.GetStoreLeaderCount(storeID))/float64(regionTotal)*100)
			regionProp := float64(regions.GetStoreRegionCount(storeID)) / float64(regionTotal*3) * 100
			regionProps = append(regionProps, regionProp)
			totalPeerLog = fmt.Sprintf("%s [store %d]:%.2f%%", totalPeerLog, storeID, regionProp)
			allRegionCount = append(allRegionCount, [3]int{int(storeID), int(checkCount), regions.GetStoreRegionCount(storeID)})
		}
		simutil.Logger.Info("import data information",
			zap.String("table-leader", tableLeaderLog),
			zap.String("table-peer", tablePeerLog),
			zap.String("total-leader", totalLeaderLog),
			zap.String("total-peer", totalPeerLog))
		checkCount++
		dev := 0.0
		for _, p := range regionProps {
			dev += (p - 10) * (p - 10) / 100
		}
		if dev > 0.02 {
			simutil.Logger.Warn("Not balanced, change scheduler or store limit", zap.Float64("dev score", dev))
		}
		if checkCount > uint64(totalRegion)/5 {
			isEnd = true
		} else if checkCount > uint64(totalRegion)/10 {
			isEnd = dev < 0.01
		}
		if isEnd {
			renderPlot("new_region.html", newRegionCount, int(checkCount), 0, totalRegion/10)
			renderPlot("all_region.html", allRegionCount, int(checkCount), 28*totalRegion/100, totalRegion/3)
		}
		return isEnd
	}
	return &simCase
}

func renderPlot(name string, data [][3]int, len, minCount, maxCount int) {
	var rangeColor = []string{
		"#313695", "#4575b4", "#74add1", "#abd9e9", "#e0f3f8",
		"#fee090", "#fdae61", "#f46d43", "#d73027", "#a50026",
	}
	bar3d := charts.NewBar3D()
	bar3d.SetGlobalOptions(
		charts.TitleOpts{Title: "Region count"},
		charts.VisualMapOpts{
			Range:      []float32{float32(minCount), float32(maxCount)},
			Calculable: true,
			InRange:    charts.VMInRange{Color: rangeColor},
			Min:        float32(minCount),
			Max:        float32(maxCount),
		},
		charts.Grid3DOpts{BoxDepth: 100, BoxWidth: 300},
	)
	xAxis := make([]int, 10)
	for i := 1; i <= 10; i++ {
		xAxis[i-1] = i
	}
	yAxis := make([]int, len)
	for i := 1; i <= len; i++ {
		yAxis[i-1] = i
	}
	bar3d.AddXYAxis(xAxis, yAxis).AddZAxis("bar3d", data)
	f, _ := os.Create(name)
	err := bar3d.Render(f)
	if err != nil {
		log.Error("render error", zap.Error(err))
	}
}
