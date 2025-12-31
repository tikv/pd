// Copyright 2020 TiKV Project Authors.
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

package input

import (
	"go.uber.org/zap"

	"github.com/pingcap/log"
	regionpkg "github.com/pingcap/tidb-dashboard/pkg/keyvisual/region"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/server"
)

const limit = 1024

// RegionsInfo implements the interface regionpkg.RegionsInfo for []*core.RegionInfo.
type RegionsInfo []*core.RegionInfo

// Len returns the number of regions.
func (rs RegionsInfo) Len() int {
	return len(rs)
}

// GetKeys returns the sorted endpoint keys of all regions.
func (rs RegionsInfo) GetKeys() []string {
	keys := make([]string, len(rs)+1)
	keys[0] = regionpkg.String(rs[0].GetStartKey())
	endKeys := keys[1:]
	for i, region := range rs {
		endKeys[i] = regionpkg.String(region.GetEndKey())
	}
	return keys
}

// GetValues returns the specified statistics of all regions, sorted by region start key.
func (rs RegionsInfo) GetValues(tag regionpkg.StatTag) []uint64 {
	values := make([]uint64, len(rs))
	switch tag {
	case regionpkg.WrittenBytes:
		for i, region := range rs {
			values[i] = region.GetBytesWritten()
		}
	case regionpkg.ReadBytes:
		for i, region := range rs {
			values[i] = region.GetBytesRead()
		}
	case regionpkg.WrittenKeys:
		for i, region := range rs {
			values[i] = region.GetKeysWritten()
		}
	case regionpkg.ReadKeys:
		for i, region := range rs {
			values[i] = region.GetKeysRead()
		}
	case regionpkg.Integration:
		for i, region := range rs {
			values[i] = region.GetBytesWritten() + region.GetBytesRead()
		}
	default:
		panic("unreachable")
	}
	return values
}

var emptyRegionsInfo RegionsInfo

// NewCorePeriodicGetter returns the regionpkg.RegionsInfoGenerator interface implemented by PD.
// It gets RegionsInfo directly from memory.
func NewCorePeriodicGetter(srv *server.Server) regionpkg.RegionsInfoGenerator {
	return func() (regionpkg.RegionsInfo, error) {
		rc := srv.GetBasicCluster()
		if rc == nil {
			return emptyRegionsInfo, nil
		}
		return clusterScan(rc), nil
	}
}

func clusterScan(rc *core.BasicCluster) RegionsInfo {
	var startKey []byte
	endKey := []byte("")

	regions := make([]*core.RegionInfo, 0, limit)

	for {
		rs := rc.ScanRegions(startKey, endKey, limit)
		length := len(rs)
		if length == 0 {
			break
		}

		regions = append(regions, rs...)

		startKey = rs[length-1].GetEndKey()
		if len(startKey) == 0 {
			break
		}
	}

	log.Debug("update key visual regions", zap.Int("total-length", len(regions)))
	return regions
}
