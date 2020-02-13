// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package input

import (
	regionPKG "github.com/pingcap-incubator/tidb-dashboard/pkg/keyvisual/region"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/cluster"
	"github.com/pingcap/pd/server/core"
)

const limit = 1024

// RegionsInfo implements the interface regionPKG.RegionsInfo for [] * core.RegionInfo.
type RegionsInfo []*core.RegionInfo

// Len returns the number of regions.
func (rs RegionsInfo) Len() int {
	return len(rs)
}

// GetKeys returns the sorted endpoint keys of all regions.
// Fixme: StartKey may not be equal to the EndKey of the previous region
func (rs RegionsInfo) GetKeys() []string {
	keys := make([]string, len(rs)+1)
	keys[0] = regionPKG.String(rs[0].GetStartKey())
	endKeys := keys[1:]
	for i, region := range rs {
		endKeys[i] = regionPKG.String(region.GetEndKey())
	}
	return keys
}

// GetValues returns the specified statistics of all regions, sorted by region start key.
func (rs RegionsInfo) GetValues(tag regionPKG.StatTag) []uint64 {
	values := make([]uint64, len(rs))
	switch tag {
	case regionPKG.WrittenBytes:
		for i, region := range rs {
			values[i] = region.GetBytesWritten()
		}
	case regionPKG.ReadBytes:
		for i, region := range rs {
			values[i] = region.GetBytesRead()
		}
	case regionPKG.WrittenKeys:
		for i, region := range rs {
			values[i] = region.GetKeysWritten()
		}
	case regionPKG.ReadKeys:
		for i, region := range rs {
			values[i] = region.GetKeysRead()
		}
	case regionPKG.Integration:
		for i, region := range rs {
			values[i] = region.GetBytesWritten() + region.GetBytesRead()
		}
	default:
		panic("unreachable")
	}
	return values
}

var emptyRegionsInfo RegionsInfo

// NewCorePeriodicGetter returns the regionPKG.RegionsInfoGenerator interface implemented by PD.
// It gets RegionsInfo directly from memory.
func NewCorePeriodicGetter(srv *server.Server) regionPKG.RegionsInfoGenerator {
	return func() (regionPKG.RegionsInfo, error) {
		rc := srv.GetRaftCluster()
		if rc == nil {
			return emptyRegionsInfo, nil
		}
		return clusterScan(rc), nil
	}
}

func clusterScan(rc *cluster.RaftCluster) RegionsInfo {
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

	// log.Info("Update key visual regions", zap.Int("total-length", len(regions)))
	return regions
}
