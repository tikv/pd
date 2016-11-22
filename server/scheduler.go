// Copyright 2016 PingCAP, Inc.
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

package server

// scheduleLeader schedules a region to transfer leader from the source store to the target store.
func scheduleLeader(cluster *clusterInfo, s Selector) (*regionInfo, *storeInfo, *storeInfo) {
	sourceStores := cluster.getStores()

	source := s.SelectSource(sourceStores)
	if source == nil {
		return nil, nil, nil
	}

	region := cluster.randLeaderRegion(source.GetId())
	if region == nil {
		return nil, nil, nil
	}

	targetStores := cluster.getFollowerStores(region)

	target := s.SelectTarget(targetStores)
	if target == nil {
		return nil, nil, nil
	}

	return region, source, target
}

// scheduleStorage schedules a region to transfer peer from the source store to the target store.
func scheduleStorage(cluster *clusterInfo, s Selector) (*regionInfo, *storeInfo, *storeInfo) {
	stores := cluster.getStores()

	source := s.SelectSource(stores)
	if source == nil {
		return nil, nil, nil
	}

	region := cluster.randFollowerRegion(source.GetId())
	if region == nil {
		region = cluster.randLeaderRegion(source.GetId())
	}
	if region == nil {
		return nil, nil, nil
	}

	target := s.SelectTarget(stores, newExcludedFilter(nil, region.GetStoreIds()))
	if target == nil {
		return nil, nil, nil
	}

	return region, source, target
}
