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

package http

import (
	"fmt"
	"net/url"
)

// The following constants are the paths of PD HTTP APIs.
const (
	HotRead             = "/pd/api/v1/hotspot/regions/read"
	HotWrite            = "/pd/api/v1/hotspot/regions/write"
	Regions             = "/pd/api/v1/regions"
	regionByID          = "/pd/api/v1/region/id"
	regionByKey         = "/pd/api/v1/region/key"
	regionsByKey        = "/pd/api/v1/regions/key"
	regionsByStoreID    = "/pd/api/v1/regions/store"
	Stores              = "/pd/api/v1/stores"
	Store               = "/pd/api/v1/store"
	MinResolvedTSPrefix = "/pd/api/v1/min-resolved-ts"

	Leader         = "/pd/api/v1/leader"
	TransferLeader = "/pd/api/v1/leader/transfer"

	Schedulers = "/pd/api/v1/schedulers"
)

// RegionByID returns the path of PD HTTP API to get region by ID.
func RegionByID(regionID uint64) string {
	return fmt.Sprintf("%s/%d", regionByID, regionID)
}

// RegionByKey returns the path of PD HTTP API to get region by key.
func RegionByKey(key []byte) string {
	return fmt.Sprintf("%s/%s", regionByKey, url.QueryEscape(string(key)))
}

// RegionsByKey returns the path of PD HTTP API to scan regions with given start key, end key and limit parameters.
func RegionsByKey(startKey, endKey []byte, limit int) string {
	return fmt.Sprintf("%s?start_key=%s&end_key=%s&limit=%d",
		regionsByKey, url.QueryEscape(string(startKey)), url.QueryEscape(string(endKey)), limit)
}

// RegionsByStoreID returns the path of PD HTTP API to get regions by store ID.
func RegionsByStoreID(storeID uint64) string {
	return fmt.Sprintf("%s/%d", regionsByStoreID, storeID)
}

// LabelByStore returns the path of PD HTTP API to set store label.
func LabelByStore(storeID int64) string {
	return fmt.Sprintf("%s/%d/label", Store, storeID)
}

// TransferLeaderID returns the path of PD HTTP API to transfer leader by ID.
func TransferLeaderID(leaderID string) string {
	return fmt.Sprintf("%s/%s", TransferLeader, leaderID)
}
