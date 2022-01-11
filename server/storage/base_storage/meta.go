// Copyright 2022 TiKV Project Authors.
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

package storage

import (
	"context"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server/core"
)

// MetaStorage defines the storage operations on the PD cluster meta info.
type MetaStorage interface {
	LoadMeta(meta *metapb.Cluster) (bool, error)
	SaveMeta(meta *metapb.Cluster) error
	LoadStore(storeID uint64, store *metapb.Store) (bool, error)
	SaveStore(store *metapb.Store) error
	SaveStoreWeight(storeID uint64, leader, region float64) error
	LoadStores(f func(store *core.StoreInfo)) error
	DeleteStore(store *metapb.Store) error
	RegionStorage
}

type RegionStorage interface {
	LoadRegion(regionID uint64, region *metapb.Region) (ok bool, err error)
	LoadRegions(ctx context.Context, f func(region *core.RegionInfo) []*core.RegionInfo) error
	LoadRegionsOnce(ctx context.Context, f func(region *core.RegionInfo) []*core.RegionInfo) error
	SaveRegion(region *metapb.Region) error
	DeleteRegion(region *metapb.Region) error
	Flush() error
	Close() error
}
