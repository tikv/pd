// Copyright 2021 TiKV Project Authors.
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

package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
)

func TestRegionState(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster := mockcluster.NewCluster(ctx, config.NewTestOptions())
	rsc := newRegionState(cluster)
	endTimestamp := time.Now().UnixNano()
	regions := []*core.RegionInfo{
		core.NewRegionInfo(
			&metapb.Region{
				Id: 1,
			},
			&metapb.Peer{
				Id:      1,
				StoreId: 1,
			},
			core.WithInterval(&pdpb.TimeInterval{
				StartTimestamp: uint64(endTimestamp - cluster.GetOpts().GetMaxStoreDownTime().Nanoseconds() - 10),
				EndTimestamp:   uint64(endTimestamp - cluster.GetOpts().GetMaxStoreDownTime().Nanoseconds()),
			}),
		),
		core.NewRegionInfo(
			&metapb.Region{
				Id: 2,
			},
			&metapb.Peer{
				Id:      2,
				StoreId: 2,
			},
			core.WithInterval(&pdpb.TimeInterval{
				StartTimestamp: uint64(endTimestamp - 10),
				EndTimestamp:   uint64(endTimestamp),
			}),
		),
	}

	rsc.observe(regions)
	re.Len(rsc.getRegionStateByType(regionStateDown), 1)
}

// GetRegionStateByType gets the states of the region by types. The regions here need to be cloned, otherwise, it may cause data race problems.
func (r *regionState) getRegionStateByType(typ regionStateType) []*core.RegionInfo {
	res := make([]*core.RegionInfo, 0, len(r.states[typ]))
	for _, r := range r.states[typ] {
		res = append(res, r.Clone())
	}
	return res
}
