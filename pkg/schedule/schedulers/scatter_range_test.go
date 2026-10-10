// Copyright 2024 TiKV Project Authors.
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
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/versioninfo"
)

func TestScatterRangeBalance(t *testing.T) {
	re := require.New(t)
	checkScatterRangeBalance(re, false /* disable placement rules */)
	checkScatterRangeBalance(re, true /* enable placement rules */)
}

func checkScatterRangeBalance(re *require.Assertions, enablePlacementRules bool) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetEnablePlacementRules(enablePlacementRules)
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 3)
	// range cluster use a special tolerant ratio, cluster opt take no impact
	tc.SetTolerantSizeRatio(10000)
	// Add stores 1,2,3,4,5.
	tc.AddRegionStore(1, 0)
	tc.AddRegionStore(2, 0)
	tc.AddRegionStore(3, 0)
	tc.AddRegionStore(4, 0)
	tc.AddRegionStore(5, 0)
	var (
		id      uint64
		regions []*metapb.Region
	)
	for i := range 50 {
		peers := []*metapb.Peer{
			{Id: id + 1, StoreId: 1},
			{Id: id + 2, StoreId: 2},
			{Id: id + 3, StoreId: 3},
		}
		regions = append(regions, &metapb.Region{
			Id:       id + 4,
			Peers:    peers,
			StartKey: []byte(fmt.Sprintf("s_%02d", i)),
			EndKey:   []byte(fmt.Sprintf("s_%02d", i+1)),
		})
		id += 4
	}
	// empty region case
	regions[49].EndKey = []byte("")
	for _, meta := range regions {
		leader := rand.IntN(4) % 3
		regionInfo := core.NewRegionInfo(
			meta,
			meta.Peers[leader],
			core.SetApproximateKeys(1),
			core.SetApproximateSize(1),
		)
		origin, overlaps, rangeChanged := tc.SetRegion(regionInfo)
		tc.UpdateSubTree(regionInfo, origin, overlaps, rangeChanged)
	}
	for range 100 {
		_, err := tc.AllocPeer(1)
		re.NoError(err)
	}
	for i := 1; i <= 5; i++ {
		tc.UpdateStoreStatus(uint64(i))
	}

	hb, err := CreateScheduler(types.ScatterRangeScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.ScatterRangeScheduler, []string{"s_00", "s_50", "t"}))
	re.NoError(err)

	scheduleAndApplyOperator(tc, hb, 100)
	for i := 1; i <= 5; i++ {
		leaderCount := tc.GetStoreLeaderCount(uint64(i))
		re.LessOrEqual(leaderCount, 12)
		regionCount = tc.GetStoreRegionCount(uint64(i))
		re.LessOrEqual(regionCount, 32)
	}
}

// TestScatterRangeConfigKeepsRawKeys is a regression test for
// https://github.com/tikv/pd/issues/9670. Raw keys are usually not valid
// UTF-8, and they must not change after the config is persisted and loaded.
func TestScatterRangeConfigKeepsRawKeys(t *testing.T) {
	re := require.New(t)
	cancel, _, _, oc := prepareSchedulersTest()
	defer cancel()

	// The key from the issue. 0x80 is not valid UTF-8.
	startKey := string([]byte{0x74, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x17, '_', 'i'})
	endKey := string([]byte{0x74, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x18, 0xff, 0xfe})
	store := storage.NewStorageWithMemoryBackend()

	// Create the scheduler from args, which is what the HTTP API does.
	s, err := CreateScheduler(types.ScatterRangeScheduler, oc, store,
		ConfigSliceDecoder(types.ScatterRangeScheduler, []string{startKey, endKey, "test"}),
		func(string) error { return nil })
	re.NoError(err)
	sr := s.(*scatterRangeScheduler)
	re.NoError(sr.config.persist())

	// Create the scheduler again from the persisted config, which is what a
	// new PD leader does.
	data, err := store.LoadSchedulerConfig(s.GetName())
	re.NoError(err)
	s2, err := CreateScheduler(types.ScatterRangeScheduler, oc, store,
		ConfigJSONDecoder([]byte(data)), func(string) error { return nil })
	re.NoError(err)
	sr2 := s2.(*scatterRangeScheduler)
	re.Equal(s.GetName(), s2.GetName())
	re.Equal([]byte(startKey), sr2.config.getStartKey())
	re.Equal([]byte(endKey), sr2.config.getEndKey())
	re.Equal("test", sr2.config.getRangeName())

	// ReloadConfig also keeps the raw keys.
	re.NoError(sr2.ReloadConfig())
	re.Equal([]byte(startKey), sr2.config.getStartKey())
	re.Equal([]byte(endKey), sr2.config.getEndKey())

	// The encoded config keeps the old string fields.
	encoded, err := sr.EncodeConfig()
	re.NoError(err)
	m := make(map[string]any)
	re.NoError(json.Unmarshal(encoded, &m))
	re.Equal("test", m["range-name"])
	re.Contains(m, "start-key")
	re.Contains(m, "end-key")
	re.Equal("7480000000000001175f69", m["start-key-hex"])
}

func TestScatterRangeConfigJSONCompatibility(t *testing.T) {
	re := require.New(t)

	// A config persisted by an older version has no hex fields.
	conf := &scatterRangeSchedulerConfig{}
	re.NoError(json.Unmarshal([]byte(`{"range-name":"test","start-key":"a_00","end-key":"a_99"}`), conf))
	re.Equal("test", conf.RangeName)
	re.Equal("a_00", conf.StartKey)
	re.Equal("a_99", conf.EndKey)

	// Empty keys mean the whole key space and do not need the hex fields.
	conf = &scatterRangeSchedulerConfig{RangeName: "test"}
	data, err := json.Marshal(conf)
	re.NoError(err)
	re.JSONEq(`{"range-name":"test","start-key":"","end-key":""}`, string(data))

	// The hex fields take precedence over the string fields.
	conf = &scatterRangeSchedulerConfig{}
	re.NoError(json.Unmarshal([]byte(`{"range-name":"test","start-key":"t\ufffd","end-key":"","start-key-hex":"7480"}`), conf))
	re.Equal(string([]byte{0x74, 0x80}), conf.StartKey)
	re.Empty(conf.EndKey)

	conf = &scatterRangeSchedulerConfig{}
	re.NoError(json.Unmarshal([]byte(`{"range-name":"test","start-key":"a_00","end-key":"a_99","start-key-hex":"","end-key-hex":""}`), conf))
	re.Empty(conf.StartKey)
	re.Empty(conf.EndKey)

	// A broken hex field is an error.
	conf = &scatterRangeSchedulerConfig{}
	re.Error(json.Unmarshal([]byte(`{"range-name":"test","start-key":"","end-key":"","start-key-hex":"zz"}`), conf))
}
