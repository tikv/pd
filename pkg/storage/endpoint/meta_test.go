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

package endpoint

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
)

type testRegionBytesLoader struct {
	kv.Base
	called    bool
	lastLimit int
}

func (l *testRegionBytesLoader) LoadRangeValues(startKey, endKey string, limit int) ([][]byte, error) {
	l.called = true
	l.lastLimit = limit
	_, values, err := l.Base.LoadRange(startKey, endKey, limit)
	if err != nil {
		return nil, err
	}
	res := make([][]byte, 0, len(values))
	for _, value := range values {
		res = append(res, []byte(value))
	}
	return res, nil
}

func TestLoadRegionsWithBytesLoader(t *testing.T) {
	re := require.New(t)
	loader := &testRegionBytesLoader{Base: kv.NewMemoryKV()}
	storage := NewStorageEndpoint(loader, nil)
	region := &metapb.Region{
		Id:       1,
		StartKey: []byte("a"),
		EndKey:   []byte("b"),
	}
	re.NoError(storage.SaveRegion(region))

	var loaded []*core.RegionInfo
	re.NoError(storage.LoadRegions(context.Background(), func(region *core.RegionInfo) []*core.RegionInfo {
		loaded = append(loaded, region)
		return nil
	}))

	re.True(loader.called)
	re.Equal(MaxLocalKVRangeLimit, loader.lastLimit)
	re.Len(loaded, 1)
	re.Equal(region, loaded[0].GetMeta())
}

func TestLoadRegionsReturnsBeforePuttingDecodedBatchOnDecodeError(t *testing.T) {
	re := require.New(t)
	loader := &testRegionBytesLoader{Base: kv.NewMemoryKV()}
	storage := NewStorageEndpoint(loader, nil)
	for i := uint64(1); i <= minParallelRegionDecodeBatch+1; i++ {
		region := &metapb.Region{
			Id:       i,
			StartKey: []byte{byte(i >> 8), byte(i)},
			EndKey:   []byte{byte((i + 1) >> 8), byte(i + 1)},
		}
		re.NoError(storage.SaveRegion(region))
	}
	re.NoError(loader.Save(keypath.RegionPath(2), string([]byte{0xff})))

	var loaded []*core.RegionInfo
	err := storage.LoadRegions(context.Background(), func(region *core.RegionInfo) []*core.RegionInfo {
		loaded = append(loaded, region)
		return nil
	})
	re.Error(err)
	re.Empty(loaded)
}

func TestLoadRegionsWithLevelDBBytesLoader(t *testing.T) {
	re := require.New(t)
	levelDB, err := kv.NewLevelDBKV(t.TempDir())
	re.NoError(err)
	defer func() {
		re.NoError(levelDB.Close())
	}()
	storage := NewStorageEndpoint(levelDB, nil)
	regions := []*metapb.Region{
		{
			Id:       1,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		},
		{
			Id:       2,
			StartKey: []byte("b"),
			EndKey:   []byte("c"),
		},
	}
	for _, region := range regions {
		re.NoError(storage.SaveRegion(region))
	}

	values, err := levelDB.LoadRangeValues(keypath.RegionPath(0), keypath.RegionPath(math.MaxUint64), len(regions))
	re.NoError(err)
	re.Len(values, len(regions))
	for i, value := range values {
		region := &metapb.Region{}
		re.NoError(region.Unmarshal(value))
		re.Equal(regions[i], region)
	}

	var loaded []*core.RegionInfo
	re.NoError(storage.LoadRegions(context.Background(), func(region *core.RegionInfo) []*core.RegionInfo {
		loaded = append(loaded, region)
		return nil
	}))
	re.Len(loaded, len(regions))
	for i, region := range loaded {
		re.Equal(regions[i], region.GetMeta())
	}
}
