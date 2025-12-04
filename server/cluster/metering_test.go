// Copyright 2025 TiKV Project Authors.
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
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/metering"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/server/config"
)

func TestCollectStorageSize(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	tc := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	count := 10000
	regions := newTestRegions(uint64(count), 3, 3)
	for _, region := range regions {
		err = tc.putRegion(region)
		re.NoError(err)
	}

	tc.regionLabeler, err = labeler.NewRegionLabeler(ctx, tc.storage, time.Second*5)
	re.NoError(err)
	keyspaceGroupManager := keyspace.NewKeyspaceGroupManager(ctx, tc.storage, tc.etcdClient)
	re.NoError(keyspaceGroupManager.Bootstrap(ctx))
	keyspaceManager := keyspace.NewKeyspaceManager(ctx, tc.storage, tc, mockid.NewIDAllocator(), &config.KeyspaceConfig{}, keyspaceGroupManager)
	for i := range 10 {
		_, err = keyspaceManager.CreateKeyspace(&keyspace.CreateKeyspaceRequest{
			Name:       fmt.Sprintf("test-keyspace-%d", i),
			CreateTime: time.Now().Unix(),
		})
		re.NoError(err)
	}

	storageSizeInfoList := tc.collectStorageSize(tc.regionLabeler, keyspaceManager)
	re.Len(storageSizeInfoList, 10)
	// Sort the storage size info list by the keyspace name.
	sort.Slice(storageSizeInfoList, func(i, j int) bool {
		return storageSizeInfoList[i].keyspaceName < storageSizeInfoList[j].keyspaceName
	})
	// Mock the storage size info since the region split won't work in the test.
	for i := range storageSizeInfoList {
		storageSizeInfoList[i].rowBasedStorageSize = uint64(i)
		storageSizeInfoList[i].columnBasedStorageSize = uint64(i)
	}

	collector := newStorageSizeCollector()
	collector.Collect(storageSizeInfoList)
	records := collector.Aggregate()
	re.Len(records, 10)
	// Sort the records by the keyspace name.
	sort.Slice(records, func(i, j int) bool {
		return records[i][metering.DataClusterIDField].(string) < records[j][metering.DataClusterIDField].(string)
	})
	for idx, record := range records {
		storageSizeInfo := storageSizeInfoList[idx]
		re.Equal(storageSizeInfo.keyspaceName, record[metering.DataClusterIDField])
		re.Equal(metering.NewBytesValue(storageSizeInfo.rowBasedStorageSize*units.MiB), record[meteringDataRowBasedStorageSizeField])
		re.Equal(metering.NewBytesValue(storageSizeInfo.columnBasedStorageSize*units.MiB), record[meteringDataColumnBasedStorageSizeField])
	}
}

func TestCollectDfsStats(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	tc := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	stores := newTestStores(4, "9.0.0")
	for _, store := range stores {
		err = tc.PutMetaStore(store.GetMeta())
		re.NoError(err)
		err = tc.setStore(store, core.SetStoreStats(&pdpb.StoreStats{
			Capacity:  100 * units.GiB,
			Available: 20 * units.GiB,
			UsedSize:  80 * units.GiB,
			Dfs: []*pdpb.DfsStatItem{
				{
					Scope: &pdpb.DfsStatScope{
						IsGlobal:  true,
						Component: "test-component",
					},
					WrittenBytes:  400,
					WriteRequests: 400,
				},
				{
					Scope: &pdpb.DfsStatScope{
						Component:  "test-component",
						KeyspaceId: 1,
					},
					WrittenBytes:  100,
					WriteRequests: 100,
				},
				{
					Scope: &pdpb.DfsStatScope{
						Component:  "test-component",
						KeyspaceId: 2,
					},
					WrittenBytes:  200,
					WriteRequests: 200,
				},
				{
					Scope: &pdpb.DfsStatScope{
						Component:  "test-component",
						KeyspaceId: 3,
					},
					WrittenBytes:  300,
					WriteRequests: 300,
				},
			},
		}))
		re.NoError(err)
	}

	tc.regionLabeler, err = labeler.NewRegionLabeler(ctx, tc.storage, time.Second*5)
	re.NoError(err)
	keyspaceGroupManager := keyspace.NewKeyspaceGroupManager(ctx, tc.storage, tc.etcdClient)
	re.NoError(keyspaceGroupManager.Bootstrap(ctx))
	keyspaceManager := keyspace.NewKeyspaceManager(ctx, tc.storage, tc, mockid.NewIDAllocator(), &config.KeyspaceConfig{}, keyspaceGroupManager)
	for i := range 3 {
		_, err = keyspaceManager.CreateKeyspace(&keyspace.CreateKeyspaceRequest{
			Name:       fmt.Sprintf("test-keyspace-%d", i+1),
			CreateTime: time.Now().Unix(),
		})
		re.NoError(err)
	}

	collector := newDfsStatsCollector()
	keyspaceDFSStats, storeCount := tc.collectDFSStats(keyspaceManager)
	re.Len(stores, storeCount)
	collector.Collect(keyspaceDFSStats)
	records := collector.Aggregate()
	re.Len(records, 4)
	// Sort the records by the keyspace name.
	sort.Slice(records, func(i, j int) bool {
		return records[i][metering.DataClusterIDField].(string) < records[j][metering.DataClusterIDField].(string)
	})
	storeNum := uint64(len(stores))
	for idx, record := range records {
		if idx == 0 {
			re.Empty(record[metering.DataClusterIDField])
			re.Equal(metering.NewBytesValue(400*storeNum), record[meteringDataDfsWrittenBytes])
			re.Equal(metering.NewRequestsValue(400*storeNum), record[meteringDataDfsWriteRequests])
		} else {
			re.Equal(fmt.Sprintf("test-keyspace-%d", idx), record[metering.DataClusterIDField])
			re.Equal(metering.NewBytesValue(uint64(idx*100)*storeNum), record[meteringDataDfsWrittenBytes])
			re.Equal(metering.NewRequestsValue(uint64(idx*100)*storeNum), record[meteringDataDfsWriteRequests])
		}
	}
}
