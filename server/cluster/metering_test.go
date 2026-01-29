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

	tc.regionLabeler = labeler.NewRegionLabeler(ctx, tc.storage)
	err = tc.regionLabeler.Initialize(time.Millisecond * 10)
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
