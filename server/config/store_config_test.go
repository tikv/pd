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

package config

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

func TestTiKVConfig(t *testing.T) {
	re := require.New(t)
	m := NewStoreConfigManager(nil)
	// case1: big region.
	{
		body := `{ "coprocessor": {
        "split-region-on-table": false,
        "batch-split-limit": 2,
        "region-max-size": "15GiB",
        "region-split-size": "10GiB",
        "region-max-keys": 144000000,
        "region-split-keys": 96000000,
        "consistency-check-method": "mvcc",
        "perf-level": 2
    	}}`
		var config StoreConfig
		re.NoError(json.Unmarshal([]byte(body), &config))
		m.update(&config)
		re.Equal(uint64(144000000), config.GetRegionMaxKeys())
		re.Equal(uint64(96000000), config.GetRegionSplitKeys())
		re.Equal(15*units.GiB/units.MiB, int(config.GetRegionMaxSize()))
		re.Equal(uint64(10*units.GiB/units.MiB), config.GetRegionSplitSize())
	}
	//case2: empty config.
	{
		body := `{}`
		var config StoreConfig
		re.NoError(json.Unmarshal([]byte(body), &config))

		re.Equal(uint64(1440000), config.GetRegionMaxKeys())
		re.Equal(uint64(960000), config.GetRegionSplitKeys())
		re.Equal(144, int(config.GetRegionMaxSize()))
		re.Equal(uint64(96), config.GetRegionSplitSize())
	}
}

func TestUpdateConfig(t *testing.T) {
	re := require.New(t)
	manager := NewTestStoreConfigManager([]string{"tidb.com"})
	manager.ObserveConfig(context.Background(), "tikv.com")
	re.Equal(uint64(144), manager.GetStoreConfig().GetRegionMaxSize())
	re.NotEqual(raftStoreV2, manager.GetStoreConfig().GetRegionMaxSize())
	manager.ObserveConfig(context.Background(), "tidb.com")
	re.Equal(uint64(10), manager.GetStoreConfig().GetRegionMaxSize())
	re.Equal(raftStoreV2, manager.GetStoreConfig().Engine)

	// case2: the config should not update if config is same expect some ignore field.
	c, err := manager.source.GetConfig(context.Background(), "tidb.com")
	re.NoError(err)
	re.True(manager.GetStoreConfig().Equal(c))

	client := &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
			TLSClientConfig:   &tls.Config{},
		},
	}
	manager = NewStoreConfigManager(client)
	re.Equal("http", manager.source.(*TiKVConfigSource).schema)
}

func TestParseConfig(t *testing.T) {
	re := require.New(t)
	m := NewStoreConfigManager(nil)
	body := `
{
"coprocessor":{
"split-region-on-table":false,
"batch-split-limit":10,
"region-max-size":"384MiB",
"region-split-size":"256MiB",
"region-max-keys":3840000,
"region-split-keys":2560000,
"consistency-check-method":"mvcc",
"enable-region-bucket":true,
"region-bucket-size":"96MiB",
"region-size-threshold-for-approximate":"384MiB",
"region-bucket-merge-size-ratio":0.33
},
"storage":{
	"engine":"raft-kv2"
}
}
`

	var config StoreConfig
	re.NoError(json.Unmarshal([]byte(body), &config))
	m.update(&config)
	re.Equal(uint64(96), config.GetRegionBucketSize())
	re.True(config.IsRaftKV2())
	re.Equal(raftStoreV2, config.Storage.Engine)
}

func TestMergeCheck(t *testing.T) {
	re := require.New(t)
	testdata := []struct {
		size      uint64
		mergeSize uint64
		keys      uint64
		mergeKeys uint64
		pass      bool
	}{{
		// case 1: the merged region size is smaller than the max region size
		size:      96 + 20,
		mergeSize: 20,
		keys:      1440000 + 200000,
		mergeKeys: 200000,
		pass:      true,
	}, {
		// case 2: the smallest region is 68MiB，it can't be merged again.
		size:      144 + 20,
		mergeSize: 20,
		keys:      1440000 + 200000,
		mergeKeys: 200000,
		pass:      true,
	}, {
		// case 3: the smallest region is 50MiB，it can be merged again.
		size:      144 + 2,
		mergeSize: 50,
		keys:      1440000 + 20000,
		mergeKeys: 500000,
		pass:      false,
	}, {
		// case4: the smallest region is 51MiB，it can't be merged again.
		size:      144 + 3,
		mergeSize: 50,
		keys:      1440000 + 30000,
		mergeKeys: 500000,
		pass:      true,
	}}
	config := &StoreConfig{}
	for _, v := range testdata {
		if v.pass {
			re.NoError(config.CheckRegionSize(v.size, v.mergeSize))
			re.NoError(config.CheckRegionKeys(v.keys, v.mergeKeys))
		} else {
			re.Error(config.CheckRegionSize(v.size, v.mergeSize))
			re.Error(config.CheckRegionKeys(v.keys, v.mergeKeys))
		}
	}
	// Test CheckRegionSize when the region split size is 0.
	config.RegionSplitSize = "100KiB"
	config.RegionSplitSizeMB = typeutil.ParseMBFromText(config.RegionSplitSize, defaultRegionSplitSize)
	re.Empty(config.GetRegionSplitSize())
	re.NoError(config.CheckRegionSize(defaultRegionMaxSize, 50))
}
