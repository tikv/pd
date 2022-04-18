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
	"encoding/json"
	"fmt"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/server/storage"
)

var _ = Suite(&testTiKVConfigSuite{})

type testTiKVConfigSuite struct{}

func (t *testTiKVConfigSuite) TestTiKVConfig(c *C) {
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
		c.Assert(json.Unmarshal([]byte(body), &config), IsNil)
		fmt.Println(config.Coprocessor)

		c.Assert(config.GetRegionMaxKeys(), Equals, uint64(144000000))
		c.Assert(config.GetRegionSplitKeys(), Equals, uint64(96000000))
		c.Assert(int(config.GetRegionMaxSize()), Equals, 15*1024)
		c.Assert(config.GetRegionSplitSize(), Equals, uint64(10*1024))
	}
	//case2: empty config.
	{
		body := `{}`
		var config StoreConfig
		c.Assert(json.Unmarshal([]byte(body), &config), IsNil)
		fmt.Println(config.Coprocessor)

		c.Assert(config.GetRegionMaxKeys(), Equals, uint64(1440000))
		c.Assert(config.GetRegionSplitKeys(), Equals, uint64(960000))
		c.Assert(int(config.GetRegionMaxSize()), Equals, 144)
		c.Assert(config.GetRegionSplitSize(), Equals, uint64(96))
	}
}

func (t *testTiKVConfigSuite) TestUpdateConfig(c *C) {
	manager := NewStoreConfigManager(nil)
	c.Assert(manager.schema, Equals, "http")
	var tlsConfig *SecurityConfig
	manager = NewStoreConfigManager(tlsConfig)
	c.Assert(manager.schema, Equals, "http")
	config := &StoreConfig{
		Coprocessor{
			RegionMaxSize: "15GiB",
		},
	}
	manager.UpdateConfig(config, nil)
	c.Assert(manager.GetStoreConfig(), NotNil)
	manager.UpdateConfig(config, nil)
	c.Assert(manager.GetStoreConfig().GetRegionMaxSize(), Equals, uint64(15*1024))
	var m StoreConfigManager
	m.UpdateConfig(nil, nil)
	c.Assert(m.GetStoreConfig().GetRegionMaxSize(), Equals, uint64(144))
}

func (t *testTiKVConfigSuite) TestReload(c *C) {
	manager := NewStoreConfigManager(nil)
	storage := storage.NewStorageWithMemoryBackend()
	c.Assert(manager.GetStoreConfig().GetRegionMaxSize(), Equals, uint64(144))
	c.Assert(manager.Reload(storage), IsNil)
	config := &StoreConfig{
		Coprocessor{
			RegionMaxSize: "15GiB",
		},
	}
	c.Assert(manager.GetStoreConfig().GetRegionMaxSize(), Equals, uint64(144))
	storage.SaveStoreConfig(config)
	c.Assert(manager.Reload(storage), IsNil)
	c.Assert(manager.GetStoreConfig().GetRegionMaxSize(), Equals, uint64(15*1024))
}
