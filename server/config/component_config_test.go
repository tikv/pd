// Copyright 2019 PingCAP, Inc.
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

package config

import (
	"strings"

	. "github.com/pingcap/check"
)

var _ = Suite(&testComponentsConfigSuite{})

type testComponentsConfigSuite struct{}

func (s *testComponentsConfigSuite) TestDecodeAndEncode(c *C) {
	cfgData := `
log-level = "debug"
panic-when-unexpected-key-or-data = true

[pd]
endpoints = [
    "example.com:443",
]

[coprocessor]
split-region-on-table = true
batch-split-limit = 1
region-max-size = "12MB"

[rocksdb]
wal-recovery-mode = 1
wal-dir = "/var"
create-if-missing = false

[rocksdb.titan]
enabled = true
dirname = "bar"
max-background-gc = 9

[rocksdb.defaultcf]
block-size = "12KB"
disable-block-cache = false
bloom-filter-bits-per-key = 123
compression-per-level = [
    "no",
    "lz4",
]

[rocksdb.defaultcf.titan]
min-blob-size = "2018B"
discardable-ratio = 0.00156

[rocksdb.writecf]
block-size = "12KB"
disable-block-cache = false
bloom-filter-bits-per-key = 123
compression-per-level = [
    "no",
    "zstd",
]
`
	cfg := make(map[string]interface{})
	err := decodeConfigs(cfgData, cfg)
	c.Assert(err, IsNil)
	decoded := make(map[string]interface{})
	decoded["log-level"] = "debug"
	decoded["panic-when-unexpected-key-or-data"] = true
	pdMap := map[string]interface{}{"endpoints": []interface{}{"example.com:443"}}
	decoded["pd"] = pdMap
	copMap := map[string]interface{}{
		"split-region-on-table": true,
		"batch-split-limit":     int64(1),
		"region-max-size":       "12MB",
	}
	decoded["coprocessor"] = copMap
	titanMap := map[string]interface{}{
		"enabled":           true,
		"dirname":           "bar",
		"max-background-gc": int64(9),
	}
	defaultcfTitanMap := map[string]interface{}{
		"min-blob-size":     "2018B",
		"discardable-ratio": 0.00156,
	}
	defaultcfMap := map[string]interface{}{
		"block-size":                "12KB",
		"disable-block-cache":       false,
		"bloom-filter-bits-per-key": int64(123),
		"compression-per-level":     []interface{}{"no", "lz4"},
		"titan":                     defaultcfTitanMap,
	}
	writecfMap := map[string]interface{}{
		"block-size":                "12KB",
		"disable-block-cache":       false,
		"bloom-filter-bits-per-key": int64(123),
		"compression-per-level":     []interface{}{"no", "zstd"},
	}
	rocksdbMap := map[string]interface{}{
		"wal-recovery-mode": int64(1),
		"wal-dir":           "/var",
		"create-if-missing": false,
		"titan":             titanMap,
		"defaultcf":         defaultcfMap,
		"writecf":           writecfMap,
	}
	decoded["rocksdb"] = rocksdbMap
	c.Assert(cfg, DeepEquals, decoded)

	str, err := encodeConfigs(decoded)
	c.Assert(err, IsNil)
	encodedStr := `log-level = "debug"
panic-when-unexpected-key-or-data = true

[coprocessor]
  batch-split-limit = 1
  region-max-size = "12MB"
  split-region-on-table = true

[pd]
  endpoints = ["example.com:443"]

[rocksdb]
  create-if-missing = false
  wal-dir = "/var"
  wal-recovery-mode = 1
  [rocksdb.defaultcf]
    block-size = "12KB"
    bloom-filter-bits-per-key = 123
    compression-per-level = ["no", "lz4"]
    disable-block-cache = false
    [rocksdb.defaultcf.titan]
      discardable-ratio = 0.00156
      min-blob-size = "2018B"
  [rocksdb.titan]
    dirname = "bar"
    enabled = true
    max-background-gc = 9
  [rocksdb.writecf]
    block-size = "12KB"
    bloom-filter-bits-per-key = 123
    compression-per-level = ["no", "zstd"]
    disable-block-cache = false
`
	c.Assert(str, Equals, encodedStr)
}

func (s *testComponentsConfigSuite) TestUpdate(c *C) {
	cfg := make(map[string]interface{})
	defaultcfTitanMap := map[string]interface{}{
		"discardable-ratio": 0.00156,
	}
	defaultcfMap := map[string]interface{}{
		"block-size":            "12KB",
		"compression-per-level": []interface{}{"no", "lz4"},
		"titan":                 defaultcfTitanMap,
	}
	rocksdbMap := map[string]interface{}{
		"wal-recovery-mode": int64(1),
		"defaultcf":         defaultcfMap,
	}
	cfg["rocksdb"] = rocksdbMap
	update(cfg, strings.Split("rocksdb.defaultcf.titan.discardable-ratio", "."), "0.002")
	c.Assert(defaultcfTitanMap["discardable-ratio"], Equals, 0.002)
}
