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

package config

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/schedule/types"
)

func TestSchedulerConfigJSON(t *testing.T) {
	for _, args := range [][]string{
		{string([]byte{0x74, 0x80, 0xff}), string([]byte{0x74, 0x80, 0xfe}), "range"},
		{"", "", "all"},
		{"a", "z", "text"},
	} {
		re := require.New(t)
		original := SchedulerConfig{
			Type:        types.SchedulerTypeCompatibleMap[types.ScatterRangeScheduler],
			Args:        args,
			Disable:     true,
			ArgsPayload: "payload",
		}
		data, err := json.Marshal(original)
		re.NoError(err)
		var decoded SchedulerConfig
		re.NoError(json.Unmarshal(data, &decoded))
		re.Equal(original, decoded)
		re.Equal(args, original.Args)
	}
}

func TestSchedulerConfigJSONCompatibility(t *testing.T) {
	re := require.New(t)
	var cfg SchedulerConfig
	re.NoError(json.Unmarshal([]byte(`{"type":"scatter-range","args":["a","z","range"],"disable":true,"args-payload":"payload"}`), &cfg))
	re.Equal([]string{"a", "z", "range"}, cfg.Args)
	re.True(cfg.Disable)
	re.Equal("payload", cfg.ArgsPayload)

	re.NoError(json.Unmarshal([]byte(`{"type":"scatter-range","args":["old","old","old"],"args-hex":["","","72616e6765"]}`), &cfg))
	re.Equal([]string{"", "", "range"}, cfg.Args)
	re.Error(json.Unmarshal([]byte(`{"type":"scatter-range","args-hex":["zz"]}`), &cfg))

	cfg = SchedulerConfig{Type: "evict-leader", Args: []string{"1"}}
	data, err := json.Marshal(cfg)
	re.NoError(err)
	re.JSONEq(`{"type":"evict-leader","args":["1"],"disable":false,"args-payload":""}`, string(data))
	re.NoError(json.Unmarshal([]byte(`{"type":"evict-leader","args":["1"],"args-hex":["zz"]}`), &cfg))
	re.Equal([]string{"1"}, cfg.Args)
}
