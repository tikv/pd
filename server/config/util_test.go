// Copyright 2018 TiKV Project Authors.
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
)

func TestValidateURLWithScheme(t *testing.T) {
	re := require.New(t)
	tests := []struct {
		addr   string
		hasErr bool
	}{
		{"", true},
		{"foo", true},
		{"/foo", true},
		{"http", true},
		{"http://", true},
		{"http://foo", false},
		{"https://foo", false},
		{"http://127.0.0.1", false},
		{"http://127.0.0.1/", false},
		{"https://foo.com/bar", false},
		{"https://foo.com/bar/", false},
	}
	for _, test := range tests {
		re.Equal(test.hasErr, ValidateURLWithScheme(test.addr) != nil)
	}
}

func TestFlattenConfigItems(t *testing.T) {
	toJSONStr := func(v interface{}) string {
		str, err := json.Marshal(v)
		require.NoError(t, err)
		return string(str)
	}

	jsonConf := `{
	"k0": 233333,
	"k1": "v1",
	"k2": ["v2-1", "v2-2", "v2-3"],
	"k3": [{"k3-1":"v3-1"}, {"k3-2":"v3-2"}, {"k3-3":"v3-3"}],
	"k4": {
		"k4-1": [1, 2, 3, 4],
		"k4-2": [5, 6, 7, 8],
		"k4-3": [666]
	}}`
	nested := make(map[string]interface{})
	require.NoError(t, json.Unmarshal([]byte(jsonConf), &nested))
	flatMap, err := FlattenConfigItems(nested)
	require.NoError(t, err)
	require.Equal(t, 7, len(flatMap))
	require.Equal(t, "233333", toJSONStr(flatMap["k0"]))
	require.Equal(t, "v1", flatMap["k1"])
	require.Equal(t, `["v2-1","v2-2","v2-3"]`, toJSONStr(flatMap["k2"]))
	require.Equal(t, `[{"k3-1":"v3-1"},{"k3-2":"v3-2"},{"k3-3":"v3-3"}]`, toJSONStr(flatMap["k3"]))
	require.Equal(t, `[1,2,3,4]`, toJSONStr(flatMap["k4.k4-1"]))
	require.Equal(t, `[5,6,7,8]`, toJSONStr(flatMap["k4.k4-2"]))
	require.Equal(t, `[666]`, toJSONStr(flatMap["k4.k4-3"]))
}
