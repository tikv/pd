// Copyright 2020 TiKV Project Authors.
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

package typeutil

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBytesToUint64(t *testing.T) {
	re := require.New(t)
	str := "\x00\x00\x00\x00\x00\x00\x03\xe8"
	a, err := BytesToUint64([]byte(str))
	re.NoError(err)
	re.Equal(uint64(1000), a)
}

func TestUint64ToBytes(t *testing.T) {
	re := require.New(t)
	var a uint64 = 1000
	b := Uint64ToBytes(a)
	str := "\x00\x00\x00\x00\x00\x00\x03\xe8"
	re.Equal([]byte(str), b)
}

func TestJSONToUint64Slice(t *testing.T) {
	re := require.New(t)
	type testArray struct {
		Array []uint64 `json:"array"`
	}
	a := testArray{
		Array: []uint64{1, 2, 3},
	}
	bytes, _ := json.Marshal(a)
	var jsonStr map[string]any
	err := json.Unmarshal(bytes, &jsonStr)
	re.NoError(err)
	// valid case
	res, ok := JSONToUint64Slice(jsonStr["array"])
	re.True(ok)
	re.Equal(reflect.Uint64, reflect.TypeOf(res[0]).Kind())
	// invalid case
	_, ok = jsonStr["array"].([]uint64)
	re.False(ok)

	// invalid type
	type testArray1 struct {
		Array []string `json:"array"`
	}
	a1 := testArray1{
		Array: []string{"1", "2", "3"},
	}
	bytes, _ = json.Marshal(a1)
	var jsonStr1 map[string]any
	err = json.Unmarshal(bytes, &jsonStr1)
	re.NoError(err)
	res, ok = JSONToUint64Slice(jsonStr1["array"])
	re.False(ok)
	re.Nil(res)
}
