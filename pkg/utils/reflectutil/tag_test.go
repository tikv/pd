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

package reflectutil

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type testStruct1 struct {
	Object testStruct2 `json:"object"`
}

type testStruct2 struct {
	Name   string      `json:"name"`
	Action testStruct3 `json:"action" online:"true"`
}

type testStruct3 struct {
	Enable bool `json:"enable,string" online:"true"`
	Online bool `json:"online" online:"false"`
}

func TestFindJSONFullTagByChildTag(t *testing.T) {
	re := require.New(t)
	key := "enable"
	result := FindJSONFullTagByChildTag(reflect.TypeOf(testStruct1{}), key)
	re.Equal("object.action.enable", result)

	key = "action"
	result = FindJSONFullTagByChildTag(reflect.TypeOf(testStruct1{}), key)
	re.Equal("object.action", result)

	key = "disable"
	result = FindJSONFullTagByChildTag(reflect.TypeOf(testStruct1{}), key)
	re.Empty(result)
}

func TestGetAllOnlineConfigTags(t *testing.T) {
	re := require.New(t)
	dict := GetAllOnlineConfigTags(reflect.TypeOf(testStruct1{}))
	re.Contains(dict, "object.name")
	re.Contains(dict, "name")
	re.Contains(dict, "object.action.enable")
	re.Contains(dict, "object.enable")
	re.Contains(dict, "enable")
	re.NotContains(dict, "object.action.online")
	re.NotContains(dict, "object.online")
	re.NotContains(dict, "online")
}

func TestFindSameFieldByJSON(t *testing.T) {
	re := require.New(t)
	input := map[string]any{
		"name": "test2",
	}
	t2 := testStruct2{}
	re.True(FindSameFieldByJSON(&t2, input))
	input = map[string]any{
		"enable": "test2",
	}
	re.False(FindSameFieldByJSON(&t2, input))
}

func TestFindFieldByJSONTag(t *testing.T) {
	re := require.New(t)
	t1 := testStruct1{}
	t2 := testStruct2{}
	t3 := testStruct3{}
	type2 := reflect.TypeOf(t2)
	type3 := reflect.TypeOf(t3)

	tags := []string{"object"}
	result := FindFieldByJSONTag(reflect.TypeOf(t1), tags)
	re.Equal(type2, result)

	tags = []string{"object", "action"}
	result = FindFieldByJSONTag(reflect.TypeOf(t1), tags)
	re.Equal(type3, result)

	tags = []string{"object", "name"}
	result = FindFieldByJSONTag(reflect.TypeOf(t1), tags)
	re.Equal(reflect.String, result.Kind())

	tags = []string{"object", "action", "enable"}
	result = FindFieldByJSONTag(reflect.TypeOf(t1), tags)
	re.Equal(reflect.Bool, result.Kind())
}
