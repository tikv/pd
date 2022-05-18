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

	. "github.com/pingcap/check"
)

type testStruct1 struct {
	Object testStruct2 `json:"object"`
}

type testStruct2 struct {
	Name   string      `json:"name"`
	Action testStruct3 `json:"action"`
}

type testStruct3 struct {
	Enable bool `json:"enable,string"`
}

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testTagSuite{})

type testTagSuite struct{}

func (s *testTagSuite) TestFindJSONFullTagByChildTag(c *C) {
	key := "enable"
	result := FindJSONFullTagByChildTag(reflect.TypeOf(testStruct1{}), key)
	c.Assert(result, Equals, "object.action.enable")

	key = "action"
	result = FindJSONFullTagByChildTag(reflect.TypeOf(testStruct1{}), key)
	c.Assert(result, Equals, "object.action")

	key = "disable"
	result = FindJSONFullTagByChildTag(reflect.TypeOf(testStruct1{}), key)
	c.Assert(result, HasLen, 0)
}

func (s *testTagSuite) TestFindSameFieldByJSON(c *C) {
	input := map[string]interface{}{
		"name": "test2",
	}
	t2 := testStruct2{}
	c.Assert(FindSameFieldByJSON(&t2, input), Equals, true)
	input = map[string]interface{}{
		"enable": "test2",
	}
	c.Assert(FindSameFieldByJSON(&t2, input), Equals, false)
}

func (s *testTagSuite) TestFindFieldByJSONTag(c *C) {
	t1 := testStruct1{}
	t2 := testStruct2{}
	t3 := testStruct3{}
	type2 := reflect.TypeOf(t2)
	type3 := reflect.TypeOf(t3)

	tags := []string{"object"}
	result := FindFieldByJSONTag(reflect.TypeOf(t1), tags)
	c.Assert(result, Equals, type2)

	tags = []string{"object", "action"}
	result = FindFieldByJSONTag(reflect.TypeOf(t1), tags)
	c.Assert(result, Equals, type3)

	tags = []string{"object", "name"}
	result = FindFieldByJSONTag(reflect.TypeOf(t1), tags)
	c.Assert(result.Kind(), Equals, reflect.String)

	tags = []string{"object", "action", "enable"}
	result = FindFieldByJSONTag(reflect.TypeOf(t1), tags)
	c.Assert(result.Kind(), Equals, reflect.Bool)
}
