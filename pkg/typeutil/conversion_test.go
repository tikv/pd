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

	. "github.com/pingcap/check"
)

var _ = Suite(&testUint64BytesSuite{})

type testUint64BytesSuite struct{}

func (s *testUint64BytesSuite) TestBytesToUint64(c *C) {
	str := "\x00\x00\x00\x00\x00\x00\x03\xe8"
	a, err := BytesToUint64([]byte(str))
	c.Assert(err, IsNil)
	c.Assert(a, Equals, uint64(1000))
}

func (s *testUint64BytesSuite) TestUint64ToBytes(c *C) {
	var a uint64 = 1000
	b := Uint64ToBytes(a)
	str := "\x00\x00\x00\x00\x00\x00\x03\xe8"
	c.Assert(b, DeepEquals, []byte(str))
}

var _ = Suite(&testJSONSuite{})

type testJSONSuite struct{}

func (s *testJSONSuite) TestJSONToUint64Slice(c *C) {
	type testArray struct {
		Array []uint64 `json:"array"`
	}
	a := testArray{
		Array: []uint64{1, 2, 3},
	}
	bytes, _ := json.Marshal(a)
	var t map[string]interface{}
	err := json.Unmarshal(bytes, &t)
	c.Assert(err, IsNil)
	res, ok := JSONToUint64Slice(t["array"])
	c.Assert(ok, IsTrue)
	c.Assert(reflect.TypeOf(res[0]).Kind(), Equals, reflect.Uint64)
}
