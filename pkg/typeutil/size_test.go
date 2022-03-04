// Copyright 2017 TiKV Project Authors.
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
	. "github.com/pingcap/check"
)

var _ = Suite(&testSizeSuite{})

type testSizeSuite struct {
}

func (s *testSizeSuite) TestJSON(c *C) {
	b := ByteSize(265421587)
	o, err := json.Marshal(b)
	c.Assert(err, IsNil)

	var nb ByteSize
	err = json.Unmarshal(o, &nb)
	c.Assert(err, IsNil)

	b = ByteSize(1756821276000)
	o, err = json.Marshal(b)
	c.Assert(err, IsNil)
	c.Assert(string(o), Equals, `"1.598TiB"`)
}

func (s *testSizeSuite) TestConvert(c *C) {
	body := []byte(`"144MiB"`)
	var bs ByteSize
	c.Assert(json.Unmarshal(body, &bs), IsNil)
	c.Assert(bs.ToMiB(), Equals, uint64(144))
	c.Assert(bs.ToGiB(), Equals, uint64(0))
	body = []byte(`"144GiB"`)
	c.Assert(json.Unmarshal(body, &bs), IsNil)
	c.Assert(bs.ToGiB(), Equals, uint64(144))
	c.Assert(bs.ToMiB(), Equals, uint64(144*1024))
}
