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

package schedulers

import (
	"bytes"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/server/core"
)

func (s *testEvictLeaderSuite) TestConfigClone(c *C) {
	emptyConf := &evictLeaderSchedulerConfig{StoreIDWithRanges: make(map[uint64][]core.KeyRange)}
	con2 := emptyConf.Clone()
	c.Assert(emptyConf.getKeyRangesByID(1), IsNil)
	c.Assert(con2.BuildWithArgs([]string{"1"}), IsNil)
	c.Assert(con2.getKeyRangesByID(1), NotNil)
	c.Assert(emptyConf.getKeyRangesByID(1), IsNil)

	con3 := con2.Clone()
	con3.StoreIDWithRanges[1], _ = getKeyRanges([]string{"a", "b", "c", "d"})
	c.Assert(emptyConf.getKeyRangesByID(1), IsNil)
	c.Assert(len(con3.getRanges(1)) == len(con2.getRanges(1)), IsFalse)

	con4 := con3.Clone()
	c.Assert(bytes.Equal(con4.StoreIDWithRanges[1][0].StartKey, con3.StoreIDWithRanges[1][0].StartKey), IsTrue)
	con4.StoreIDWithRanges[1][0].StartKey = []byte("aaa")
	c.Assert(bytes.Equal(con4.StoreIDWithRanges[1][0].StartKey, con3.StoreIDWithRanges[1][0].StartKey), IsFalse)
}
