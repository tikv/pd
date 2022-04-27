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

package labeler

import (
	"encoding/json"
	"math"
	"time"

	. "github.com/pingcap/check"
)

var _ = Suite(&testRuleSuite{})

type testRuleSuite struct{}

func (s *testLabelerSuite) TestRegionLabelTTL(c *C) {
	label := RegionLabel{Key: "k1", Value: "v1"}

	// test label with no ttl.
	err := label.checkAndAdjustExpire()
	c.Assert(err, IsNil)
	c.Assert(label.StartAt, HasLen, 0)
	c.Assert(label.expire, IsNil)

	// test rule with illegal ttl.
	label.TTL = "ttl"
	err = label.checkAndAdjustExpire()
	c.Assert(err, NotNil)

	// test legal rule with ttl
	label.TTL = "10h10m10s10ms"
	err = label.checkAndAdjustExpire()
	c.Assert(err, IsNil)
	c.Assert(len(label.StartAt) > 0, IsTrue)
	c.Assert(label.expireBefore(time.Now().Add(time.Hour)), IsFalse)
	c.Assert(label.expireBefore(time.Now().Add(24*time.Hour)), IsTrue)

	// test legal rule with ttl, rule unmarshal from json.
	data, err := json.Marshal(label)
	c.Assert(err, IsNil)
	var label2 RegionLabel
	err = json.Unmarshal(data, &label2)
	c.Assert(err, IsNil)
	c.Assert(label2.StartAt, Equals, label.StartAt)
	c.Assert(label2.TTL, Equals, label.TTL)
	label2.checkAndAdjustExpire()
	// The `expire` should be the same with minor inaccuracies.
	c.Assert(math.Abs(label2.expire.Sub(*label.expire).Seconds()) < 1, IsTrue)
}
