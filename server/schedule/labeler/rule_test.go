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
	c.Assert(label.start.IsZero(), IsTrue)
	c.Assert(label.StartAt, Equals, "")
	c.Assert(label.expire, Equals, unlimittedExpire)

	// test rule with illegal ttl.
	label.TTL = "ttl"
	err = label.checkAndAdjustExpire()
	c.Assert(err, NotNil)

	// test legal rule with ttl
	label.TTL = "10h10m10s10ms"
	err = label.checkAndAdjustExpire()
	c.Assert(err, IsNil)
	c.Assert(label.start.IsZero(), IsFalse)
	c.Assert(label.start.Format(time.UnixDate), Equals, label.StartAt)
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
	// the private field should be empty.
	c.Assert(label2.start.IsZero(), IsTrue)
	expire := label2.getExpire()
	c.Assert(label.expire.Format(time.UnixDate), Equals, expire.Format(time.UnixDate))
}
