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

func (s *testLabelerSuite) TestRuleTTL(c *C) {
	rule := LabelRule{
		ID: "foo",
		Labels: []RegionLabel{
			{Key: "k1", Value: "v1"},
		},
		RuleType: "key-range",
		Data:     makeKeyRanges("12abcd", "34cdef", "56abcd", "78cdef"),
	}
	err := rule.checkAndAdjust()
	c.Assert(err, IsNil)
	// test rule with no ttl.
	err = rule.checkAndAdjustExpire()
	c.Assert(err, IsNil)
	c.Assert(rule.start.IsZero(), IsTrue)
	c.Assert(rule.StartAt, Equals, "")
	c.Assert(rule.expire, Equals, unlimittedExpire)

	// test rule with illegal ttl.
	rule.TTL = "ttl"
	err = rule.checkAndAdjustExpire()
	c.Assert(err, NotNil)

	// test legal rule with ttl
	rule.TTL = "10h10m10s"
	err = rule.checkAndAdjustExpire()
	c.Assert(err, IsNil)
	c.Assert(rule.start.IsZero(), IsFalse)
	c.Assert(rule.start.Format(time.UnixDate), Equals, rule.StartAt)
	c.Assert(rule.expireBefore(time.Now().Add(time.Hour)), IsFalse)
	c.Assert(rule.expireBefore(time.Now().Add(24*time.Hour)), IsTrue)

	// test legal rule with ttl, rule unmarshal from json.
	data, err := json.Marshal(rule)
	c.Assert(err, IsNil)
	var rule2 LabelRule
	err = json.Unmarshal(data, &rule2)
	c.Assert(err, IsNil)
	c.Assert(rule2.StartAt, Equals, rule.StartAt)
	c.Assert(rule2.TTL, Equals, rule.TTL)
	// the private field should be empty.
	c.Assert(rule2.start.IsZero(), IsTrue)
	expire := rule2.getExpire()
	c.Assert(rule.expire.Format(time.UnixDate), Equals, expire.Format(time.UnixDate))
}
