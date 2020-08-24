// Copyright 2019 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package analysis

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
)

func TestParser(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testParseLog{})

type testParseLog struct{}

func transferCounterParseLog(operator, content string, expect []uint64) bool {
	r, _ := GetTransferCounter().CompileRegex(operator)
	results, _ := GetTransferCounter().parseLine(content, r)
	if len(results) != len(expect) {
		return false
	}
	for i := 0; i < len(results); i++ {
		if results[i] != expect[i] {
			return false
		}
	}
	return true
}

func (t *testParseLog) TestTransferCounterParseLog(c *C) {
	{
		operator := "balance-leader"
		content := "[2019/09/05 04:15:52.404 +00:00] [INFO] [operator_controller.go:119] [\"operator finish\"] [region-id=54252] [operator=\"\"balance-leader {transfer leader: store 4 to 6} (kind:leader,balance, region:54252(8243,398), createAt:2019-09-05 04:15:52.400290023 +0000 UTC m=+91268.739649520, startAt:2019-09-05 04:15:52.400489629 +0000 UTC m=+91268.739849120, currentStep:1, steps:[transfer leader from store 4 to store 6]) finished\"\"]"
		var expect = []uint64{54252, 4, 6}
		c.Assert(transferCounterParseLog(operator, content, expect), Equals, true)
	}
	{
		operator := "balance-region"
		content := "[2019/09/03 17:42:07.898 +08:00] [INFO] [operator_controller.go:119] [\"operator finish\"] [region-id=24622] [operator=\"\"balance-region {mv peer: store [6] to [1]} (kind:region,balance, region:24622(1,1), createAt:2019-09-03 17:42:06.602589701 +0800 CST m=+737.457773921, startAt:2019-09-03 17:42:06.602849306 +0800 CST m=+737.458033475, currentStep:3, steps:[add learner peer 64064 on store 1, promote learner peer 64064 on store 1 to voter, remove peer on store 6]) finished\"\"]\""
		var expect = []uint64{24622, 6, 1}
		c.Assert(transferCounterParseLog(operator, content, expect), Equals, true)
	}
	{
		operator := "transfer-hot-write-leader"
		content := "[2019/09/05 14:05:42.811 +08:00] [INFO] [operator_controller.go:119] [\"operator finish\"] [region-id=94] [operator=\"\"transfer-hot-write-leader {transfer leader: store 2 to 1} (kind:leader,hot-region, region:94(1,1), createAt:2019-09-05 14:05:42.676394689 +0800 CST m=+14.955640307, startAt:2019-09-05 14:05:42.676589507 +0800 CST m=+14.955835051, currentStep:1, steps:[transfer leader from store 2 to store 1]) finished\"\"]"
		var expect = []uint64{94, 2, 1}
		c.Assert(transferCounterParseLog(operator, content, expect), Equals, true)
	}
	{
		operator := "move-hot-write-region"
		content := "[2019/09/05 14:05:54.311 +08:00] [INFO] [operator_controller.go:119] [\"operator finish\"] [region-id=98] [operator=\"\"move-hot-write-region {mv peer: store [2] to [10]} (kind:region,hot-region, region:98(1,1), createAt:2019-09-05 14:05:49.718201432 +0800 CST m=+21.997446945, startAt:2019-09-05 14:05:49.718336308 +0800 CST m=+21.997581822, currentStep:3, steps:[add learner peer 2048 on store 10, promote learner peer 2048 on store 10 to voter, remove peer on store 2]) finished\"\"]"
		var expect = []uint64{98, 2, 10}
		c.Assert(transferCounterParseLog(operator, content, expect), Equals, true)
	}
	{
		operator := "transfer-hot-read-leader"
		content := "[2019/09/05 14:16:38.758 +08:00] [INFO] [operator_controller.go:119] [\"operator finish\"] [region-id=85] [operator=\"\"transfer-hot-read-leader {transfer leader: store 1 to 5} (kind:leader,hot-region, region:85(1,1), createAt:2019-09-05 14:16:38.567463945 +0800 CST m=+29.117453011, startAt:2019-09-05 14:16:38.567603515 +0800 CST m=+29.117592496, currentStep:1, steps:[transfer leader from store 1 to store 5]) finished\"\"]"
		var expect = []uint64{85, 1, 5}
		c.Assert(transferCounterParseLog(operator, content, expect), Equals, true)
	}
	{
		operator := "move-hot-read-region"
		content := "[2019/09/05 14:19:15.066 +08:00] [INFO] [operator_controller.go:119] [\"operator finish\"] [region-id=389] [operator=\"\"move-hot-read-region {mv peer: store [5] to [4]} (kind:leader,region,hot-region, region:389(1,1), createAt:2019-09-05 14:19:13.576359364 +0800 CST m=+25.855737101, startAt:2019-09-05 14:19:13.576556556 +0800 CST m=+25.855934288, currentStep:4, steps:[add learner peer 2014 on store 4, promote learner peer 2014 on store 4 to voter, transfer leader from store 5 to store 3, remove peer on store 5]) finished\"\"]"
		var expect = []uint64{389, 5, 4}
		c.Assert(transferCounterParseLog(operator, content, expect), Equals, true)
	}
}

func (t *testParseLog) TestIsExpectTime(c *C) {
	{
		testFunction := isExpectTime("2019/09/05 14:19:15", DefaultLayout, true)
		current, _ := time.Parse(DefaultLayout, "2019/09/05 14:19:14")
		c.Assert(testFunction(current), Equals, true)
	}
	{
		testFunction := isExpectTime("2019/09/05 14:19:15", DefaultLayout, false)
		current, _ := time.Parse(DefaultLayout, "2019/09/05 14:19:16")
		c.Assert(testFunction(current), Equals, true)
	}
	{
		testFunction := isExpectTime("", DefaultLayout, true)
		current, _ := time.Parse(DefaultLayout, "2019/09/05 14:19:14")
		c.Assert(testFunction(current), Equals, true)
	}
	{
		testFunction := isExpectTime("", DefaultLayout, false)
		current, _ := time.Parse(DefaultLayout, "2019/09/05 14:19:16")
		c.Assert(testFunction(current), Equals, true)
	}
}

func (t *testParseLog) TestCurrentTime(c *C) {
	getCurrentTime := currentTime(DefaultLayout)
	content := "[2019/09/05 14:19:15.066 +08:00] [INFO] [operator_controller.go:119] [\"operator finish\"] [region-id=389] [operator=\"\"move-hot-read-region {mv peer: store 5 to 4} (kind:leader,region,hot-region, region:389(1,1), createAt:2019-09-05 14:19:13.576359364 +0800 CST m=+25.855737101, startAt:2019-09-05 14:19:13.576556556 +0800 CST m=+25.855934288, currentStep:4, steps:[add learner peer 2014 on store 4, promote learner peer 2014 on store 4 to voter, transfer leader from store 5 to store 3, remove peer on store 5]) finished\"\"]"
	current, err := getCurrentTime(content)
	c.Assert(err, Equals, nil)
	expect, _ := time.Parse(DefaultLayout, "2019/09/05 14:19:15")
	c.Assert(current, Equals, expect)
}
