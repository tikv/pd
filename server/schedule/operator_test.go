// Copyright 2016 PingCAP, Inc.
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

package schedule

import (
	"encoding/json"

	. "github.com/pingcap/check"
)

var _ = Suite(&testOperatorSuite{})

type testOperatorSuite struct{}

func (o *testOperatorSuite) TestOperatorStateString(c *C) {
	tbl := []struct {
		value OperatorState
		name  string
	}{
		{OperatorUnKnownState, "unknown"},
		{OperatorWaiting, "waiting"},
		{OperatorRunning, "running"},
		{OperatorFinished, "finished"},
		{OperatorTimeOut, "timeout"},
		{OperatorReplaced, "replaced"},
		{OperatorState(404), "unknown"},
	}
	for _, t := range tbl {
		c.Assert(t.value.String(), Equals, t.name)
	}
}

func (o *testOperatorSuite) TestOperatorStateMarshal(c *C) {
	tbl := []struct {
		state  OperatorState
		except OperatorState
	}{
		{OperatorUnKnownState, OperatorUnKnownState},
		{OperatorWaiting, OperatorWaiting},
		{OperatorRunning, OperatorRunning},
		{OperatorFinished, OperatorFinished},
		{OperatorTimeOut, OperatorTimeOut},
		{OperatorReplaced, OperatorReplaced},
		{OperatorState(404), OperatorUnKnownState},
	}
	for _, t := range tbl {
		data, err := json.Marshal(t.state)
		c.Assert(err, IsNil)
		var newState OperatorState
		err = json.Unmarshal(data, &newState)
		c.Assert(err, IsNil)
		c.Assert(newState, Equals, t.except)
	}
}
