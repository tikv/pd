// Copyright 2019 PingCAP, Inc.
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

package operator

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testOpStatusSuite{})

type testOpStatusSuite struct{}

func (s *testOpStatusSuite) TestIsEndStatus(c *C) {
	for st := OpStatus(0); st < firstEndStatus; st++ {
		c.Assert(IsEndStatus(st), IsFalse)
	}
	for st := firstEndStatus; st < statusCount; st++ {
		c.Assert(IsEndStatus(st), IsTrue)
	}
	for st := statusCount; st < statusCount+100; st++ {
		c.Assert(IsEndStatus(st), IsFalse)
	}
}
