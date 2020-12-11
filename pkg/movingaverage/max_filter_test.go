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
// See the License for the specific language governing permissions and
// limitations under the License.

package movingaverage

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testMaxFilter{})

type testMaxFilter struct{}

func (t *testMaxFilter) TestMaxFilter(c *C) {
	var empty float64 = 0
	data := []float64{2, 1, 3, 4, 1, 1, 3, 3, 2, 0, 5}
	expected := []float64{2, 2, 3, 4, 4, 4, 4, 4, 3, 3, 5}

	mf := NewMaxFilter(5)
	c.Assert(mf.Get(), Equals, empty)

	checkReset(c, mf, empty)
	checkAdd(c, mf, data, expected)
	checkSet(c, mf, data, expected)
}
