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

package progress

import (
	"math"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testProgressSuite{})

type testProgressSuite struct{}

func (s *testProgressSuite) Test(c *C) {
	n := "test"
	m := NewManager()
	c.Assert(m.AddProgress(n, 100, 100, 10*time.Second), IsFalse)
	p, ls, cs, err := m.Status(n)
	c.Assert(err, IsNil)
	c.Assert(p, Equals, 0.0)
	c.Assert(ls, Equals, math.MaxFloat64)
	c.Assert(cs, Equals, 0.0)
	time.Sleep(time.Second)
	c.Assert(m.AddProgress(n, 100, 100, 10*time.Second), IsTrue)

	m.UpdateProgress(n, 30, 30, false)
	p, ls, cs, err = m.Status(n)
	c.Assert(err, IsNil)
	c.Assert(p, Equals, 0.7)
	// 30/(70/1s+) > 30/70
	c.Assert(ls, Greater, 30.0/70.0)
	// 70/1s+ > 70
	c.Assert(cs, Less, 70.0)
	// there is no scheduling
	for i := 0; i < 100; i++ {
		m.UpdateProgress(n, 30, 30, false)
	}
	c.Assert(m.progesses[n].history.Len(), Equals, 61)
	p, ls, cs, err = m.Status(n)
	c.Assert(err, IsNil)
	c.Assert(p, Equals, 0.7)
	c.Assert(ls, Equals, math.MaxFloat64)
	c.Assert(cs, Equals, 0.0)

	ps := m.GetProgresses(func(p string) bool {
		return strings.Contains(p, n)
	})
	c.Assert(ps, HasLen, 1)
	c.Assert(ps[0], Equals, n)
	ps = m.GetProgresses(func(p string) bool {
		return strings.Contains(p, "a")
	})
	c.Assert(ps, HasLen, 0)
	c.Assert(m.RemoveProgress(n), IsTrue)
	c.Assert(m.RemoveProgress(n), IsFalse)
}

func (s *testProgressSuite) TestAbnormal(c *C) {
	n := "test"
	m := NewManager()
	c.Assert(m.AddProgress(n, 100, 100, 10*time.Second), IsFalse)
	p, ls, cs, err := m.Status(n)
	c.Assert(err, IsNil)
	c.Assert(p, Equals, 0.0)
	c.Assert(ls, Equals, math.MaxFloat64)
	c.Assert(cs, Equals, 0.0)
	// When offline a store, but there are still many write operations
	m.UpdateProgress(n, 110, 110, false)
	p, ls, cs, err = m.Status(n)
	c.Assert(err, IsNil)
	c.Assert(p, Equals, 0.0)
	c.Assert(ls, Equals, math.MaxFloat64)
	c.Assert(cs, Equals, 0.0)
	// It usually won't happens
	m.UpdateProgressTotal(n, 10)
	p, ls, cs, err = m.Status(n)
	c.Assert(err, NotNil)
	c.Assert(p, Equals, 0.0)
	c.Assert(ls, Equals, 0.0)
	c.Assert(cs, Equals, 0.0)
}
