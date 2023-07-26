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

package component

import (
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/kv"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testManagerSuite{})

type testManagerSuite struct{}

func (s *testManagerSuite) TestManager(c *C) {
	m := NewManager(core.NewStorage(kv.NewMemoryKV()))
	// register legal address
	c.Assert(m.Register("c1", "127.0.0.1:1"), IsNil)
	c.Assert(m.Register("c1", "127.0.0.1:2"), IsNil)
	// register repeatedly
	c.Assert(strings.Contains(m.Register("c1", "127.0.0.1:2").Error(), "already"), IsTrue)
	c.Assert(m.Register("c2", "127.0.0.1:3"), IsNil)

	// register illegal address
	c.Assert(m.Register("c1", " 127.0.0.1:4"), NotNil)

	// get all addresses
	all := map[string][]string{
		"c1": {"127.0.0.1:1", "127.0.0.1:2"},
		"c2": {"127.0.0.1:3"},
	}
	c.Assert(m.GetAllComponentAddrs(), DeepEquals, all)

	// get the specific component addresses
	c.Assert(m.GetComponentAddrs("c1"), DeepEquals, all["c1"])
	c.Assert(m.GetComponentAddrs("c2"), DeepEquals, all["c2"])

	// get the component from the address
	c.Assert(m.GetComponent("127.0.0.1:1"), Equals, "c1")
	c.Assert(m.GetComponent("127.0.0.1:2"), Equals, "c1")
	c.Assert(m.GetComponent("127.0.0.1:3"), Equals, "c2")

	// unregister address
	c.Assert(m.UnRegister("c1", "127.0.0.1:1"), IsNil)
	c.Assert(m.GetComponentAddrs("c1"), DeepEquals, []string{"127.0.0.1:2"})
	c.Assert(m.UnRegister("c1", "127.0.0.1:2"), IsNil)
	c.Assert(m.GetComponentAddrs("c1"), DeepEquals, []string{})
	all = map[string][]string{"c2": {"127.0.0.1:3"}}
	c.Assert(m.GetAllComponentAddrs(), DeepEquals, all)
}
