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

package auth

import (
	"encoding/json"

	. "github.com/pingcap/check"
)

var _ = Suite(&testUserSuite{})

type testUserSuite struct{}

func (s *testUserSuite) TestUser(c *C) {
	_, err := NewUser("00test", "")
	c.Assert(err, NotNil)
	user, err := NewUser("test", "a0705ec8c6c0dab42344570b50608935174f84b5c365e9ff2b3ed92e0fc8e037")
	c.Assert(err, IsNil)
	user.RoleKeys = []string{"reader", "writer"}

	c.Assert(user.GetUsername(), Equals, "test")
	c.Assert(user.GetRoleKeys(), DeepEquals, []string{"reader", "writer"})
	c.Assert(user.hasRole("reader"), IsTrue)
	c.Assert(user.hasRole("admin"), IsFalse)
	c.Assert(user.appendRole("reader"), IsFalse)
	c.Assert(user.appendRole("admin"), IsTrue)
	c.Assert(user.hasRole("admin"), IsTrue)

	c.Assert(user.Clone(), DeepEquals, user)

	c.Assert(user.ComparePassword("somethingwrong"), NotNil)
	c.Assert(user.ComparePassword("ItsaCrazilysEcurepass"), IsNil)

	marshalledUser := "{\"username\":\"test\"," +
		"\"hash\":\"a0705ec8c6c0dab42344570b50608935174f84b5c365e9ff2b3ed92e0fc8e037\"," +
		"\"roles\":[\"reader\",\"writer\",\"admin\"]}"
	j, err := json.Marshal(user)
	c.Assert(err, IsNil)
	c.Assert(string(j), Equals, marshalledUser)

	unmarshalledUser, err := UnmarshalUser(string(j))
	c.Assert(err, IsNil)
	c.Assert(unmarshalledUser, DeepEquals, user)

	c.Assert(user.removeRole("somebody"), IsFalse)
	c.Assert(user.removeRole("reader"), IsTrue)
	c.Assert(user.removeRole("writer"), IsTrue)
	c.Assert(user.removeRole("admin"), IsTrue)
	c.Assert(user.GetRoleKeys(), DeepEquals, []string{})
}
