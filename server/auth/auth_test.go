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
// See the License for the specific language governing permissionKeys and
// limitations under the License.
//

package auth

import (
	"encoding/json"
	"testing"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testAuthSuite{})

type testAuthSuite struct{}

func (s *testAuthSuite) TestUser(c *C) {
	_, err := NewUser("00test", "")
	c.Assert(err, NotNil)
	user, err := NewUser("test", "a0705ec8c6c0dab42344570b50608935174f84b5c365e9ff2b3ed92e0fc8e037")
	c.Assert(err, IsNil)
	user.roleKeys = map[string]struct{}{"reader": {}, "writer": {}}

	c.Assert(user.GetUsername(), Equals, "test")
	c.Assert(user.GetRoleKeys(), DeepEquals, map[string]struct{}{"reader": {}, "writer": {}})
	c.Assert(user.HasRole("reader"), IsTrue)
	c.Assert(user.HasRole("admin"), IsFalse)

	c.Assert(user.Clone(), DeepEquals, *user)

	c.Assert(user.ComparePassword("somethingwrong"), NotNil)
	c.Assert(user.ComparePassword("ItsaCrazilysEcurepass"), IsNil)

	marshalledUser := "{\"username\":\"test\"," +
		"\"hash\":\"a0705ec8c6c0dab42344570b50608935174f84b5c365e9ff2b3ed92e0fc8e037\"," +
		"\"roles\":[\"reader\",\"writer\"]}"
	j, err := json.Marshal(user)
	c.Assert(err, IsNil)
	c.Assert(string(j), Equals, marshalledUser)

	unmarshalledUser, err := NewUserFromJson(string(j))
	c.Assert(err, IsNil)
	c.Assert(unmarshalledUser, DeepEquals, user)

	safeUser := user.GetSafeUser()
	marshalledSafeUser, err := json.Marshal(safeUser)
	c.Assert(err, IsNil)
	c.Assert(string(marshalledSafeUser), Equals, "{\"username\":\"test\",\"roles\":[\"reader\",\"writer\"]}")
}

func (s *testAuthSuite) TestRole(c *C) {
	_, err := NewRole("00test")
	c.Assert(err, NotNil)
	role, err := NewRole("test")
	c.Assert(err, IsNil)
	p1, err := NewPermission("storage", "get")
	p2, err := NewPermission("region", "list")
	p3, err := NewPermission("region", "get")
	p4, err := NewPermission("region", "delete")
	c.Assert(err, IsNil)
	role.permissions = map[Permission]struct{}{*p1: {}, *p2: {}, *p3: {}}

	c.Assert(role.GetName(), Equals, "test")
	c.Assert(role.GetPermissions(), DeepEquals, map[Permission]struct{}{*p1: {}, *p2: {}, *p3: {}})
	c.Assert(role.HasPermission(*p1), IsTrue)
	c.Assert(role.HasPermission(*p4), IsFalse)

	c.Assert(role.Clone(), DeepEquals, *role)

	marshalledRole := "{\"name\":\"test\",\"permissions\":" +
		"[{\"resource\":\"region\",\"action\":\"get\"}," +
		"{\"resource\":\"region\",\"action\":\"list\"}," +
		"{\"resource\":\"storage\",\"action\":\"get\"}]}"
	j, err := json.Marshal(role)
	c.Assert(err, IsNil)
	c.Assert(string(j), Equals, marshalledRole)

	unmarshalledRole, err := NewRoleFromJson(string(j))
	c.Assert(err, IsNil)
	c.Assert(unmarshalledRole, DeepEquals, role)
}
