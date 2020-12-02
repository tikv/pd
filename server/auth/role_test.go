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
	. "github.com/pingcap/check"
)

var _ = Suite(&testRoleSuite{})

type testRoleSuite struct{}

func (s *testRoleSuite) TestRole(c *C) {
	_, err := NewRole("00test")
	c.Assert(err, NotNil)
	role, err := NewRole("test")
	c.Assert(err, IsNil)
	p1, err := NewPermission("storage", "get")
	c.Assert(err, IsNil)
	p2, err := NewPermission("region", "list")
	c.Assert(err, IsNil)
	p3, err := NewPermission("region", "get")
	c.Assert(err, IsNil)
	p4, err := NewPermission("region", "delete")
	c.Assert(err, IsNil)
	role.Permissions = map[Permission]struct{}{*p1: {}, *p2: {}, *p3: {}}

	c.Assert(role.GetName(), Equals, "test")
	c.Assert(role.GetPermissions(), DeepEquals, map[Permission]struct{}{*p1: {}, *p2: {}, *p3: {}})
	c.Assert(role.HasPermission(*p1), IsTrue)
	c.Assert(role.HasPermission(*p4), IsFalse)

	c.Assert(role.Clone(), DeepEquals, role)

	marshalledRole := "{\"name\":\"test\",\"permissions\":" +
		"[{\"resource\":\"region\",\"action\":\"get\"}," +
		"{\"resource\":\"region\",\"action\":\"list\"}," +
		"{\"resource\":\"storage\",\"action\":\"get\"}]}"
	j, err := json.Marshal(role)
	c.Assert(err, IsNil)
	c.Assert(string(j), Equals, marshalledRole)

	unmarshalledRole, err := NewRoleFromJSON(string(j))
	c.Assert(err, IsNil)
	c.Assert(unmarshalledRole, DeepEquals, role)
}
