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

package auth

import (
	"encoding/json"

	"github.com/tikv/pd/pkg/errs"

	. "github.com/pingcap/check"
)

var _ = Suite(&testPermissionSuite{})

type testPermissionSuite struct{}

func (s *testPermissionSuite) TestPermission(c *C) {
	_, err := NewPermission("storage", "invalid_action")
	c.Assert(err, NotNil)
	c.Assert(errs.ErrInvalidPermissionAction.Equal(err), IsTrue)

	permission, err := NewPermission("storage", GET)
	c.Assert(err, IsNil)
	c.Assert(permission.String(), Equals, "get(storage)")

	marshalledPermission := "{\"resource\":\"storage\",\"action\":\"get\"}"
	j, err := json.Marshal(permission)
	c.Assert(err, IsNil)
	c.Assert(string(j), Equals, marshalledPermission)

	unmarshalledPermission := new(Permission)
	err = json.Unmarshal([]byte(marshalledPermission), unmarshalledPermission)
	c.Assert(err, IsNil)
	c.Assert(unmarshalledPermission, DeepEquals, permission)
}
