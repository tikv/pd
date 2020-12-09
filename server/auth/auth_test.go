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
	"path"
	"reflect"
	"testing"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/kv"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testAuthSuite{})

type testAuthSuite struct{}
type testFunc func(*C, *roleManager)

func (s *testAuthSuite) TestRoleManager(c *C) {
	testFuncs := []testFunc{
		s.testGetRole,
		s.testGetRoles,
		s.testCreateRole,
		s.testDeleteRole,
		s.testSetPermissions,
		s.testAddPermission,
		s.testRemovePermission,
	}
	for _, f := range testFuncs {
		k := kv.NewMemoryKV()
		initKV(c, k)
		manager := newRoleManager(k)
		err := manager.UpdateCache()
		c.Assert(err, IsNil)
		f(c, manager)
	}
}

func (s *testAuthSuite) testGetRole(c *C, m *roleManager) {
	expectedRole := Role{
		Name: "reader",
		Permissions: []Permission{
			{Resource: "region", Action: "get"},
			{Resource: "region", Action: "list"},
			{Resource: "store", Action: "get"},
			{Resource: "store", Action: "list"},
		},
	}
	role, err := m.GetRole("reader")
	c.Assert(err, IsNil)
	c.Assert(role, DeepEquals, &expectedRole)
	_, err = m.GetRole("somebody")
	c.Assert(err, NotNil)
	c.Assert(errs.ErrRoleNotFound.Equal(err), IsTrue)
}

func (s *testAuthSuite) testGetRoles(c *C, m *roleManager) {
	expectedRoles := []Role{
		{Name: "reader", Permissions: []Permission{
			{Resource: "region", Action: "get"},
			{Resource: "region", Action: "list"},
			{Resource: "store", Action: "get"},
			{Resource: "store", Action: "list"},
		}},
		{Name: "writer", Permissions: []Permission{
			{Resource: "region", Action: "delete"},
			{Resource: "region", Action: "update"},
			{Resource: "store", Action: "delete"},
			{Resource: "store", Action: "update"},
		}},
		{Name: "admin", Permissions: []Permission{
			{Resource: "region", Action: "delete"},
			{Resource: "region", Action: "get"},
			{Resource: "region", Action: "list"},
			{Resource: "region", Action: "update"},
			{Resource: "store", Action: "delete"},
			{Resource: "store", Action: "get"},
			{Resource: "store", Action: "list"},
			{Resource: "store", Action: "update"},
			{Resource: "users", Action: "delete"},
			{Resource: "users", Action: "get"},
			{Resource: "users", Action: "list"},
			{Resource: "users", Action: "update"},
		}},
	}
	roles := m.GetRoles()
	c.Assert(len(roles), Equals, 3)
	for _, role := range roles {
		hasRole := false
		for _, expectedRole := range expectedRoles {
			if role.Name == expectedRole.Name &&
				reflect.DeepEqual(role.Permissions, expectedRole.Permissions) {
				hasRole = true
				break
			}
		}
		c.Assert(hasRole, IsTrue)
	}
}

func (s *testAuthSuite) testCreateRole(c *C, m *roleManager) {
	expectedRole := Role{Name: "nobody", Permissions: []Permission{}}
	err := m.CreateRole("reader")
	c.Assert(err, NotNil)
	c.Assert(errs.ErrRoleExists.Equal(err), IsTrue)
	err = m.CreateRole("!")
	c.Assert(err, NotNil)
	c.Assert(errs.ErrInvalidRoleName.Equal(err), IsTrue)

	_, err = m.GetRole("nobody")
	c.Assert(err, NotNil)
	c.Assert(errs.ErrRoleNotFound.Equal(err), IsTrue)
	err = m.CreateRole("nobody")
	c.Assert(err, IsNil)
	role, err := m.GetRole("nobody")
	c.Assert(err, IsNil)
	c.Assert(role, DeepEquals, &expectedRole)
}

func (s *testAuthSuite) testDeleteRole(c *C, m *roleManager) {
	err := m.DeleteRole("somebody")
	c.Assert(err, NotNil)
	c.Assert(errs.ErrRoleNotFound.Equal(err), IsTrue)

	err = m.DeleteRole("reader")
	c.Assert(err, IsNil)

	err = m.DeleteRole("reader")
	c.Assert(err, NotNil)
	c.Assert(errs.ErrRoleNotFound.Equal(err), IsTrue)
}

func (s *testAuthSuite) testSetPermissions(c *C, m *roleManager) {
	err := m.SetPermissions("reader", []Permission{
		{Resource: "region", Action: "get"},
		{Resource: "store", Action: "list"},
	})
	c.Assert(err, IsNil)

	role, err := m.GetRole("reader")
	c.Assert(err, IsNil)
	c.Assert(role.GetPermissions(), DeepEquals, []Permission{
		{Resource: "region", Action: "get"},
		{Resource: "store", Action: "list"},
	})
}

func (s *testAuthSuite) testAddPermission(c *C, m *roleManager) {
	err := m.AddPermission("reader", Permission{Resource: "region", Action: "get"})
	c.Assert(err, NotNil)
	c.Assert(errs.ErrRoleHasPermission.Equal(err), IsTrue)

	err = m.AddPermission("reader", Permission{Resource: "region", Action: "update"})
	c.Assert(err, IsNil)

	role, err := m.GetRole("reader")
	c.Assert(err, IsNil)
	c.Assert(role.GetPermissions(), DeepEquals, []Permission{
		{Resource: "region", Action: "get"},
		{Resource: "region", Action: "list"},
		{Resource: "store", Action: "get"},
		{Resource: "store", Action: "list"},
		{Resource: "region", Action: "update"},
	})
}

func (s *testAuthSuite) testRemovePermission(c *C, m *roleManager) {
	err := m.RemovePermission("reader", Permission{Resource: "region", Action: "update"})
	c.Assert(err, NotNil)
	c.Assert(errs.ErrRoleMissingPermission.Equal(err), IsTrue)

	err = m.RemovePermission("reader", Permission{Resource: "region", Action: "get"})
	c.Assert(err, IsNil)

	role, err := m.GetRole("reader")
	c.Assert(err, IsNil)
	c.Assert(role.GetPermissions(), DeepEquals, []Permission{
		{Resource: "store", Action: "list"},
		{Resource: "region", Action: "list"},
		{Resource: "store", Action: "get"},
	})
}

func initKV(c *C, k kv.Base) {
	roles := []struct {
		Name        string `json:"name"`
		Permissions []struct {
			Resource string `json:"resource"`
			Action   string `json:"action"`
		} `json:"permissions"`
	}{
		{Name: "reader", Permissions: []struct {
			Resource string `json:"resource"`
			Action   string `json:"action"`
		}{
			{Resource: "region", Action: "get"},
			{Resource: "region", Action: "list"},
			{Resource: "store", Action: "get"},
			{Resource: "store", Action: "list"},
		}},
		{Name: "writer", Permissions: []struct {
			Resource string `json:"resource"`
			Action   string `json:"action"`
		}{
			{Resource: "region", Action: "delete"},
			{Resource: "region", Action: "update"},
			{Resource: "store", Action: "delete"},
			{Resource: "store", Action: "update"},
		}},
		{Name: "admin", Permissions: []struct {
			Resource string `json:"resource"`
			Action   string `json:"action"`
		}{
			{Resource: "region", Action: "delete"},
			{Resource: "region", Action: "get"},
			{Resource: "region", Action: "list"},
			{Resource: "region", Action: "update"},
			{Resource: "store", Action: "delete"},
			{Resource: "store", Action: "get"},
			{Resource: "store", Action: "list"},
			{Resource: "store", Action: "update"},
			{Resource: "users", Action: "delete"},
			{Resource: "users", Action: "get"},
			{Resource: "users", Action: "list"},
			{Resource: "users", Action: "update"},
		}},
	}
	for _, role := range roles {
		value, err := json.Marshal(role)
		c.Assert(err, IsNil)
		err = k.Save(path.Join(rolePrefix, role.Name), string(value))
		c.Assert(err, IsNil)
	}
}
