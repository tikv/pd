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
type testUserFunc func(*C, *userManager)
type testRoleFunc func(*C, *roleManager)

func (s *testAuthSuite) TestRoleManager(c *C) {
	testFuncs := []testRoleFunc{
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

func (s *testAuthSuite) TestUserManager(c *C) {
	testFuncs := []testUserFunc{
		s.testGetUser,
		s.testGetUsers,
		s.testCreateUser,
		s.testDeleteUser,
		s.testChangePassword,
		s.testSetRoles,
		s.testAddRole,
		s.testRemoveRole,
	}
	for _, f := range testFuncs {
		k := kv.NewMemoryKV()
		initKV(c, k)
		manager := newUserManager(k)
		err := manager.UpdateCache()
		c.Assert(err, IsNil)
		f(c, manager)
	}
}

func (s *testAuthSuite) testGetUser(c *C, m *userManager) {
	expectedUser := User{
		Username: "bob",
		Hash:     "da7655b5bf67039c3e76a99d8e6fb6969370bbc0fa440cae699cf1a3e2f1e0a1",
		RoleKeys: []string{"reader", "writer"},
	}
	user, err := m.GetUser("bob")
	c.Assert(err, IsNil)
	c.Assert(user, DeepEquals, &expectedUser)
	_, err = m.GetUser("john")
	c.Assert(err, NotNil)
	c.Assert(errs.ErrUserNotFound.Equal(err), IsTrue)
}

func (s *testAuthSuite) testGetUsers(c *C, m *userManager) {
	expectedUsers := []User{
		{
			Username: "alice",
			Hash:     "13dc8554575637802eec3c0117f41591a990e1a2d37160018c48c9125063838a",
			RoleKeys: []string{"reader"}},
		{
			Username: "bob",
			Hash:     "da7655b5bf67039c3e76a99d8e6fb6969370bbc0fa440cae699cf1a3e2f1e0a1",
			RoleKeys: []string{"reader", "writer"}},
		{
			Username: "lambda",
			Hash:     "f9f967e71dff16bd5ce92e62d50140503a3ce399f294b1848adb210149bc1fd0",
			RoleKeys: []string{"admin"},
		},
	}
	users := m.GetUsers()
	c.Assert(len(users), Equals, 3)
	for _, user := range users {
		hasUser := false
		for _, expectedUser := range expectedUsers {
			if user.Username == expectedUser.Username &&
				user.Hash == expectedUser.Hash &&
				reflect.DeepEqual(user.RoleKeys, expectedUser.RoleKeys) {
				hasUser = true
				break
			}
		}
		c.Assert(hasUser, IsTrue)
	}
}

func (s *testAuthSuite) testCreateUser(c *C, m *userManager) {
	expectedUser := User{Username: "jane", Hash: "100e060425c270b01138bc4ed9b498897d2ec525baa766d9a57004b318e99e19", RoleKeys: []string{}}
	err := m.CreateUser("bob", "bobpass")
	c.Assert(err, NotNil)
	c.Assert(errs.ErrUserExists.Equal(err), IsTrue)
	err = m.CreateUser("!", "!pass")
	c.Assert(err, NotNil)
	c.Assert(errs.ErrInvalidUserName.Equal(err), IsTrue)

	_, err = m.GetUser("jane")
	c.Assert(err, NotNil)
	c.Assert(errs.ErrUserNotFound.Equal(err), IsTrue)
	err = m.CreateUser("jane", "janepass")
	c.Assert(err, IsNil)
	user, err := m.GetUser("jane")
	c.Assert(err, IsNil)
	c.Assert(user, DeepEquals, &expectedUser)
}

func (s *testAuthSuite) testDeleteUser(c *C, m *userManager) {
	err := m.DeleteUser("john")
	c.Assert(err, NotNil)
	c.Assert(errs.ErrUserNotFound.Equal(err), IsTrue)

	err = m.DeleteUser("alice")
	c.Assert(err, IsNil)
	err = m.DeleteUser("alice")
	c.Assert(err, NotNil)
	c.Assert(errs.ErrUserNotFound.Equal(err), IsTrue)
}

func (s *testAuthSuite) testChangePassword(c *C, m *userManager) {
	err := m.ChangePassword("john", "johnpass")
	c.Assert(err, NotNil)
	c.Assert(errs.ErrUserNotFound.Equal(err), IsTrue)

	user, err := m.GetUser("alice")
	c.Assert(err, IsNil)
	c.Assert(user.ComparePassword("alicepass"), IsNil)

	err = m.ChangePassword("alice", "testpass")
	c.Assert(err, IsNil)

	user, err = m.GetUser("alice")
	c.Assert(err, IsNil)
	c.Assert(user.ComparePassword("testpass"), IsNil)
}

func (s *testAuthSuite) testSetRoles(c *C, m *userManager) {
	err := m.SetRoles("alice", []string{"writer", "admin"})
	c.Assert(err, IsNil)

	user, err := m.GetUser("alice")
	c.Assert(err, IsNil)
	c.Assert(user.GetRoleKeys(), DeepEquals, []string{"writer", "admin"})
}

func (s *testAuthSuite) testAddRole(c *C, m *userManager) {
	err := m.AddRole("alice", "reader")
	c.Assert(err, NotNil)
	c.Assert(errs.ErrUserHasRole.Equal(err), IsTrue)

	err = m.AddRole("alice", "writer")
	c.Assert(err, IsNil)

	user, err := m.GetUser("alice")
	c.Assert(err, IsNil)
	c.Assert(user.GetRoleKeys(), DeepEquals, []string{"reader", "writer"})
}

func (s *testAuthSuite) testRemoveRole(c *C, m *userManager) {
	err := m.RemoveRole("alice", "writer")
	c.Assert(err, NotNil)
	c.Assert(errs.ErrUserMissingRole.Equal(err), IsTrue)

	err = m.RemoveRole("alice", "reader")
	c.Assert(err, IsNil)

	user, err := m.GetUser("alice")
	c.Assert(err, IsNil)
	c.Assert(user.GetRoleKeys(), DeepEquals, []string{})
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
	users := []struct {
		Username string   `json:"username"`
		Hash     string   `json:"hash"`
		Roles    []string `json:"roles"`
	}{
		{
			Username: "alice",
			Hash:     "13dc8554575637802eec3c0117f41591a990e1a2d37160018c48c9125063838a", // pass: alicepass
			Roles:    []string{"reader"}},
		{
			Username: "bob",
			Hash:     "da7655b5bf67039c3e76a99d8e6fb6969370bbc0fa440cae699cf1a3e2f1e0a1", // pass: bobpass
			Roles:    []string{"reader", "writer"}},
		{
			Username: "lambda",
			Hash:     "f9f967e71dff16bd5ce92e62d50140503a3ce399f294b1848adb210149bc1fd0", // pass: lambdapass
			Roles:    []string{"admin"}},
	}
	for _, role := range roles {
		value, err := json.Marshal(role)
		c.Assert(err, IsNil)
		err = k.Save(GetRolePath(role.Name), string(value))
		c.Assert(err, IsNil)
	}
	for _, user := range users {
		value, err := json.Marshal(user)
		c.Assert(err, IsNil)
		err = k.Save(GetUserPath(user.Username), string(value))
		c.Assert(err, IsNil)
	}
}
