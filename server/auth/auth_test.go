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
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"reflect"
	"strconv"
	"testing"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"

	"github.com/tikv/pd/pkg/tempurl"
	"github.com/tikv/pd/server/kv"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testAuthSuite{})

type testAuthSuite struct{}
type testFunc func(*C, *Manager)

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

	c.Assert(user.Clone(), DeepEquals, user)

	c.Assert(user.ComparePassword("somethingwrong"), NotNil)
	c.Assert(user.ComparePassword("ItsaCrazilysEcurepass"), IsNil)

	marshalledUser := "{\"username\":\"test\"," +
		"\"hash\":\"a0705ec8c6c0dab42344570b50608935174f84b5c365e9ff2b3ed92e0fc8e037\"," +
		"\"roles\":[\"reader\",\"writer\"]}"
	j, err := json.Marshal(user)
	c.Assert(err, IsNil)
	c.Assert(string(j), Equals, marshalledUser)

	unmarshalledUser, err := NewUserFromJSON(string(j))
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
	c.Assert(err, IsNil)
	p2, err := NewPermission("region", "list")
	c.Assert(err, IsNil)
	p3, err := NewPermission("region", "get")
	c.Assert(err, IsNil)
	p4, err := NewPermission("region", "delete")
	c.Assert(err, IsNil)
	role.permissions = map[Permission]struct{}{*p1: {}, *p2: {}, *p3: {}}

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

func (s *testAuthSuite) TestManager(c *C) {
	cfg := newTestSingleConfig()
	defer cleanConfig(cfg)
	etcd, err := embed.StartEtcd(cfg)
	c.Assert(err, IsNil)
	defer etcd.Close()

	ep := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	c.Assert(err, IsNil)
	rootPath := path.Join("/pd", strconv.FormatUint(100, 10))

	manager := NewManager(kv.NewEtcdKVBase(client, rootPath))
	testFuncs := []testFunc{
		s.testGetUser,
		s.testGetUsers,
		s.testCreateUser,
		s.testDeleteUser,
		s.testChangePassword,
		s.testSetRole,
		s.testAddRole,
		s.testRemoveRole,
		s.testHasPermissions,
		s.testGetRole,
		s.testGetRoles,
		s.testCreateRole,
		s.testDeleteRole,
		s.testSetPermissions,
		s.testAddPermission,
		s.testRemovePermission,
	}
	for _, f := range testFuncs {
		initKV(c, client, rootPath)
		manager.InvalidateCache()
		f(c, manager)
	}
}

func (s *testAuthSuite) testGetUser(c *C, m *Manager) {
	expectedUser := User{
		username: "bob",
		hash:     "da7655b5bf67039c3e76a99d8e6fb6969370bbc0fa440cae699cf1a3e2f1e0a1",
		roleKeys: map[string]struct{}{"reader": {}, "writer": {}},
	}
	for range [2]int{} {
		user, err := m.GetUser("bob")
		c.Assert(err, IsNil)
		c.Assert(user, DeepEquals, &expectedUser)
		_, err = m.GetUser("john")
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, ErrUserNotFound("john").Error())
	}
}

func (s *testAuthSuite) testGetUsers(c *C, m *Manager) {
	expectedUsers := []User{
		{
			username: "alice",
			hash:     "13dc8554575637802eec3c0117f41591a990e1a2d37160018c48c9125063838a",
			roleKeys: map[string]struct{}{"reader": {}}},
		{
			username: "bob",
			hash:     "da7655b5bf67039c3e76a99d8e6fb6969370bbc0fa440cae699cf1a3e2f1e0a1",
			roleKeys: map[string]struct{}{"reader": {}, "writer": {}}},
		{
			username: "lambda",
			hash:     "f9f967e71dff16bd5ce92e62d50140503a3ce399f294b1848adb210149bc1fd0",
			roleKeys: map[string]struct{}{"admin": {}},
		},
	}
	users, err := m.GetUsers()
	c.Assert(err, IsNil)
	c.Assert(len(users), Equals, 3)
	for user := range users {
		hasUser := false
		for _, expectedUser := range expectedUsers {
			if user.username == expectedUser.username &&
				user.hash == expectedUser.hash &&
				reflect.DeepEqual(user.roleKeys, expectedUser.roleKeys) {
				hasUser = true
				break
			}
		}
		c.Assert(hasUser, IsTrue)
	}
}

func (s *testAuthSuite) testCreateUser(c *C, m *Manager) {
	expectedUser := User{username: "jane", hash: "100e060425c270b01138bc4ed9b498897d2ec525baa766d9a57004b318e99e19", roleKeys: map[string]struct{}{}}
	err := m.CreateUser("bob", "bobpass")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, ErrUserExists("bob").Error())
	err = m.CreateUser("!", "!pass")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, ErrInvalidName.Error())

	_, err = m.GetUser("jane")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, ErrUserNotFound("jane").Error())
	err = m.CreateUser("jane", "janepass")
	c.Assert(err, IsNil)
	user, err := m.GetUser("jane")
	c.Assert(err, IsNil)
	c.Assert(user, DeepEquals, &expectedUser)
}

func (s *testAuthSuite) testDeleteUser(c *C, m *Manager) {
	err := m.DeleteUser("john")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, ErrUserNotFound("john").Error())

	err = m.DeleteUser("alice")
	c.Assert(err, IsNil)
	err = m.DeleteUser("alice")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, ErrUserNotFound("alice").Error())
}

func (s *testAuthSuite) testChangePassword(c *C, m *Manager) {
	user, err := m.GetUser("alice")
	c.Assert(err, IsNil)
	c.Assert(user.ComparePassword("alicepass"), IsNil)

	err = m.ChangePassword("alice", "testpass")
	c.Assert(err, IsNil)

	user, err = m.GetUser("alice")
	c.Assert(err, IsNil)
	c.Assert(user.ComparePassword("testpass"), IsNil)
}

func (s *testAuthSuite) testSetRole(c *C, m *Manager) {
	err := m.SetRole("alice", map[string]struct{}{"somebody": {}, "admin": {}})
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, ErrRoleNotFound("somebody").Error())

	err = m.SetRole("alice", map[string]struct{}{"writer": {}, "admin": {}})
	c.Assert(err, IsNil)

	user, err := m.GetUser("alice")
	c.Assert(err, IsNil)
	c.Assert(user.GetRoleKeys(), DeepEquals, map[string]struct{}{"writer": {}, "admin": {}})
}

func (s *testAuthSuite) testAddRole(c *C, m *Manager) {
	err := m.AddRole("alice", "reader")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, ErrUserHasRole("alice", "reader").Error())

	err = m.AddRole("alice", "somebody")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, ErrRoleNotFound("somebody").Error())

	err = m.AddRole("alice", "writer")
	c.Assert(err, IsNil)

	user, err := m.GetUser("alice")
	c.Assert(err, IsNil)
	c.Assert(user.GetRoleKeys(), DeepEquals, map[string]struct{}{"reader": {}, "writer": {}})
}

func (s *testAuthSuite) testRemoveRole(c *C, m *Manager) {
	err := m.RemoveRole("alice", "writer")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, ErrUserMissingRole("alice", "writer").Error())

	err = m.RemoveRole("alice", "somebody")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, ErrRoleNotFound("somebody").Error())

	err = m.RemoveRole("alice", "reader")
	c.Assert(err, IsNil)

	user, err := m.GetUser("alice")
	c.Assert(err, IsNil)
	c.Assert(user.GetRoleKeys(), DeepEquals, map[string]struct{}{})
}

func (s *testAuthSuite) testHasPermissions(c *C, m *Manager) {
	_, err := m.HasPermissions("john", map[Permission]struct{}{})
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, ErrUserNotFound("john").Error())

	testCases := []struct {
		username    string
		permissions map[Permission]struct{}
		expected    bool
	}{
		{
			username:    "alice",
			permissions: map[Permission]struct{}{{resource: "store", action: "get"}: {}},
			expected:    true,
		},
		{
			username:    "alice",
			permissions: map[Permission]struct{}{{resource: "store", action: "get"}: {}, {resource: "region", action: "list"}: {}},
			expected:    true,
		},
		{
			username:    "alice",
			permissions: map[Permission]struct{}{{resource: "store", action: "update"}: {}},
			expected:    false,
		},
		{
			username:    "alice",
			permissions: map[Permission]struct{}{{resource: "store", action: "get"}: {}, {resource: "region", action: "update"}: {}},
			expected:    false,
		},
		{
			username:    "alice",
			permissions: map[Permission]struct{}{{resource: "user", action: "get"}: {}},
			expected:    false,
		},
	}
	for _, testCase := range testCases {
		hasPermission, err := m.HasPermissions(testCase.username, testCase.permissions)
		c.Assert(err, IsNil)
		c.Assert(hasPermission, Equals, testCase.expected)
	}
}

func (s *testAuthSuite) testGetRole(c *C, m *Manager) {
	expectedRole := Role{
		name: "reader",
		permissions: map[Permission]struct{}{
			{resource: "store", action: "get"}:   {},
			{resource: "store", action: "list"}:  {},
			{resource: "region", action: "get"}:  {},
			{resource: "region", action: "list"}: {},
		},
	}
	for range [2]int{} {
		role, err := m.GetRole("reader")
		c.Assert(err, IsNil)
		c.Assert(role, DeepEquals, &expectedRole)
		_, err = m.GetRole("somebody")
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, ErrRoleNotFound("somebody").Error())
	}
}

func (s *testAuthSuite) testGetRoles(c *C, m *Manager) {
	expectedRoles := []Role{
		{name: "reader", permissions: map[Permission]struct{}{
			{resource: "store", action: "get"}:   {},
			{resource: "store", action: "list"}:  {},
			{resource: "region", action: "get"}:  {},
			{resource: "region", action: "list"}: {},
		}},
		{name: "writer", permissions: map[Permission]struct{}{
			{resource: "store", action: "update"}:  {},
			{resource: "store", action: "delete"}:  {},
			{resource: "region", action: "update"}: {},
			{resource: "region", action: "delete"}: {},
		}},
		{name: "admin", permissions: map[Permission]struct{}{
			{resource: "store", action: "get"}:     {},
			{resource: "store", action: "list"}:    {},
			{resource: "store", action: "update"}:  {},
			{resource: "store", action: "delete"}:  {},
			{resource: "region", action: "get"}:    {},
			{resource: "region", action: "list"}:   {},
			{resource: "region", action: "update"}: {},
			{resource: "region", action: "delete"}: {},
			{resource: "users", action: "get"}:     {},
			{resource: "users", action: "list"}:    {},
			{resource: "users", action: "update"}:  {},
			{resource: "users", action: "delete"}:  {},
		}},
	}
	roles, err := m.GetRoles()
	c.Assert(err, IsNil)
	c.Assert(len(roles), Equals, 3)
	for role := range roles {
		hasRole := false
		for _, expectedRole := range expectedRoles {
			if role.name == expectedRole.name &&
				reflect.DeepEqual(role.permissions, expectedRole.permissions) {
				hasRole = true
				break
			}
		}
		c.Assert(hasRole, IsTrue)
	}
}

func (s *testAuthSuite) testCreateRole(c *C, m *Manager) {
	expectedRole := Role{name: "nobody", permissions: map[Permission]struct{}{}}
	err := m.CreateRole("reader")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, ErrRoleExists("reader").Error())
	err = m.CreateRole("!")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, ErrInvalidName.Error())

	_, err = m.GetRole("nobody")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, ErrRoleNotFound("nobody").Error())
	err = m.CreateRole("nobody")
	c.Assert(err, IsNil)
	role, err := m.GetRole("nobody")
	c.Assert(err, IsNil)
	c.Assert(role, DeepEquals, &expectedRole)
}

func (s *testAuthSuite) testDeleteRole(c *C, m *Manager) {
	err := m.DeleteRole("somebody")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, ErrRoleNotFound("somebody").Error())

	err = m.DeleteRole("reader")
	c.Assert(err, IsNil)

	for _, username := range [3]string{"alice", "bob", "lambda"} {
		user, err := m.GetUser(username)
		c.Assert(err, IsNil)
		c.Assert(user.HasRole("reader"), IsFalse)
	}

	err = m.DeleteRole("reader")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, ErrRoleNotFound("reader").Error())
}

func (s *testAuthSuite) testSetPermissions(c *C, m *Manager) {
	err := m.SetPermissions("reader", map[Permission]struct{}{
		{resource: "region", action: "get"}: {},
		{resource: "store", action: "list"}: {},
	})
	c.Assert(err, IsNil)

	role, err := m.GetRole("reader")
	c.Assert(err, IsNil)
	c.Assert(role.GetPermissions(), DeepEquals, map[Permission]struct{}{
		{resource: "region", action: "get"}: {},
		{resource: "store", action: "list"}: {},
	})
}

func (s *testAuthSuite) testAddPermission(c *C, m *Manager) {
	err := m.AddPermission("reader", Permission{resource: "region", action: "get"})
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, ErrRoleHasPermission("reader", Permission{resource: "region", action: "get"}).Error())

	err = m.AddPermission("reader", Permission{resource: "region", action: "update"})
	c.Assert(err, IsNil)

	role, err := m.GetRole("reader")
	c.Assert(err, IsNil)
	c.Assert(role.GetPermissions(), DeepEquals, map[Permission]struct{}{
		{resource: "region", action: "get"}:    {},
		{resource: "region", action: "list"}:   {},
		{resource: "region", action: "update"}: {},
		{resource: "store", action: "get"}:     {},
		{resource: "store", action: "list"}:    {},
	})
}

func (s *testAuthSuite) testRemovePermission(c *C, m *Manager) {
	err := m.RemovePermission("reader", Permission{resource: "region", action: "update"})
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, ErrRoleMissingPermission("reader", Permission{resource: "region", action: "update"}).Error())

	err = m.RemovePermission("reader", Permission{resource: "region", action: "get"})
	c.Assert(err, IsNil)

	role, err := m.GetRole("reader")
	c.Assert(err, IsNil)
	c.Assert(role.GetPermissions(), DeepEquals, map[Permission]struct{}{
		{resource: "region", action: "list"}: {},
		{resource: "store", action: "get"}:   {},
		{resource: "store", action: "list"}:  {},
	})
}

func initKV(c *C, client *clientv3.Client, rootPath string) {
	_, err := client.Delete(context.TODO(), rootPath, clientv3.WithPrefix())
	c.Assert(err, IsNil)

	userPrefix := path.Join(rootPath, "users")
	rolePrefix := path.Join(rootPath, "roles")

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
			{Resource: "store", Action: "update"},
			{Resource: "store", Action: "delete"},
			{Resource: "store", Action: "get"},
			{Resource: "store", Action: "list"},
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
		_, err = client.Put(context.TODO(), path.Join(rolePrefix, role.Name), string(value))
		c.Assert(err, IsNil)
	}
	for _, user := range users {
		value, err := json.Marshal(user)
		c.Assert(err, IsNil)
		userPath := path.Join(userPrefix, user.Username)
		_, err = client.Put(context.TODO(), userPath, string(value))
		c.Assert(err, IsNil)
	}
}

func newTestSingleConfig() *embed.Config {
	cfg := embed.NewConfig()
	cfg.Name = "test_etcd"
	cfg.Dir, _ = ioutil.TempDir("/tmp", "test_etcd")
	cfg.WalDir = ""
	cfg.Logger = "zap"
	cfg.LogOutputs = []string{"stdout"}

	pu, _ := url.Parse(tempurl.Alloc())
	cfg.LPUrls = []url.URL{*pu}
	cfg.APUrls = cfg.LPUrls
	cu, _ := url.Parse(tempurl.Alloc())
	cfg.LCUrls = []url.URL{*cu}
	cfg.ACUrls = cfg.LCUrls

	cfg.StrictReconfigCheck = false
	cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, &cfg.LPUrls[0])
	cfg.ClusterState = embed.ClusterStateFlagNew
	return cfg
}

func cleanConfig(cfg *embed.Config) {
	// Clean data directory
	_ = os.RemoveAll(cfg.Dir)
}
