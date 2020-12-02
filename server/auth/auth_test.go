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
	"fmt"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/tempurl"
	"github.com/tikv/pd/server/kv"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"reflect"
	"strconv"
	"testing"

	"encoding/json"
	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testAuthSuite{})

type testAuthSuite struct{}
type testFunc func(*C, *roleManager)

func (s *testAuthSuite) TestRoleManager(c *C) {
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

	manager := newRoleManager(kv.NewEtcdKVBase(client, rootPath))
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
		initKV(c, client, rootPath)
		err = manager.UpdateCache()
		c.Assert(err, IsNil)
		f(c, manager)
	}
}

func (s *testAuthSuite) testGetRole(c *C, m *roleManager) {
	expectedRole := Role{
		Name: "reader",
		Permissions: map[Permission]struct{}{
			{Resource: "store", Action: "get"}:   {},
			{Resource: "store", Action: "list"}:  {},
			{Resource: "region", Action: "get"}:  {},
			{Resource: "region", Action: "list"}: {},
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
		{Name: "reader", Permissions: map[Permission]struct{}{
			{Resource: "store", Action: "get"}:   {},
			{Resource: "store", Action: "list"}:  {},
			{Resource: "region", Action: "get"}:  {},
			{Resource: "region", Action: "list"}: {},
		}},
		{Name: "writer", Permissions: map[Permission]struct{}{
			{Resource: "store", Action: "update"}:  {},
			{Resource: "store", Action: "delete"}:  {},
			{Resource: "region", Action: "update"}: {},
			{Resource: "region", Action: "delete"}: {},
		}},
		{Name: "admin", Permissions: map[Permission]struct{}{
			{Resource: "store", Action: "get"}:     {},
			{Resource: "store", Action: "list"}:    {},
			{Resource: "store", Action: "update"}:  {},
			{Resource: "store", Action: "delete"}:  {},
			{Resource: "region", Action: "get"}:    {},
			{Resource: "region", Action: "list"}:   {},
			{Resource: "region", Action: "update"}: {},
			{Resource: "region", Action: "delete"}: {},
			{Resource: "users", Action: "get"}:     {},
			{Resource: "users", Action: "list"}:    {},
			{Resource: "users", Action: "update"}:  {},
			{Resource: "users", Action: "delete"}:  {},
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
	expectedRole := Role{Name: "nobody", Permissions: map[Permission]struct{}{}}
	err := m.CreateRole("reader")
	c.Assert(err, NotNil)
	c.Assert(errs.ErrRoleExists.Equal(err), IsTrue)
	err = m.CreateRole("!")
	c.Assert(err, NotNil)
	c.Assert(errs.ErrInvalidName.Equal(err), IsTrue)

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
	err := m.SetPermissions("reader", map[Permission]struct{}{
		{Resource: "region", Action: "get"}: {},
		{Resource: "store", Action: "list"}: {},
	})
	c.Assert(err, IsNil)

	role, err := m.GetRole("reader")
	c.Assert(err, IsNil)
	c.Assert(role.GetPermissions(), DeepEquals, map[Permission]struct{}{
		{Resource: "region", Action: "get"}: {},
		{Resource: "store", Action: "list"}: {},
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
	c.Assert(role.GetPermissions(), DeepEquals, map[Permission]struct{}{
		{Resource: "region", Action: "get"}:    {},
		{Resource: "region", Action: "list"}:   {},
		{Resource: "region", Action: "update"}: {},
		{Resource: "store", Action: "get"}:     {},
		{Resource: "store", Action: "list"}:    {},
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
	c.Assert(role.GetPermissions(), DeepEquals, map[Permission]struct{}{
		{Resource: "region", Action: "list"}: {},
		{Resource: "store", Action: "get"}:   {},
		{Resource: "store", Action: "list"}:  {},
	})
}

func initKV(c *C, client *clientv3.Client, rootPath string) {
	_, err := client.Delete(context.TODO(), rootPath, clientv3.WithPrefix())
	c.Assert(err, IsNil)

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
	for _, role := range roles {
		value, err := json.Marshal(role)
		c.Assert(err, IsNil)
		_, err = client.Put(context.TODO(), path.Join(rolePrefix, role.Name), string(value))
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
