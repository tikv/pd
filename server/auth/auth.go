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
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"go.uber.org/zap"
	"path"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/tikv/pd/server/kv"
	"go.etcd.io/etcd/clientv3"
)

var (
	ErrorUserNotFound    = func(name string) error { return errors.Errorf("user not found: %s", name) }
	ErrorUserExists      = func(name string) error { return errors.Errorf("user already exists: %s", name) }
	ErrorRoleNotFound    = func(name string) error { return errors.Errorf("role not found: %s", name) }
	ErrorRoleExists      = func(name string) error { return errors.Errorf("role already exists: %s", name) }
	ErrorUserHasRole     = func(user string, role string) error { return errors.Errorf("user %s already has role: %s", user, role) }
	ErrorUserMissingRole = func(user string, role string) error {
		return errors.Errorf("user %s doesn't have role: %s", user, role)
	}
	ErrorRoleHasPermission = func(role string, permission Permission) error {
		return errors.Errorf("role %s already has permission: %s", role, permission.String())
	}
	ErrorRoleMissingPermission = func(role string, permission Permission) error {
		return errors.Errorf("role %s doesn't have permission: %s", role, permission.String())
	}
)

var (
	userPrefix = "users"
	rolePrefix = "roles"
)

type Manager struct {
	kv    kv.TxnBase
	users sync.Map // map[string]*User
	roles sync.Map // map[string]*Role
}

func NewManager(k kv.TxnBase) *Manager {
	return &Manager{kv: k}
}

func (m *Manager) GetUser(name string) (*User, error) {
	_cachedUser, ok := m.users.Load(name)
	// Cache hit, so we return cached value.
	if ok {
		cachedUser := _cachedUser.(*User)
		// Nil in cache implies this user doesn't exist
		if cachedUser == nil {
			return nil, ErrorUserNotFound(name)
		}
		return cachedUser, nil
	}

	// Cache miss. Load value from etcd.
	userJson, err := m.kv.Load(path.Join(userPrefix, name))
	if err != nil {
		return nil, err
	}

	var user *User
	if userJson == "" {
		user = nil
	} else {
		user, err = NewUserFromJson(userJson)
		if err != nil {
			return nil, err
		}
	}

	// If another (write) operation has already updated the cache,
	// we abandon the data we've just retrieved and use the cached version.
	_latestUser, _ := m.users.LoadOrStore(name, user)
	latestUser := _latestUser.(*User)

	if latestUser == nil {
		return nil, ErrorUserNotFound(name)
	}
	return latestUser, nil
}

func (m *Manager) GetUsers() (map[*User]struct{}, error) {
	// NOTE: GetUsers doesn't use the cache and read data from kv directly to avoid cache inconsistency.
	usersPath := strings.Join([]string{userPrefix, ""}, "/")
	_, _users, err := m.kv.LoadRange(usersPath, clientv3.GetPrefixRangeEnd(usersPath), 0)
	if err != nil {
		return nil, err
	}

	users := make(map[*User]struct{})
	for _, userJson := range _users {
		user, err := NewUserFromJson(userJson)
		if err != nil {
			return nil, err
		}

		users[user] = struct{}{}
	}

	return users, nil
}

func (m *Manager) GetUserKeys() (map[string]struct{}, error) {
	// NOTE: GetRoleKeys doesn't use the cache and read data from kv directly to avoid cache inconsistency.
	usersPath := strings.Join([]string{userPrefix, ""}, "/")
	_keys, _, err := m.kv.LoadRange(usersPath, clientv3.GetPrefixRangeEnd(usersPath), 0)
	if err != nil {
		return nil, err
	}

	keys := make(map[string]struct{})
	for _, key := range _keys {
		keys[key] = struct{}{}
	}

	return keys, nil
}

func (m *Manager) CreateUser(name string, password string) error {
	user, err := m.GetUser(name)
	if err == nil {
		return ErrorUserExists(name)
	}
	if err.Error() != ErrorUserNotFound(name).Error() {
		return err
	}

	user, err = NewUser(name, GenerateHash(password))
	if err != nil {
		return err
	}

	// Try to add user to kv first.
	userJson, err := json.Marshal(user)
	if err != nil {
		return err
	}
	userPath := path.Join(userPrefix, name)

	_, err = m.kv.NewTxn().
		If(kv.Eq(kv.Create(userPath), 0)).
		Then(kv.OpSave(name, string(userJson))).
		Commit()
	if err != nil {
		return err
	}

	// Add user to memory cache.
	m.users.Store(name, user)

	return nil
}

func (m *Manager) DeleteUser(name string) error {
	user, err := m.GetUser(name)
	if err != nil {
		return err
	}

	// Try to delete user from kv first.
	userJson, err := json.Marshal(user)
	if err != nil {
		return err
	}
	userPath := path.Join(userPrefix, name)

	_, err = m.kv.NewTxn().
		If(kv.Eq(kv.Value(userPath), string(userJson))).
		Then(kv.OpRemove(userPath)).
		Commit()
	if err != nil {
		return err
	}

	// Delete user from memory cache.
	m.users.Store(name, (*User)(nil))

	return nil
}

func (m *Manager) ChangePassword(name string, password string) error {
	_user, err := m.GetUser(name)
	if err != nil {
		return err
	}

	_userJson, err := json.Marshal(_user)
	if err != nil {
		return err
	}

	user := _user.Clone()
	user.hash = GenerateHash(password)

	// Try to update user in kv first.
	userJson, err := json.Marshal(user)
	if err != nil {
		return err
	}
	userPath := path.Join(userPrefix, name)

	_, err = m.kv.NewTxn().
		If(kv.Eq(kv.Value(userPath), string(_userJson))).
		Then(kv.OpSave(userPath, string(userJson))).
		Commit()
	if err != nil {
		return err
	}

	// Update user in memory cache.
	m.users.Store(name, user)

	return nil
}

func (m *Manager) SetRole(name string, roles map[string]struct{}) error {
	_user, err := m.GetUser(name)
	if err != nil {
		return err
	}

	_userJson, err := json.Marshal(_user)
	if err != nil {
		return err
	}

	cmps := make([]kv.Cmp, 0)
	for role := range roles {
		_, err := m.GetRole(role)
		if err != nil {
			return err
		}
		cmps = append(cmps, kv.Gt(kv.Create(path.Join(rolePrefix, role)), 0))
	}

	user := _user.Clone()
	user.roleKeys = roles

	// Try to update user in kv first.
	userJson, err := json.Marshal(user)
	if err != nil {
		return err
	}
	userPath := path.Join(userPrefix, name)

	cmps = append(cmps, kv.Eq(kv.Value(userPath), string(_userJson)))
	_, err = m.kv.NewTxn().
		If(cmps...).
		Then(kv.OpSave(userPath, string(userJson))).
		Commit()
	if err != nil {
		return err
	}

	// Update user in memory cache.
	m.users.Store(name, user)

	return nil
}

func (m *Manager) AddRole(name string, roleName string) error {
	_user, err := m.GetUser(name)
	if err != nil {
		return err
	}
	_, err = m.GetRole(roleName)
	if err != nil {
		return err
	}

	if _, ok := _user.roleKeys[roleName]; ok {
		return ErrorUserHasRole(name, roleName)
	}
	_, err = m.GetRole(roleName)
	if err != nil {
		return ErrorRoleNotFound(roleName)
	}

	_userJson, err := json.Marshal(_user)
	if err != nil {
		return err
	}

	user := _user.Clone()
	user.roleKeys[roleName] = struct{}{}

	// Try to update user in kv first.
	userJson, err := json.Marshal(user)
	if err != nil {
		return err
	}
	userPath := path.Join(userPrefix, name)
	rolePath := path.Join(rolePrefix, roleName)

	_, err = m.kv.NewTxn().
		If(kv.Eq(kv.Value(userPath), string(_userJson)),
			kv.Gt(kv.Create(rolePath), 0)).
		Then(kv.OpSave(userPath, string(userJson))).
		Commit()
	if err != nil {
		return err
	}

	// Update user in memory cache.
	m.users.Store(name, user)

	return nil
}

func (m *Manager) RemoveRole(name string, roleName string) error {
	_user, err := m.GetUser(name)
	if err != nil {
		return err
	}
	_, err = m.GetRole(roleName)
	if err != nil {
		return err
	}

	if _, ok := _user.roleKeys[roleName]; !ok {
		return ErrorUserMissingRole(name, roleName)
	}

	_userJson, err := json.Marshal(_user)
	if err != nil {
		return err
	}

	user := _user.Clone()
	delete(user.roleKeys, roleName)

	// Try to update user in kv first.
	userJson, err := json.Marshal(user)
	if err != nil {
		return err
	}
	userPath := path.Join(userPrefix, name)

	_, err = m.kv.NewTxn().
		If(kv.Eq(kv.Value(userPath), string(_userJson))).
		Then(kv.OpSave(userPath, string(userJson))).
		Commit()
	if err != nil {
		return err
	}

	// Update user in memory cache.
	m.users.Store(name, user)

	return nil
}

func (m *Manager) HasPermissions(name string, expectedPermissions map[Permission]struct{}) (bool, error) {
	user, err := m.GetUser(name)
	if err != nil {
		return false, err
	}

	for roleName := range user.roleKeys {
		if len(expectedPermissions) == 0 {
			break
		}

		role, err := m.GetRole(roleName)
		if err != nil {
			if err.Error() == ErrorRoleNotFound(roleName).Error() {
				log.Warn("missing role when validating user permission. skipped.", zap.String("user", name), errs.ZapError(err))
				continue
			} else {
				return false, err
			}
		}

		for expectedPermission := range expectedPermissions {
			hasPermission := role.HasPermission(expectedPermission)
			if hasPermission {
				delete(expectedPermissions, expectedPermission)
			}
		}
	}

	return len(expectedPermissions) == 0, nil
}

func (m *Manager) GetRole(name string) (*Role, error) {
	_cachedRole, ok := m.roles.Load(name)
	// Cache hit, so we return cached value.
	if ok {
		cachedRole := _cachedRole.(*Role)
		// Nil in cache implies this user doesn't exist
		if cachedRole == nil {
			return nil, ErrorRoleNotFound(name)
		}
		return cachedRole, nil
	}

	// Cache miss. Load value from etcd.
	roleJson, err := m.kv.Load(path.Join(rolePrefix, name))
	if err != nil {
		return nil, err
	}

	var role *Role
	if roleJson == "" {
		role = nil
	} else {
		role, err = NewRoleFromJson(roleJson)
		if err != nil {
			return nil, err
		}
	}

	// If another (write) operation has already updated the cache,
	// we abandon the data we've just retrieved and use the cached version.
	_latestRole, _ := m.roles.LoadOrStore(name, role)
	latestRole := _latestRole.(*Role)

	if latestRole == nil {
		return nil, ErrorRoleNotFound(name)
	}
	return latestRole, nil
}

func (m *Manager) GetRoles() (map[*Role]struct{}, error) {
	// NOTE: GetRoles doesn't use the cache and read data from kv directly to avoid cache inconsistency.
	rolesPath := strings.Join([]string{rolePrefix, ""}, "/")
	_, _roles, err := m.kv.LoadRange(rolesPath, clientv3.GetPrefixRangeEnd(rolesPath), 0)
	if err != nil {
		return nil, err
	}

	roles := make(map[*Role]struct{})
	for _, roleJson := range _roles {
		role, err := NewRoleFromJson(roleJson)
		if err != nil {
			return nil, err
		}

		roles[role] = struct{}{}
	}

	return roles, nil
}

func (m *Manager) GetRoleKeys() (map[string]struct{}, error) {
	// NOTE: GetRoleKeys doesn't use the cache and read data from kv directly to avoid cache inconsistency.
	rolesPath := strings.Join([]string{rolePrefix, ""}, "/")
	_keys, _, err := m.kv.LoadRange(rolesPath, clientv3.GetPrefixRangeEnd(rolesPath), 0)
	if err != nil {
		return nil, err
	}

	keys := make(map[string]struct{})
	for _, key := range _keys {
		keys[key] = struct{}{}
	}

	return keys, nil
}

func (m *Manager) CreateRole(name string) error {
	role, err := m.GetRole(name)
	if err == nil {
		return ErrorRoleExists(name)
	}
	if err.Error() != ErrorRoleNotFound(name).Error() {
		return err
	}

	role, err = NewRole(name)
	if err != nil {
		return err
	}

	// Try to add role to kv first.
	roleJson, err := json.Marshal(role)
	if err != nil {
		return err
	}
	rolePath := path.Join(rolePrefix, name)

	_, err = m.kv.NewTxn().
		If(kv.Eq(kv.Create(rolePath), 0)).
		Then(kv.OpSave(name, string(roleJson))).
		Commit()
	if err != nil {
		return err
	}

	// Add role to memory cache.
	m.roles.Store(name, role)

	return nil
}

func (m *Manager) DeleteRole(name string) error {
	role, err := m.GetRole(name)
	if err != nil {
		return err
	}

	// Try to delete role from kv first.
	// First remove the role from all users
	users, err := m.GetUsers()
	if err != nil {
		return err
	}

	editedUsers := make([]string, 0)
	cmps := make([]kv.Cmp, 0)
	ops := make([]kv.Op, 0)
	for user := range users {
		if user.HasRole(name) {
			roles := user.GetRoleKeys()
			newRoles := make(map[string]struct{})
			for role := range roles {
				if role != name {
					newRoles[role] = struct{}{}
				}
			}
			newUser := User{username: user.username, hash: user.hash, roleKeys: newRoles}
			userJson, err := user.MarshalJSON()
			if err != nil {
				return err
			}
			newUserJson, err := newUser.MarshalJSON()
			if err != nil {
				return err
			}

			editedUsers = append(editedUsers, name)
			userPath := path.Join(userPrefix, user.username)
			cmps = append(cmps, kv.Eq(kv.Value(userPath), string(userJson)))
			ops = append(ops, kv.OpSave(userPath, string(newUserJson)))
		}
	}

	// Then delete the role itself
	roleJson, err := json.Marshal(role)
	if err != nil {
		return err
	}
	rolePath := path.Join(rolePrefix, name)

	cmps = append(cmps, kv.Eq(kv.Value(rolePath), string(roleJson)))
	ops = append(ops, kv.OpRemove(rolePath))
	_, err = m.kv.NewTxn().
		If(cmps...).
		Then(ops...).
		Commit()
	if err != nil {
		return err
	}

	// Delete role from memory cache.
	m.roles.Store(name, (*Role)(nil))
	// Invalidate cache of all related users.
	m.users.Range(func(key, value interface{}) bool {
		for _, editedUser := range editedUsers {
			if key == editedUser {
				m.users.Delete(key)
				break
			}
		}
		return true
	})

	return nil
}

func (m *Manager) SetPermissions(name string, permissions map[Permission]struct{}) error {
	_role, err := m.GetRole(name)
	if err != nil {
		return err
	}

	_roleJson, err := json.Marshal(_role)
	if err != nil {
		return err
	}

	role := _role.Clone()
	role.permissions = permissions

	// Try to update user in kv first.
	roleJson, err := json.Marshal(role)
	if err != nil {
		return err
	}
	rolePath := path.Join(rolePrefix, name)

	_, err = m.kv.NewTxn().
		If(kv.Eq(kv.Value(rolePath), string(_roleJson))).
		Then(kv.OpSave(rolePath, string(roleJson))).
		Commit()
	if err != nil {
		return err
	}

	// Update role in memory cache.
	m.roles.Store(name, role)

	return nil
}

func (m *Manager) AddPermission(name string, permission Permission) error {
	_role, err := m.GetRole(name)
	if err != nil {
		return err
	}

	if _, ok := _role.permissions[permission]; ok {
		return ErrorRoleHasPermission(name, permission)
	}

	_roleJson, err := json.Marshal(_role)
	if err != nil {
		return err
	}

	role := _role.Clone()
	role.permissions[permission] = struct{}{}

	// Try to update user in kv first.
	roleJson, err := json.Marshal(role)
	if err != nil {
		return err
	}
	rolePath := path.Join(rolePrefix, name)

	_, err = m.kv.NewTxn().
		If(kv.Eq(kv.Value(rolePath), string(_roleJson))).
		Then(kv.OpSave(rolePath, string(roleJson))).
		Commit()
	if err != nil {
		return err
	}

	// Update user in memory cache.
	m.roles.Store(name, role)

	return nil
}

func (m *Manager) RemovePermission(name string, permission Permission) error {
	_role, err := m.GetRole(name)
	if err != nil {
		return err
	}

	if _, ok := _role.permissions[permission]; !ok {
		return ErrorRoleMissingPermission(name, permission)
	}

	_roleJson, err := json.Marshal(_role)
	if err != nil {
		return err
	}

	role := _role.Clone()
	delete(role.permissions, permission)

	// Try to update user in kv first.
	roleJson, err := json.Marshal(role)
	if err != nil {
		return err
	}
	rolePath := path.Join(rolePrefix, name)

	_, err = m.kv.NewTxn().
		If(kv.Eq(kv.Value(rolePath), string(_roleJson))).
		Then(kv.OpSave(rolePath, string(roleJson))).
		Commit()
	if err != nil {
		return err
	}

	// Update user in memory cache.
	m.roles.Store(name, role)

	return nil
}

func (m *Manager) InvalidateCache() {
	m.users.Range(func(key, _ interface{}) bool {
		m.users.Delete(key)
		return true
	})
	m.roles.Range(func(key, _ interface{}) bool {
		m.roles.Delete(key)
		return true
	})
}
