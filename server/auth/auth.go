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
	// ErrUserNotFound is error info for user not found.
	ErrUserNotFound = func(name string) error { return errors.Errorf("user not found: %s", name) }
	// ErrUserExists is error info for user exists.
	ErrUserExists = func(name string) error { return errors.Errorf("user already exists: %s", name) }
	// ErrRoleNotFound is error info for role not found.
	ErrRoleNotFound = func(name string) error { return errors.Errorf("role not found: %s", name) }
	// ErrRoleExists is error info for role exists.
	ErrRoleExists = func(name string) error { return errors.Errorf("role already exists: %s", name) }
	// ErrUserHasRole is error info for user has role.
	ErrUserHasRole = func(user string, role string) error { return errors.Errorf("user %s already has role: %s", user, role) }
	// ErrUserMissingRole is error info for user lacks a required role.
	ErrUserMissingRole = func(user string, role string) error {
		return errors.Errorf("user %s doesn't have role: %s", user, role)
	}
	// ErrRoleHasPermission is error info for role has permission.
	ErrRoleHasPermission = func(role string, permission Permission) error {
		return errors.Errorf("role %s already has permission: %s", role, permission.String())
	}
	// ErrRoleMissingPermission is error info for role lacks a required role.
	ErrRoleMissingPermission = func(role string, permission Permission) error {
		return errors.Errorf("role %s doesn't have permission: %s", role, permission.String())
	}
)

var (
	userPrefix = "users"
	rolePrefix = "roles"
)

// Manager is used for the rbac storage, cache, management and enforcing logic.
type Manager struct {
	kv    kv.TxnBase
	users sync.Map // map[string]*User
	roles sync.Map // map[string]*Role
}

// NewManager creates a new Manager.
func NewManager(k kv.TxnBase) *Manager {
	return &Manager{kv: k}
}

// GetUser returns a user.
func (m *Manager) GetUser(name string) (*User, error) {
	_cachedUser, ok := m.users.Load(name)
	// Cache hit, so we return cached value.
	if ok {
		cachedUser := _cachedUser.(*User)
		// Nil in cache implies this user doesn't exist
		if cachedUser == nil {
			return nil, ErrUserNotFound(name)
		}
		return cachedUser, nil
	}

	// Cache miss. Load value from etcd.
	userJSON, err := m.kv.Load(path.Join(userPrefix, name))
	if err != nil {
		return nil, err
	}

	var user *User
	if userJSON == "" {
		user = nil
	} else {
		user, err = NewUserFromJSON(userJSON)
		if err != nil {
			return nil, err
		}
	}

	// If another (write) operation has already updated the cache,
	// we abandon the data we've just retrieved and use the cached version.
	_latestUser, _ := m.users.LoadOrStore(name, user)
	latestUser := _latestUser.(*User)

	if latestUser == nil {
		return nil, ErrUserNotFound(name)
	}
	return latestUser, nil
}

// GetUsers returns all available users.
func (m *Manager) GetUsers() (map[*User]struct{}, error) {
	// NOTE: GetUsers doesn't use the cache and read data from kv directly to avoid cache inconsistency.
	usersPath := strings.Join([]string{userPrefix, ""}, "/")
	_, _users, err := m.kv.LoadRange(usersPath, clientv3.GetPrefixRangeEnd(usersPath), 0)
	if err != nil {
		return nil, err
	}

	users := make(map[*User]struct{})
	for _, userJSON := range _users {
		user, err := NewUserFromJSON(userJSON)
		if err != nil {
			return nil, err
		}

		users[user] = struct{}{}
	}

	return users, nil
}

// GetUserKeys returns names of all available users.
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

// CreateUser creates a new user.
func (m *Manager) CreateUser(name string, password string) error {
	_, err := m.GetUser(name)
	if err == nil {
		return ErrUserExists(name)
	}
	if err.Error() != ErrUserNotFound(name).Error() {
		return err
	}

	user, err := NewUser(name, GenerateHash(password))
	if err != nil {
		return err
	}

	// Try to add user to kv first.
	userJSON, err := json.Marshal(user)
	if err != nil {
		return err
	}
	userPath := path.Join(userPrefix, name)

	_, err = m.kv.NewTxn().
		If(kv.Eq(kv.Create(userPath), 0)).
		Then(kv.OpSave(name, string(userJSON))).
		Commit()
	if err != nil {
		return err
	}

	// Add user to memory cache.
	m.users.Store(name, user)

	return nil
}

// DeleteUser deletes a user.
func (m *Manager) DeleteUser(name string) error {
	user, err := m.GetUser(name)
	if err != nil {
		return err
	}

	// Try to delete user from kv first.
	userJSON, err := json.Marshal(user)
	if err != nil {
		return err
	}
	userPath := path.Join(userPrefix, name)

	_, err = m.kv.NewTxn().
		If(kv.Eq(kv.Value(userPath), string(userJSON))).
		Then(kv.OpRemove(userPath)).
		Commit()
	if err != nil {
		return err
	}

	// Delete user from memory cache.
	m.users.Store(name, (*User)(nil))

	return nil
}

// ChangePassword changes password of a user.
func (m *Manager) ChangePassword(name string, password string) error {
	_user, err := m.GetUser(name)
	if err != nil {
		return err
	}

	_userJSON, err := json.Marshal(_user)
	if err != nil {
		return err
	}

	user := _user.Clone()
	user.hash = GenerateHash(password)

	// Try to update user in kv first.
	userJSON, err := json.Marshal(user)
	if err != nil {
		return err
	}
	userPath := path.Join(userPrefix, name)

	_, err = m.kv.NewTxn().
		If(kv.Eq(kv.Value(userPath), string(_userJSON))).
		Then(kv.OpSave(userPath, string(userJSON))).
		Commit()
	if err != nil {
		return err
	}

	// Update user in memory cache.
	m.users.Store(name, user)

	return nil
}

// SetRole sets roles of a user.
func (m *Manager) SetRole(name string, roles map[string]struct{}) error {
	_user, err := m.GetUser(name)
	if err != nil {
		return err
	}

	_userJSON, err := json.Marshal(_user)
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
	userJSON, err := json.Marshal(user)
	if err != nil {
		return err
	}
	userPath := path.Join(userPrefix, name)

	cmps = append(cmps, kv.Eq(kv.Value(userPath), string(_userJSON)))
	_, err = m.kv.NewTxn().
		If(cmps...).
		Then(kv.OpSave(userPath, string(userJSON))).
		Commit()
	if err != nil {
		return err
	}

	// Update user in memory cache.
	m.users.Store(name, user)

	return nil
}

// AddRole adds a role to a user.
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
		return ErrUserHasRole(name, roleName)
	}
	_, err = m.GetRole(roleName)
	if err != nil {
		return ErrRoleNotFound(roleName)
	}

	_userJSON, err := json.Marshal(_user)
	if err != nil {
		return err
	}

	user := _user.Clone()
	user.roleKeys[roleName] = struct{}{}

	// Try to update user in kv first.
	userJSON, err := json.Marshal(user)
	if err != nil {
		return err
	}
	userPath := path.Join(userPrefix, name)
	rolePath := path.Join(rolePrefix, roleName)

	_, err = m.kv.NewTxn().
		If(kv.Eq(kv.Value(userPath), string(_userJSON)),
			kv.Gt(kv.Create(rolePath), 0)).
		Then(kv.OpSave(userPath, string(userJSON))).
		Commit()
	if err != nil {
		return err
	}

	// Update user in memory cache.
	m.users.Store(name, user)

	return nil
}

// RemoveRole removes a role from a user.
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
		return ErrUserMissingRole(name, roleName)
	}

	_userJSON, err := json.Marshal(_user)
	if err != nil {
		return err
	}

	user := _user.Clone()
	delete(user.roleKeys, roleName)

	// Try to update user in kv first.
	userJSON, err := json.Marshal(user)
	if err != nil {
		return err
	}
	userPath := path.Join(userPrefix, name)

	_, err = m.kv.NewTxn().
		If(kv.Eq(kv.Value(userPath), string(_userJSON))).
		Then(kv.OpSave(userPath, string(userJSON))).
		Commit()
	if err != nil {
		return err
	}

	// Update user in memory cache.
	m.users.Store(name, user)

	return nil
}

// HasPermissions checks whether a user has each of a given set of permissions.
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
			if err.Error() == ErrRoleNotFound(roleName).Error() {
				log.Warn("missing role when validating user permission. skipped.", zap.String("user", name), errs.ZapError(err))
				continue
			}
			return false, err
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

// GetRole returns a role.
func (m *Manager) GetRole(name string) (*Role, error) {
	_cachedRole, ok := m.roles.Load(name)
	// Cache hit, so we return cached value.
	if ok {
		cachedRole := _cachedRole.(*Role)
		// Nil in cache implies this user doesn't exist
		if cachedRole == nil {
			return nil, ErrRoleNotFound(name)
		}
		return cachedRole, nil
	}

	// Cache miss. Load value from etcd.
	roleJSON, err := m.kv.Load(path.Join(rolePrefix, name))
	if err != nil {
		return nil, err
	}

	var role *Role
	if roleJSON == "" {
		role = nil
	} else {
		role, err = NewRoleFromJSON(roleJSON)
		if err != nil {
			return nil, err
		}
	}

	// If another (write) operation has already updated the cache,
	// we abandon the data we've just retrieved and use the cached version.
	_latestRole, _ := m.roles.LoadOrStore(name, role)
	latestRole := _latestRole.(*Role)

	if latestRole == nil {
		return nil, ErrRoleNotFound(name)
	}
	return latestRole, nil
}

// GetRoles returns all available roles.
func (m *Manager) GetRoles() (map[*Role]struct{}, error) {
	// NOTE: GetRoles doesn't use the cache and read data from kv directly to avoid cache inconsistency.
	rolesPath := strings.Join([]string{rolePrefix, ""}, "/")
	_, _roles, err := m.kv.LoadRange(rolesPath, clientv3.GetPrefixRangeEnd(rolesPath), 0)
	if err != nil {
		return nil, err
	}

	roles := make(map[*Role]struct{})
	for _, roleJSON := range _roles {
		role, err := NewRoleFromJSON(roleJSON)
		if err != nil {
			return nil, err
		}

		roles[role] = struct{}{}
	}

	return roles, nil
}

// GetRoleKeys returns names of all available roles.
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

// CreateRole creates a new role.
func (m *Manager) CreateRole(name string) error {
	_, err := m.GetRole(name)
	if err == nil {
		return ErrRoleExists(name)
	}
	if err.Error() != ErrRoleNotFound(name).Error() {
		return err
	}

	role, err := NewRole(name)
	if err != nil {
		return err
	}

	// Try to add role to kv first.
	roleJSON, err := json.Marshal(role)
	if err != nil {
		return err
	}
	rolePath := path.Join(rolePrefix, name)

	_, err = m.kv.NewTxn().
		If(kv.Eq(kv.Create(rolePath), 0)).
		Then(kv.OpSave(name, string(roleJSON))).
		Commit()
	if err != nil {
		return err
	}

	// Add role to memory cache.
	m.roles.Store(name, role)

	return nil
}

// DeleteRole deletes a role.
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
			userJSON, err := user.MarshalJSON()
			if err != nil {
				return err
			}
			newUserJSON, err := newUser.MarshalJSON()
			if err != nil {
				return err
			}

			editedUsers = append(editedUsers, name)
			userPath := path.Join(userPrefix, user.username)
			cmps = append(cmps, kv.Eq(kv.Value(userPath), string(userJSON)))
			ops = append(ops, kv.OpSave(userPath, string(newUserJSON)))
		}
	}

	// Then delete the role itself
	roleJSON, err := json.Marshal(role)
	if err != nil {
		return err
	}
	rolePath := path.Join(rolePrefix, name)

	cmps = append(cmps, kv.Eq(kv.Value(rolePath), string(roleJSON)))
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

// SetPermissions sets permissions of a role.
func (m *Manager) SetPermissions(name string, permissions map[Permission]struct{}) error {
	_role, err := m.GetRole(name)
	if err != nil {
		return err
	}

	_roleJSON, err := json.Marshal(_role)
	if err != nil {
		return err
	}

	role := _role.Clone()
	role.permissions = permissions

	// Try to update user in kv first.
	roleJSON, err := json.Marshal(role)
	if err != nil {
		return err
	}
	rolePath := path.Join(rolePrefix, name)

	_, err = m.kv.NewTxn().
		If(kv.Eq(kv.Value(rolePath), string(_roleJSON))).
		Then(kv.OpSave(rolePath, string(roleJSON))).
		Commit()
	if err != nil {
		return err
	}

	// Update role in memory cache.
	m.roles.Store(name, role)

	return nil
}

// AddPermission adds a permission to a role.
func (m *Manager) AddPermission(name string, permission Permission) error {
	_role, err := m.GetRole(name)
	if err != nil {
		return err
	}

	if _, ok := _role.permissions[permission]; ok {
		return ErrRoleHasPermission(name, permission)
	}

	_roleJSON, err := json.Marshal(_role)
	if err != nil {
		return err
	}

	role := _role.Clone()
	role.permissions[permission] = struct{}{}

	// Try to update user in kv first.
	roleJSON, err := json.Marshal(role)
	if err != nil {
		return err
	}
	rolePath := path.Join(rolePrefix, name)

	_, err = m.kv.NewTxn().
		If(kv.Eq(kv.Value(rolePath), string(_roleJSON))).
		Then(kv.OpSave(rolePath, string(roleJSON))).
		Commit()
	if err != nil {
		return err
	}

	// Update user in memory cache.
	m.roles.Store(name, role)

	return nil
}

// RemovePermission removes a permission from a role.
func (m *Manager) RemovePermission(name string, permission Permission) error {
	_role, err := m.GetRole(name)
	if err != nil {
		return err
	}

	if _, ok := _role.permissions[permission]; !ok {
		return ErrRoleMissingPermission(name, permission)
	}

	_roleJSON, err := json.Marshal(_role)
	if err != nil {
		return err
	}

	role := _role.Clone()
	delete(role.permissions, permission)

	// Try to update user in kv first.
	roleJSON, err := json.Marshal(role)
	if err != nil {
		return err
	}
	rolePath := path.Join(rolePrefix, name)

	_, err = m.kv.NewTxn().
		If(kv.Eq(kv.Value(rolePath), string(_roleJSON))).
		Then(kv.OpSave(rolePath, string(roleJSON))).
		Commit()
	if err != nil {
		return err
	}

	// Update user in memory cache.
	m.roles.Store(name, role)

	return nil
}

// InvalidateCache purges memory cache of users and roles.
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
