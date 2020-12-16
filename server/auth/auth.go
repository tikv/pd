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
	"sync"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/kv"
	"go.etcd.io/etcd/clientv3"
)

// RBACManager is used for the rbac storage, cache, management and enforcing logic.
type RBACManager struct {
	userManager
	roleManager
}

// NewRBACManager creates a new RBACManager.
func NewRBACManager(kv kv.Base) *RBACManager {
	return &RBACManager{
		userManager{
			kv:    kv,
			users: make(map[string]*User),
		},
		roleManager{
			kv:    kv,
			roles: make(map[string]*Role),
		}}
}

type userManager struct {
	kv    kv.Base
	mu    sync.RWMutex
	users map[string]*User
}

// newUserManager creates a new roleManager.
func newUserManager(kv kv.Base) *userManager {
	return &userManager{kv: kv, users: make(map[string]*User)}
}

// GetUser returns a user.
func (m *userManager) GetUser(name string) (*User, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	user, ok := m.users[name]
	if !ok {
		return nil, errs.ErrUserNotFound.FastGenByArgs(name)
	}

	return user, nil
}

// GetUsers returns all available roles.
func (m *userManager) GetUsers() map[string]*User {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.users
}

// CreateUser creates a new user.
func (m *userManager) CreateUser(name string, password string) error {
	_, err := m.GetUser(name)
	if err == nil {
		return errs.ErrUserExists.GenWithStackByArgs(name)
	}

	user, err := NewUser(name, GenerateHash(password))
	if err != nil {
		return err
	}

	userJSON, err := json.Marshal(user)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByCause()
	}
	userPath := GetUserPath(name)

	m.mu.Lock()
	defer m.mu.Unlock()

	// Add user to kv.
	err = m.kv.Save(userPath, string(userJSON))
	if err != nil {
		return err
	}

	// Add user to memory cache.
	m.users[name] = user

	return nil
}

// DeleteUser deletes a user.
func (m *userManager) DeleteUser(name string) error {
	_, err := m.GetUser(name)
	if err != nil {
		return err
	}

	userPath := GetUserPath(name)

	m.mu.Lock()
	defer m.mu.Unlock()

	// Delete user from kv.
	err = m.kv.Remove(userPath)
	if err != nil {
		return err
	}

	// Delete user from memory cache.
	delete(m.users, name)

	return nil
}

// ChangePassword changes password of a user.
func (m *userManager) ChangePassword(name string, password string) error {
	user, err := m.GetUser(name)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	updatedUser := user.Clone()
	hash := GenerateHash(password)
	updatedUser.Hash = hash

	// Update user in kv
	userJSON, err := json.Marshal(updatedUser)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByCause()
	}
	userPath := GetUserPath(name)

	err = m.kv.Save(userPath, string(userJSON))
	if err != nil {
		return err
	}

	// Update user in memory cache.
	user.Hash = hash

	return nil
}

// SetRoles sets roles of a user.
func (m *userManager) SetRoles(name string, roles []string) error {
	user, err := m.GetUser(name)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	updatedUser := user.Clone()
	updatedUser.RoleKeys = roles

	// Update user in kv
	userJSON, err := json.Marshal(updatedUser)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByCause()
	}
	userPath := GetUserPath(name)

	err = m.kv.Save(userPath, string(userJSON))
	if err != nil {
		return err
	}

	// Update user in memory cache.
	user.RoleKeys = roles

	return nil
}

// AddRole adds a role to a user.
func (m *userManager) AddRole(name string, role string) error {
	user, err := m.GetUser(name)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if ok := user.appendRole(role); !ok {
		return errs.ErrUserHasRole.FastGenByArgs(name, role)
	}

	userJSON, err := json.Marshal(role)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByCause()
	}
	userPath := GetUserPath(name)

	// Update user in kv.
	err = m.kv.Save(userPath, string(userJSON))
	if err != nil {
		return err
	}

	// Update user in memory cache.
	m.users[name] = user

	return nil
}

// RemoveRole removes a role from a user.
func (m *userManager) RemoveRole(name string, role string) error {
	user, err := m.GetUser(name)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if ok := user.removeRole(role); !ok {
		return errs.ErrUserMissingRole.FastGenByArgs(name, role)
	}

	userJSON, err := json.Marshal(role)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByCause()
	}
	userPath := GetUserPath(name)

	// Update user in kv.
	err = m.kv.Save(userPath, string(userJSON))
	if err != nil {
		return err
	}

	// Update user in memory cache.
	m.users[name] = user

	return nil
}

// UpdateCache refreshes in-memory cache of users.
func (m *userManager) UpdateCache() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	userPath := GetUserPath("")

	keys, values, err := m.kv.LoadRange(userPath, clientv3.GetPrefixRangeEnd(userPath), 0)
	if err != nil {
		return err
	}

	m.users = make(map[string]*User)
	for i := range keys {
		value := values[i]

		user, err := UnmarshalUser(value)
		if err != nil {
			return err
		}
		m.users[user.Username] = user
	}
	return nil
}

type roleManager struct {
	kv    kv.Base
	mu    sync.RWMutex
	roles map[string]*Role
}

// newRoleManager creates a new roleManager.
func newRoleManager(kv kv.Base) *roleManager {
	return &roleManager{kv: kv, roles: make(map[string]*Role)}
}

// GetRole returns a role.
func (m *roleManager) GetRole(name string) (*Role, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	role, ok := m.roles[name]
	if !ok {
		return nil, errs.ErrRoleNotFound.FastGenByArgs(name)
	}

	return role, nil
}

// GetRoles returns all available roles.
func (m *roleManager) GetRoles() map[string]*Role {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.roles
}

// CreateRole creates a new role.
func (m *roleManager) CreateRole(name string) error {
	_, err := m.GetRole(name)
	if err == nil {
		return errs.ErrRoleExists.GenWithStackByArgs(name)
	}

	role, err := NewRole(name)
	if err != nil {
		return err
	}

	roleJSON, err := json.Marshal(role)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByCause()
	}
	rolePath := GetRolePath(name)

	m.mu.Lock()
	defer m.mu.Unlock()

	// Add role to kv.
	err = m.kv.Save(rolePath, string(roleJSON))
	if err != nil {
		return err
	}

	// Add role to memory cache.
	m.roles[name] = role

	return nil
}

// DeleteRole deletes a role.
func (m *roleManager) DeleteRole(name string) error {
	_, err := m.GetRole(name)
	if err != nil {
		return err
	}

	rolePath := GetRolePath(name)

	m.mu.Lock()
	defer m.mu.Unlock()

	// Delete role from kv.
	err = m.kv.Remove(rolePath)
	if err != nil {
		return err
	}

	// Delete role from memory cache.
	delete(m.roles, name)

	return nil
}

// RoleHasPermission checks whether a role has a specific permission.
func (m *roleManager) RoleHasPermission(name string, permission Permission) (bool, error) {
	role, err := m.GetRole(name)
	if err != nil {
		return false, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	return role.hasPermission(permission), nil
}

// SetPermissions sets permissions of a role.
func (m *roleManager) SetPermissions(name string, permissions []Permission) error {
	role, err := m.GetRole(name)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	updatedRole := role.Clone()
	updatedRole.Permissions = permissions

	// Update role in kv
	roleJSON, err := json.Marshal(updatedRole)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByCause()
	}
	rolePath := GetRolePath(name)

	err = m.kv.Save(rolePath, string(roleJSON))
	if err != nil {
		return err
	}

	// Update role in memory cache.
	role.Permissions = permissions

	return nil
}

// AddPermission adds a permission to a role.
func (m *roleManager) AddPermission(name string, permission Permission) error {
	role, err := m.GetRole(name)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if ok := role.appendPermission(permission); !ok {
		return errs.ErrRoleHasPermission.FastGenByArgs(name, permission)
	}

	roleJSON, err := json.Marshal(role)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByCause()
	}
	rolePath := GetRolePath(name)

	// Update user in kv.
	err = m.kv.Save(rolePath, string(roleJSON))
	if err != nil {
		return err
	}

	// Update role in memory cache.
	m.roles[name] = role

	return nil
}

// RemovePermission removes a permission from a role.
func (m *roleManager) RemovePermission(name string, permission Permission) error {
	role, err := m.GetRole(name)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if ok := role.removePermission(permission); !ok {
		return errs.ErrRoleMissingPermission.FastGenByArgs(name, permission)
	}

	roleJSON, err := json.Marshal(role)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByCause()
	}
	rolePath := GetRolePath(name)

	// Update user in kv.
	err = m.kv.Save(rolePath, string(roleJSON))
	if err != nil {
		return err
	}

	// Update role in memory cache.
	m.roles[name] = role

	return nil
}

// UpdateCache refreshes in-memory cache of roles.
func (m *roleManager) UpdateCache() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rolePath := GetRolePath("")

	keys, values, err := m.kv.LoadRange(rolePath, clientv3.GetPrefixRangeEnd(rolePath), 0)
	if err != nil {
		return err
	}

	m.roles = make(map[string]*Role)
	for i := range keys {
		value := values[i]
		role, err := UnmarshalRole(value)
		if err != nil {
			return err
		}
		m.roles[role.Name] = role
	}
	return nil
}
