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

	"github.com/tikv/pd/pkg/errs"
)

const (
	rolePrefix = "roles"
)

// Role records role info.
// Read-Only once created.
type Role struct {
	Name        string       `json:"name"`
	Permissions []Permission `json:"permissions"`
}

// NewRole safely creates a new role instance.
func NewRole(name string) (*Role, error) {
	ok := validateName(name)
	if !ok {
		return nil, errs.ErrInvalidRoleName.FastGenByArgs(name)
	}

	return &Role{Name: name, Permissions: make([]Permission, 0)}, nil
}

// NewRoleFromJSON safely deserialize a json string to a role instance.
func NewRoleFromJSON(j string) (*Role, error) {
	role := Role{Permissions: make([]Permission, 0)}
	err := json.Unmarshal([]byte(j), &role)
	if err != nil {
		return nil, errs.ErrJSONUnmarshal.Wrap(err).GenWithStackByCause()
	}

	ok := validateName(role.Name)
	if !ok {
		return nil, errs.ErrInvalidRoleName.FastGenByArgs(role.Name)
	}

	return &role, nil
}

// Clone creates a deep copy of role instance.
func (r *Role) Clone() *Role {
	return &Role{Name: r.Name, Permissions: r.Permissions}
}

// GetName returns name of this role.
func (r *Role) GetName() string {
	return r.Name
}

// GetPermissions returns permissions of this role.
func (r *Role) GetPermissions() []Permission {
	return r.Permissions
}

// hasPermission checks whether this user has a specific permission.
func (r *Role) hasPermission(permission Permission) bool {
	for _, p := range r.Permissions {
		if p == permission {
			return true
		}
	}
	return false
}

// appendPermission appends a permission to this role.
func (r *Role) appendPermission(permission Permission) bool {
	if ok := r.hasPermission(permission); ok {
		return false
	}

	r.Permissions = append(r.Permissions, permission)
	return true
}

// removePermission deletes a permission from this role.
func (r *Role) removePermission(permission Permission) bool {
	for i, perm := range r.Permissions {
		if perm == permission {
			r.Permissions[i] = r.Permissions[len(r.Permissions)-1]
			r.Permissions = r.Permissions[:len(r.Permissions)-1]
			return true
		}
	}
	return false
}
