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
	"sort"
)

// Role records role info.
// Read-Only once created.
type Role struct {
	name        string
	permissions map[Permission]struct{}
}

type jsonRole struct {
	Name        string       `json:"name"`
	Permissions []Permission `json:"permissions"`
}

// MarshalJSON implements Marshaler interface.
func (r *Role) MarshalJSON() ([]byte, error) {
	permissions := make([]Permission, 0, len(r.permissions))
	for p := range r.permissions {
		permissions = append(permissions, p)
	}
	sortPermissions(permissions)

	_r := jsonRole{Name: r.name, Permissions: permissions}
	return json.Marshal(_r)
}

// UnmarshalJSON implements Unmarshaler interface.
func (r *Role) UnmarshalJSON(bytes []byte) error {
	var _r jsonRole

	err := json.Unmarshal(bytes, &_r)
	if err != nil {
		return err
	}

	r.name = _r.Name
	for _, permission := range _r.Permissions {
		r.permissions[permission] = struct{}{}
	}

	return nil
}

// NewRole safely creates a new role instance.
func NewRole(name string) (*Role, error) {
	err := validateName(name)
	if err != nil {
		return nil, err
	}

	return &Role{name: name, permissions: make(map[Permission]struct{})}, nil
}

// NewRoleFromJSON safely deserialize a json string to a role instance.
func NewRoleFromJSON(j string) (*Role, error) {
	role := Role{permissions: make(map[Permission]struct{})}
	err := json.Unmarshal([]byte(j), &role)
	if err != nil {
		return nil, err
	}

	err = validateName(role.name)
	if err != nil {
		return nil, err
	}

	return &role, nil
}

// Clone creates a deep copy of role instance.
func (r *Role) Clone() *Role {
	return &Role{name: r.name, permissions: r.permissions}
}

// GetName returns name of this role.
func (r *Role) GetName() string {
	return r.name
}

// GetPermissions returns permissions of this role.
func (r *Role) GetPermissions() map[Permission]struct{} {
	return r.permissions
}

// HasPermission checks whether this user has a specific permission.
func (r *Role) HasPermission(permission Permission) bool {
	for p := range r.permissions {
		if p == permission {
			return true
		}
	}

	return false
}

// sortPermissions is used to ensure that identical sets of permissions always yield the same json output,
// so that we may reliably check them when committing kv transactions.
func sortPermissions(permissions []Permission) {
	sort.Slice(permissions, func(i, j int) bool {
		if permissions[i].resource != permissions[j].resource {
			return permissions[i].resource < permissions[j].resource
		}
		return permissions[i].action < permissions[j].action
	})
}
