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
	"encoding/json"
	"sort"
)

var (
	rolePrefix = "roles"
)

// Role records role info.
// Read-Only once created.
type Role struct {
	Name        string
	Permissions map[Permission]struct{}
}

// jsonRole is used as an intermediate model when marshaling/unmarshaling json data
// because we need to convert map[Permission]struct{} from/to []Permission first.
type jsonRole struct {
	Name        string       `json:"name"`
	Permissions []Permission `json:"permissions"`
}

// MarshalJSON implements Marshaler interface.
func (r *Role) MarshalJSON() ([]byte, error) {
	permissions := make([]Permission, 0, len(r.Permissions))
	for p := range r.Permissions {
		permissions = append(permissions, p)
	}
	sortPermissions(permissions)

	_r := jsonRole{Name: r.Name, Permissions: permissions}
	return json.Marshal(_r)
}

// UnmarshalJSON implements Unmarshaler interface.
func (r *Role) UnmarshalJSON(bytes []byte) error {
	var _r jsonRole

	err := json.Unmarshal(bytes, &_r)
	if err != nil {
		return err
	}

	r.Name = _r.Name
	for _, permission := range _r.Permissions {
		r.Permissions[permission] = struct{}{}
	}

	return nil
}

// NewRole safely creates a new role instance.
func NewRole(name string) (*Role, error) {
	err := validateName(name)
	if err != nil {
		return nil, err
	}

	return &Role{Name: name, Permissions: make(map[Permission]struct{})}, nil
}

// NewRoleFromJSON safely deserialize a json string to a role instance.
func NewRoleFromJSON(j string) (*Role, error) {
	role := Role{Permissions: make(map[Permission]struct{})}
	err := json.Unmarshal([]byte(j), &role)
	if err != nil {
		return nil, err
	}

	err = validateName(role.Name)
	if err != nil {
		return nil, err
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
func (r *Role) GetPermissions() map[Permission]struct{} {
	return r.Permissions
}

// HasPermission checks whether this user has a specific permission.
func (r *Role) HasPermission(permission Permission) bool {
	for p := range r.Permissions {
		if p == permission {
			return true
		}
	}

	return false
}

// sortPermissions is used to ensure that identical sets of permissions always yield the same json output.
func sortPermissions(permissions []Permission) {
	sort.Slice(permissions, func(i, j int) bool {
		if permissions[i].Resource != permissions[j].Resource {
			return permissions[i].Resource < permissions[j].Resource
		}
		return permissions[i].Action < permissions[j].Action
	})
}
