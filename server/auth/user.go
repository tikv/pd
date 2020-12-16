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

// User records user info.
// Read-Only once created.
type User struct {
	Username string   `json:"username"`
	Hash     string   `json:"hash"` // sha256 hash for password
	RoleKeys []string `json:"roles"`
}

// NewUser safely creates a new user instance.
func NewUser(username string, hash string) (*User, error) {
	ok := validateName(username)
	if !ok {
		return nil, errs.ErrInvalidUserName.FastGenByArgs(username)
	}

	return &User{Username: username, Hash: hash, RoleKeys: make([]string, 0)}, nil
}

// UnmarshalUser safely deserialize a json string to a user instance.
func UnmarshalUser(j string) (*User, error) {
	user := User{RoleKeys: make([]string, 0)}
	err := json.Unmarshal([]byte(j), &user)
	if err != nil {
		return nil, errs.ErrJSONUnmarshal.Wrap(err).GenWithStackByCause()
	}

	ok := validateName(user.Username)
	if !ok {
		return nil, errs.ErrInvalidUserName.FastGenByArgs(user.Username)
	}

	return &user, nil
}

// Clone creates a deep copy of user instance.
func (u *User) Clone() *User {
	return &User{Username: u.Username, Hash: u.Hash, RoleKeys: u.RoleKeys}
}

// GetUsername returns username of this user.
func (u *User) GetUsername() string {
	return u.Username
}

// GetRoleKeys returns role keys of this user.
func (u *User) GetRoleKeys() []string {
	return u.RoleKeys
}

// ComparePassword checks whether given string matches the password of this user.
func (u *User) ComparePassword(candidate string) error {
	return compareHashAndPassword(u.Hash, candidate)
}

// hasRole checks whether this user has a specific role.
func (u *User) hasRole(name string) bool {
	for _, k := range u.RoleKeys {
		if k == name {
			return true
		}
	}

	return false
}

// appendPermission appends a permission to this role.
func (u *User) appendRole(role string) bool {
	if ok := u.hasRole(role); ok {
		return false
	}

	u.RoleKeys = append(u.RoleKeys, role)
	return true
}

// removePermission deletes a permission from this role.
func (u *User) removeRole(role string) bool {
	for i, perm := range u.RoleKeys {
		if perm == role {
			u.RoleKeys[i] = u.RoleKeys[len(u.RoleKeys)-1]
			u.RoleKeys = u.RoleKeys[:len(u.RoleKeys)-1]
			return true
		}
	}
	return false
}
