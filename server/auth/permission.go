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
	"strings"

	"github.com/pingcap/errors"
)

var (
	// ErrInvalidAction is error info for invalid action.
	ErrInvalidAction = func(action Action) error { return errors.Errorf("invalid action: %s", action) }
)

// Action represents rbac actions.
type Action string

// All available actions types.
const (
	GET    Action = "get"
	LIST   Action = "list"
	CREATE Action = "create"
	UPDATE Action = "update"
	DELETE Action = "delete"
)

// Permission represents a permission to a specific pair of resource and action.
type Permission struct {
	Resource string `json:"resource"`
	Action   Action `json:"action"`
}

// NewPermission safely creates a new permission instance.
func NewPermission(resource string, action Action) (*Permission, error) {
	err := validateAction(action)
	if err != nil {
		return nil, err
	}
	return &Permission{Resource: resource, Action: action}, nil
}

// String implements Stringer interface.
func (p *Permission) String() string {
	var builder strings.Builder
	builder.WriteString(string(p.Action))
	builder.WriteString("(")
	builder.WriteString(p.Resource)
	builder.WriteString(")")

	return builder.String()
}

func validateAction(action Action) error {
	switch action {
	case GET, LIST, CREATE, UPDATE, DELETE:
		return nil
	default:
		return ErrInvalidAction(action)
	}
}
