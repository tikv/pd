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
	"strings"

	"github.com/pingcap/errors"
)

var (
	ErrorInvalidAction = func(action Action) error { return errors.Errorf("invalid action: %s", action) }
)

type Action string

const (
	GET    Action = "get"
	LIST   Action = "list"
	CREATE Action = "create"
	UPDATE Action = "update"
	DELETE Action = "delete"
)

type Permission struct {
	resource string
	action   Action
}

type jsonPermission struct {
	Resource string `json:"resource"`
	Action   Action `json:"action"`
}

func (p *Permission) MarshalJSON() ([]byte, error) {
	_p := jsonPermission{Resource: p.resource, Action: p.action}
	return json.Marshal(_p)
}

func (p *Permission) UnmarshalJSON(bytes []byte) error {
	var _p jsonPermission

	err := json.Unmarshal(bytes, &_p)
	if err != nil {
		return err
	}
	if err = validateAction(_p.Action); err != nil {
		return err
	}

	p.resource = _p.Resource
	p.action = _p.Action
	return nil
}

func NewPermission(resource string, action Action) (*Permission, error) {
	err := validateAction(action)
	if err != nil {
		return nil, err
	}
	return &Permission{resource: resource, action: action}, nil
}

func (p *Permission) GetAction() Action {
	return p.action
}

func (p *Permission) GetResource() string {
	return p.resource
}

func (p *Permission) String() string {
	var builder strings.Builder
	builder.WriteString(string(p.action))
	builder.WriteString("(")
	builder.WriteString(p.resource)
	builder.WriteString(")")

	return builder.String()
}

func validateAction(action Action) error {
	switch action {
	case GET, LIST, CREATE, UPDATE, DELETE:
		return nil
	default:
		return ErrorInvalidAction(action)
	}
}
