// Copyright 2017 PingCAP, Inc.
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

package core

// Namespace defines two things:
// 1. relation between a name and several tables
// 2. relation between a name and several stores
// It is used to bind tables with stores
type Namespace struct {
	ID       uint64   `json:"id"`
	Name     string   `json:"name"`
	TableIDs []int64  `json:"table_ids,omitempty"`
	StoreIDs []uint64 `json:"store_ids,omitempty"`
}

// GetName returns namespace's name or default 'global' value
func (ns *Namespace) GetName() string {
	if ns != nil {
		return ns.Name
	}
	//TODO consider move outer definition in namespace/classfier.go here
	return "global"
}

// GetID returns namespace's ID or 0
func (ns *Namespace) GetID() uint64 {
	if ns != nil {
		return ns.ID
	}
	return 0
}
