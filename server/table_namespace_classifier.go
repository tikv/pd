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

package server

import (
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
)

// Namespace defines two things:
// 1. relation between a Name and several tables
// 2. relation between a Name and several stores
// It is used to bind tables with stores
type Namespace struct {
	ID       uint64          `json:"ID"`
	Name     string          `json:"Name"`
	TableIDs map[int64]bool  `json:"table_ids,omitempty"`
	StoreIDs map[uint64]bool `json:"store_ids,omitempty"`
}

// NewNamespace creates a new namespace
func NewNamespace(id uint64, name string) *Namespace {
	return &Namespace{
		ID:       id,
		Name:     name,
		TableIDs: make(map[int64]bool),
		StoreIDs: make(map[uint64]bool),
	}
}

// GetName returns namespace's Name or default 'global' value
func (ns *Namespace) GetName() string {
	if ns != nil {
		return ns.Name
	}
	return namespace.DefaultNamespace
}

// GetID returns namespace's ID or 0
func (ns *Namespace) GetID() uint64 {
	if ns != nil {
		return ns.ID
	}
	return 0
}

// AddTableID adds a tableID to this namespace
func (ns *Namespace) AddTableID(tableID int64) {
	ns.TableIDs[tableID] = true
}

// AddStoreID adds a storeID to this namespace
func (ns *Namespace) AddStoreID(storeID uint64) {
	ns.StoreIDs[storeID] = true
}

// tableNamespaceClassifier implements Classifier interface
type tableNamespaceClassifier struct {
	nsInfo         *namespacesInfo
	tableIDDecoder core.TableIDDecoder
}

func newTableNamespaceClassifier(nsInfo *namespacesInfo, tableIDDecoder core.TableIDDecoder) tableNamespaceClassifier {
	return tableNamespaceClassifier{
		nsInfo,
		tableIDDecoder,
	}
}

func (c tableNamespaceClassifier) GetAllNamespaces() []string {
	nsList := make([]string, 0, len(c.nsInfo.namespaces))
	for name := range c.nsInfo.namespaces {
		nsList = append(nsList, name)
	}
	return nsList
}

func (c tableNamespaceClassifier) GetStoreNamespace(storeInfo *core.StoreInfo) string {
	for name, ns := range c.nsInfo.namespaces {
		_, ok := ns.StoreIDs[storeInfo.Id]
		if ok {
			return name
		}
	}
	return namespace.DefaultNamespace
}

func (c tableNamespaceClassifier) GetRegionNamespace(regionInfo *core.RegionInfo) string {
	startTable := c.tableIDDecoder.DecodeTableID(regionInfo.StartKey)
	endTable := c.tableIDDecoder.DecodeTableID(regionInfo.EndKey)
	if startTable != endTable {
		return namespace.DefaultNamespace
	}

	for name, ns := range c.nsInfo.namespaces {
		for tableID := range ns.TableIDs {
			if tableID == startTable && tableID == endTable {
				return name
			}
		}
	}
	return namespace.DefaultNamespace
}

type namespacesInfo struct {
	namespaces map[string]*Namespace
}

func newNamespacesInfo() *namespacesInfo {
	return &namespacesInfo{
		namespaces: make(map[string]*Namespace),
	}
}

func (namespaceInfo *namespacesInfo) getNamespaceByName(name string) *Namespace {
	namespace, ok := namespaceInfo.namespaces[name]
	if !ok {
		return nil
	}
	return namespace
}

func (namespaceInfo *namespacesInfo) setNamespace(item *Namespace) {
	namespaceInfo.namespaces[item.Name] = item
}

func (namespaceInfo *namespacesInfo) getNamespaceCount() int {
	return len(namespaceInfo.namespaces)
}

func (namespaceInfo *namespacesInfo) getNamespaces() []*Namespace {
	nsList := make([]*Namespace, 0, len(namespaceInfo.namespaces))
	for _, item := range namespaceInfo.namespaces {
		nsList = append(nsList, item)
	}
	return nsList
}

// IsTableIDExist returns true if table ID exists in namespacesInfo
func (namespaceInfo *namespacesInfo) IsTableIDExist(tableID int64) bool {
	for _, ns := range namespaceInfo.namespaces {
		_, ok := ns.TableIDs[tableID]
		if ok {
			return true
		}
	}
	return false
}

// IsStoreIDExist returns true if store ID exists in namespacesInfo
func (namespaceInfo *namespacesInfo) IsStoreIDExist(storeID uint64) bool {
	for _, ns := range namespaceInfo.namespaces {
		_, ok := ns.StoreIDs[storeID]
		if ok {
			return true
		}
	}
	return false
}
