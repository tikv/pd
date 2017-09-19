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
	return namespace.DefaultNamespace
}

// GetID returns namespace's ID or 0
func (ns *Namespace) GetID() uint64 {
	if ns != nil {
		return ns.ID
	}
	return 0
}

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
		for _, storeID := range ns.StoreIDs {
			if storeID == storeInfo.Id {
				return name
			}
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
		for _, tableID := range ns.TableIDs {
			if tableID == startTable && tableID == endTable {
				return name
			}
		}
	}
	return namespace.DefaultNamespace
}
