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
	"encoding/json"
	"fmt"
	"math"
	"path"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
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
	if ns.TableIDs == nil {
		ns.TableIDs = make(map[int64]bool)
	}
	ns.TableIDs[tableID] = true
}

// AddStoreID adds a storeID to this namespace
func (ns *Namespace) AddStoreID(storeID uint64) {
	if ns.StoreIDs == nil {
		ns.StoreIDs = make(map[uint64]bool)
	}
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
	tableID := c.getTableID(regionInfo)
	if tableID == 0 {
		return namespace.DefaultNamespace
	}

	for name, ns := range c.nsInfo.namespaces {
		_, ok := ns.TableIDs[tableID]
		if ok {
			return name
		}
	}
	return namespace.DefaultNamespace
}

// getTableID returns the meaningful tableID (not 0) only if
// the region contains only the contents of that table
// else it returns 0
func (c tableNamespaceClassifier) getTableID(regionInfo *core.RegionInfo) int64 {
	startTableID := c.tableIDDecoder.DecodeTableID(regionInfo.StartKey)
	endTableID := c.tableIDDecoder.DecodeTableID(regionInfo.EndKey)
	if startTableID == 0 || endTableID == 0 {
		// The startTableID or endTableID cannot be decoded,
		// indicating the region contains meta-info or infinite edge
		return 0
	}

	if startTableID == endTableID {
		return startTableID
	}

	// The startTableID is not equal to the endTableID for regionInfo,
	// so check whether endKey is the startKey of the next table
	if (startTableID == endTableID-1) && core.IsPureTableID(regionInfo.EndKey) {
		return startTableID
	}

	return 0
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

func (namespaceInfo *namespacesInfo) namespacePath(nsID uint64) string {
	return path.Join("namespace", fmt.Sprintf("%20d", nsID))
}

func (namespaceInfo *namespacesInfo) saveNamespace(kv *core.KV, ns *Namespace) error {
	value, err := json.Marshal(ns)
	if err != nil {
		return errors.Trace(err)
	}
	err = kv.Save(namespaceInfo.namespacePath(ns.GetID()), string(value))
	return errors.Trace(err)
}

func (namespaceInfo *namespacesInfo) loadNamespaces(kv *core.KV, rangeLimit int) error {
	start := time.Now()

	nextID := uint64(0)
	endKey := namespaceInfo.namespacePath(math.MaxUint64)

	for {
		key := namespaceInfo.namespacePath(nextID)
		res, err := kv.LoadRange(key, endKey, rangeLimit)
		if err != nil {
			return errors.Trace(err)
		}
		for _, s := range res {
			ns := &Namespace{}
			if err := json.Unmarshal([]byte(s), ns); err != nil {
				return errors.Trace(err)
			}
			nextID = ns.GetID() + 1
			namespaceInfo.setNamespace(ns)
		}

		if len(res) < rangeLimit {
			log.Infof("load %v namespacesInfo cost %v", namespaceInfo.getNamespaceCount(), time.Since(start))
			return nil
		}
	}
}
