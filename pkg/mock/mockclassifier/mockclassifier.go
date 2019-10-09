// Copyright 2019 PingCAP, Inc.
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

package mockclassifier

import "github.com/pingcap/pd/server/core"

// Classifier is used for test purpose.
type Classifier struct{}

// GetAllNamespaces mocks method.
func (c Classifier) GetAllNamespaces() []string {
	return []string{"global", "unknown"}
}

// GetStoreNamespace mocks method.
func (c Classifier) GetStoreNamespace(store *core.StoreInfo) string {
	if store.GetID() < 5 {
		return "global"
	}
	return "unknown"
}

// GetRegionNamespace mocks method.
func (c Classifier) GetRegionNamespace(*core.RegionInfo) string {
	return "global"
}

// IsNamespaceExist mocks method.
func (c Classifier) IsNamespaceExist(name string) bool {
	return true
}

// AllowMerge mocks method.
func (c Classifier) AllowMerge(*core.RegionInfo, *core.RegionInfo) bool {
	return true
}

// ReloadNamespaces mocks method.
func (c Classifier) ReloadNamespaces() error {
	return nil
}

// IsMetaExist mocks method.
func (c Classifier) IsMetaExist() bool {
	return false
}

// IsTableIDExist mocks method.
func (c Classifier) IsTableIDExist(tableID int64) bool {
	return false
}

// IsStoreIDExist mocks method.
func (c Classifier) IsStoreIDExist(storeID uint64) bool {
	return false
}
