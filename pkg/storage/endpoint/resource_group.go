// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package endpoint

import (
	"path"

	"github.com/gogo/protobuf/proto"
)

// ResourceGroupStorage defines the storage operations on the rule.
type ResourceGroupStorage interface {
	LoadResourceGroups(prefix string, f func(k, v string)) error
	SaveResourceGroup(prefix string, msg proto.Message) error
	DeleteResourceGroup(prefix string) error
}

var _ ResourceGroupStorage = (*StorageEndpoint)(nil)

// SaveResourceGroup stores a resource group to storage.
func (se *StorageEndpoint) SaveResourceGroup(prefix string, msg proto.Message) error {
	path := path.Join(resourceGroupPath, prefix)
	return se.saveProto(path, msg)
}

// DeleteResourceGroup removes a resource group from storage.
func (se *StorageEndpoint) DeleteResourceGroup(prefix string) error {
	return se.Remove(resourceGroupKeyPath(prefix))
}

// LoadResourceGroups loads all resource groups from storage.
func (se *StorageEndpoint) LoadResourceGroups(prefix string, f func(k, v string)) error {
	path := path.Join(resourceGroupPath, prefix)
	return se.loadRangeByPrefix(path, f)
}
