// Copyright 2026 TiKV Project Authors.
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

// ForceMergeStorage defines the storage operations on force merge ranges.
type ForceMergeStorage interface {
	LoadForceMergeRanges(f func(k, v string)) error
	SaveForceMergeRange(rangeKey string, data interface{}) error
	DeleteForceMergeRange(rangeKey string) error
}

var _ ForceMergeStorage = (*StorageEndpoint)(nil)

// LoadForceMergeRanges loads all force merge ranges from storage.
func (se *StorageEndpoint) LoadForceMergeRanges(f func(k, v string)) error {
	return se.loadRangeByPrefix(forceMergePath+"/", f)
}

// SaveForceMergeRange stores a force merge range config to storage.
func (se *StorageEndpoint) SaveForceMergeRange(rangeKey string, data interface{}) error {
	return se.saveJSON(forceMergePath, rangeKey, data)
}

// DeleteForceMergeRange removes a force merge range from storage.
func (se *StorageEndpoint) DeleteForceMergeRange(rangeKey string) error {
	return se.Remove(forceMergeRangePath(rangeKey))
}
