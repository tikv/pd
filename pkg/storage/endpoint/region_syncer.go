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

import (
	"strconv"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// RegionSyncerStorage defines the storage operations on the region syncer's
// cluster-level committed state.
type RegionSyncerStorage interface {
	LoadRegionSyncerCommittedRegionCount() (uint64, error)
	SaveRegionSyncerCommittedRegionCount(count uint64) error
}

var _ RegionSyncerStorage = (*StorageEndpoint)(nil)

// LoadRegionSyncerCommittedRegionCount loads the region count the current
// leader last published. It returns (0, nil) when the key is absent (a fresh
// cluster or one upgraded from a version that never wrote it), which callers
// treat as "no committed regions".
func (se *StorageEndpoint) LoadRegionSyncerCommittedRegionCount() (uint64, error) {
	value, err := se.Load(keypath.RegionSyncerCommittedRegionCountPath())
	if err != nil || value == "" {
		return 0, err
	}
	count, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, errs.ErrStrconvParseUint.Wrap(err).GenWithStackByArgs()
	}
	return count, nil
}

// SaveRegionSyncerCommittedRegionCount persists the region count the current
// leader is serving so other members can tell whether they are caught up
// before campaigning for PD leadership.
func (se *StorageEndpoint) SaveRegionSyncerCommittedRegionCount(count uint64) error {
	return se.Save(keypath.RegionSyncerCommittedRegionCountPath(), strconv.FormatUint(count, 10))
}
