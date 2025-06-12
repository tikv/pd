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
	"strconv"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// MinResolvedTSPoint is the min resolved ts for a store
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type MinResolvedTSPoint struct {
	MinResolvedTS uint64 `json:"min_resolved_ts"`
}

// MinResolvedTSStorage defines the storage operations on the min resolved ts.
type MinResolvedTSStorage interface {
	LoadMinResolvedTS() (uint64, error)
	SaveMinResolvedTS(minResolvedTS uint64) error
}

var _ MinResolvedTSStorage = (*StorageEndpoint)(nil)

// LoadMinResolvedTS loads the min resolved ts from storage.
func (se *StorageEndpoint) LoadMinResolvedTS() (uint64, error) {
	value, err := se.Load(keypath.MinResolvedTSPath())
	if err != nil || value == "" {
		return 0, err
	}
	minResolvedTS, err := strconv.ParseUint(value, 16, 64)
	if err != nil {
		return 0, errs.ErrStrconvParseUint.Wrap(err).GenWithStackByArgs()
	}
	return minResolvedTS, nil
}

// SaveMinResolvedTS saves the min resolved ts.
func (se *StorageEndpoint) SaveMinResolvedTS(minResolvedTS uint64) error {
	value := strconv.FormatUint(minResolvedTS, 16)
	return se.Save(keypath.MinResolvedTSPath(), value)
}
