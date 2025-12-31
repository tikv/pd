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

// ExternalTimestamp is the external timestamp.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type ExternalTimestamp struct {
	ExternalTimestamp uint64 `json:"external_timestamp"`
}

// ExternalTSStorage defines the storage operations on the external timestamp.
type ExternalTSStorage interface {
	LoadExternalTS() (uint64, error)
	SaveExternalTS(timestamp uint64) error
}

var _ ExternalTSStorage = (*StorageEndpoint)(nil)

// LoadExternalTS loads the external timestamp from storage.
func (se *StorageEndpoint) LoadExternalTS() (uint64, error) {
	value, err := se.Load(keypath.ExternalTimestampPath())
	if err != nil || value == "" {
		return 0, err
	}
	timestamp, err := strconv.ParseUint(value, 16, 64)
	if err != nil {
		return 0, errs.ErrStrconvParseUint.Wrap(err).GenWithStackByArgs()
	}
	return timestamp, nil
}

// SaveExternalTS saves the external timestamp.
func (se *StorageEndpoint) SaveExternalTS(timestamp uint64) error {
	value := strconv.FormatUint(timestamp, 16)
	return se.Save(keypath.ExternalTimestampPath(), value)
}
