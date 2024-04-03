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
)

// MinWatermarkPoint is the min watermark for a store
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type MinWatermarkPoint struct {
	MinWatermark uint64 `json:"min_resolved_ts"`
}

// MinWatermarkStorage defines the storage operations on the min watermark.
type MinWatermarkStorage interface {
	LoadMinWatermark() (uint64, error)
	SaveMinWatermark(minWatermark uint64) error
}

var _ MinWatermarkStorage = (*StorageEndpoint)(nil)

// LoadMinWatermark loads the min watermark from storage.
func (se *StorageEndpoint) LoadMinWatermark() (uint64, error) {
	value, err := se.Load(MinWatermarkPath())
	if err != nil || value == "" {
		return 0, err
	}
	minWatermark, err := strconv.ParseUint(value, 16, 64)
	if err != nil {
		return 0, errs.ErrStrconvParseUint.Wrap(err).GenWithStackByArgs()
	}
	return minWatermark, nil
}

// SaveMinWatermark saves the min watermark.
func (se *StorageEndpoint) SaveMinWatermark(minWatermark uint64) error {
	value := strconv.FormatUint(minWatermark, 16)
	return se.Save(MinWatermarkPath(), value)
}
