// Copyright 2025 TiKV Project Authors.
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

package schedulers

import (
	"bytes"
	"slices"

	"github.com/tikv/pd/pkg/core"
)

type KeyRangeManager interface {
	GetAvailableKeyRange() []core.KeyRange
	AddKeyRange([]core.KeyRange) error
	RemoveDirtyKeyRange([]core.KeyRange)
}

var _ KeyRangeManager = &DefaultKeyRangeManager{}

type DefaultKeyRangeManager struct {
	sortedKeyRanges []core.KeyRange
}

func (rm *DefaultKeyRangeManager) GetAvailableKeyRange() []core.KeyRange {

}

func (rm *DefaultKeyRangeManager) AddKeyRange(keyRanges []core.KeyRange) error {
	rm.sortedKeyRanges = append(rm.sortedKeyRanges, keyRanges...)
	slices.sli
	return nil
}

func (rm *DefaultKeyRangeManager) RemoveDirtyKeyRange(keyRanges []core.KeyRange) {
	for _, keyRange := range keyRanges {
		for i, dirtyKeyRange := range rm.dirtyKeyRanges {
			if bytes.Equal(keyRange.StartKey, dirtyKeyRange.StartKey) &&
				bytes.Equal(keyRange.EndKey, dirtyKeyRange.EndKey) {
				rm.dirtyKeyRanges = append(rm.dirtyKeyRanges[:i], rm.dirtyKeyRanges[i+1:]...)
				break
			}
		}
	}
	return
}

func (rm *DefaultKeyRangeManager) removeKeyRange(keyRange core.KeyRange) []core.KeyRange {

}
