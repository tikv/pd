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

package handlers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
)

func TestConvertAndValidateRangeOpsRequestOrder(t *testing.T) {
	re := require.New(t)
	store := storage.NewStorageWithMemoryBackend()
	regionLabeler, err := labeler.NewRegionLabeler(t.Context(), store, time.Second)
	re.NoError(err)
	manager, err := affinity.NewManager(t.Context(), store, core.NewStoresInfo(), mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)
	re.NoError(manager.CreateAffinityGroups([]affinity.GroupKeyRanges{{GroupID: "z"}, {GroupID: "a"}, {GroupID: "m"}}))
	var ops []GroupRangesModification
	for i, id := range []string{"z", "a", "z", "m", "a"} {
		ops = append(ops, GroupRangesModification{ID: id, Ranges: []AffinityKeyRange{{StartKey: []byte{byte(i + 1)}, EndKey: []byte{byte(i + 2)}}}})
	}
	for _, remove := range []bool{false, true} {
		for range 64 {
			result, err := convertAndValidateRangeOps(ops, manager, make(map[string]bool), remove)
			re.NoError(err)
			re.Len(result, 3)
			re.Equal([]string{"z", "a", "m"}, []string{result[0].GroupID, result[1].GroupID, result[2].GroupID})
			re.Len(result[0].KeyRanges, 2)
			re.Equal([]byte{1}, result[0].KeyRanges[0].StartKey)
			re.Equal([]byte{3}, result[0].KeyRanges[1].StartKey)
		}
	}
}
