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

package command

import (
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/client/clients/gc"
	"github.com/tikv/pd/pkg/keyspace/constant"
)

type gcBarrierOutput struct {
	BarrierID  string `json:"barrier_id"`
	BarrierTS  uint64 `json:"barrier_ts"`
	TTLSeconds int64  `json:"ttl_seconds"`
}

type gcStateOutput struct {
	KeyspaceID        uint32            `json:"keyspace_id"`
	IsKeyspaceLevelGC bool              `json:"is_keyspace_level_gc"`
	TxnSafePoint      uint64            `json:"txn_safe_point"`
	GCSafePoint       uint64            `json:"gc_safe_point"`
	GCBarriers        []gcBarrierOutput `json:"gc_barriers"`
}

type keyspaceGCStateOutput struct {
	RequestedKeyspaceID uint32            `json:"requested_keyspace_id"`
	EffectiveKeyspaceID uint32            `json:"effective_keyspace_id"`
	IsKeyspaceLevelGC   bool              `json:"is_keyspace_level_gc"`
	TxnSafePoint        uint64            `json:"txn_safe_point"`
	GCSafePoint         uint64            `json:"gc_safe_point"`
	GCBarriers          []gcBarrierOutput `json:"gc_barriers"`
}

type allGCStatesOutput struct {
	GCStates         []gcStateOutput   `json:"gc_states"`
	GlobalGCBarriers []gcBarrierOutput `json:"global_gc_barriers"`
}

func parseGCStateKeyspaceID(value string) (uint32, error) {
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, errors.Annotatef(err, "invalid keyspace ID %q", value)
	}
	keyspaceID := uint32(parsed)
	if keyspaceID > constant.MaxValidKeyspaceID && keyspaceID != constant.NullKeyspaceID {
		return 0, errors.Errorf(
			"invalid keyspace ID %q: expected 0 through %d or %d",
			value,
			constant.MaxValidKeyspaceID,
			constant.NullKeyspaceID,
		)
	}
	return keyspaceID, nil
}

func gcStateTTLSeconds(ttl time.Duration) int64 {
	if ttl == gc.TTLNeverExpire {
		return math.MaxInt64
	}
	return int64(ttl / time.Second)
}

func sortGCBarrierOutputs(barriers []gcBarrierOutput) {
	sort.Slice(barriers, func(i, j int) bool {
		if barriers[i].BarrierTS != barriers[j].BarrierTS {
			return barriers[i].BarrierTS < barriers[j].BarrierTS
		}
		return barriers[i].BarrierID < barriers[j].BarrierID
	})
}

func newLocalGCBarrierOutputs(barriers []*gc.GCBarrierInfo) []gcBarrierOutput {
	result := make([]gcBarrierOutput, 0, len(barriers))
	for _, barrier := range barriers {
		result = append(result, gcBarrierOutput{
			BarrierID:  barrier.BarrierID,
			BarrierTS:  barrier.BarrierTS,
			TTLSeconds: gcStateTTLSeconds(barrier.TTL),
		})
	}
	sortGCBarrierOutputs(result)
	return result
}

func newGlobalGCBarrierOutputs(barriers []*gc.GlobalGCBarrierInfo) []gcBarrierOutput {
	result := make([]gcBarrierOutput, 0, len(barriers))
	for _, barrier := range barriers {
		result = append(result, gcBarrierOutput{
			BarrierID:  barrier.BarrierID,
			BarrierTS:  barrier.BarrierTS,
			TTLSeconds: gcStateTTLSeconds(barrier.TTL),
		})
	}
	sortGCBarrierOutputs(result)
	return result
}

func newKeyspaceGCStateOutput(
	requestedKeyspaceID uint32,
	state gc.GCState,
) (keyspaceGCStateOutput, error) {
	barriers, err := state.GetGCBarriers()
	if err != nil {
		return keyspaceGCStateOutput{}, errors.Annotatef(
			err,
			"failed to read GC barriers for keyspace %d",
			requestedKeyspaceID,
		)
	}
	return keyspaceGCStateOutput{
		RequestedKeyspaceID: requestedKeyspaceID,
		EffectiveKeyspaceID: state.KeyspaceID,
		IsKeyspaceLevelGC:   state.IsKeyspaceLevelGC,
		TxnSafePoint:        state.TxnSafePoint,
		GCSafePoint:         state.GCSafePoint,
		GCBarriers:          newLocalGCBarrierOutputs(barriers),
	}, nil
}

func newGCStateOutput(state gc.GCState) (gcStateOutput, error) {
	barriers, err := state.GetGCBarriers()
	if err != nil {
		return gcStateOutput{}, errors.Annotatef(
			err,
			"failed to read GC barriers for keyspace %d",
			state.KeyspaceID,
		)
	}
	return gcStateOutput{
		KeyspaceID:        state.KeyspaceID,
		IsKeyspaceLevelGC: state.IsKeyspaceLevelGC,
		TxnSafePoint:      state.TxnSafePoint,
		GCSafePoint:       state.GCSafePoint,
		GCBarriers:        newLocalGCBarrierOutputs(barriers),
	}, nil
}

func newAllGCStatesOutput(clusterState gc.ClusterGCStates) (allGCStatesOutput, error) {
	states := make([]gcStateOutput, 0, len(clusterState.GCStates))
	for _, state := range clusterState.GCStates {
		converted, err := newGCStateOutput(state)
		if err != nil {
			return allGCStatesOutput{}, err
		}
		states = append(states, converted)
	}
	sort.Slice(states, func(i, j int) bool {
		return states[i].KeyspaceID < states[j].KeyspaceID
	})

	globalBarriers, err := clusterState.GetGlobalGCBarriers()
	if err != nil {
		return allGCStatesOutput{}, errors.Annotate(err, "failed to read global GC barriers")
	}
	return allGCStatesOutput{
		GCStates:         states,
		GlobalGCBarriers: newGlobalGCBarrierOutputs(globalBarriers),
	}, nil
}
