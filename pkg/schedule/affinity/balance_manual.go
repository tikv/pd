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

package affinity

import (
	"context"
	"errors"
	"fmt"

	"github.com/tikv/pd/pkg/errs"
)

// BalancePlan is one target in a table rebalance preview or apply.
// The checker supplies targets; Manager owns persistence.
type BalancePlan struct {
	GroupID          string        `json:"group_id"`
	LeaderStoreID    uint64        `json:"leader_store_id"`
	VoterStoreIDs    []uint64      `json:"voter_store_ids"`
	Changed          bool          `json:"changed"`
	OldLeaderStoreID uint64        `json:"old_leader_store_id,omitempty"`
	OldVoterStoreIDs []uint64      `json:"old_voter_store_ids,omitempty"`
	BeforeScore      *BalanceScore `json:"score_before,omitempty"`
	AfterScore       *BalanceScore `json:"score_after,omitempty"`
	MoveBytes        uint64        `json:"move_bytes,omitempty"`
	Revision         string        `json:"revision,omitempty"`
	Status           string        `json:"status,omitempty"`
	Reason           string        `json:"reason,omitempty"`
}

// BalanceApplyResult preserves per-group progress when apply stops.
type BalanceApplyResult struct {
	Plans []BalancePlan `json:"plans"`
	Error string        `json:"error"`
}

// BalanceRebalancer plans, submits and waits for a manual table operation.
// Its concrete implementation lives in checker alongside placement validation.
type BalanceRebalancer func(context.Context, string, bool) ([]BalancePlan, error)

// SetBalanceRebalancer connects the manual API to the checker implementation.
func (m *Manager) SetBalanceRebalancer(rebalance BalanceRebalancer) {
	m.Lock()
	defer m.Unlock()
	m.balanceRebalancer = rebalance
}

// IsManualBalanceRunning lets the automatic worker skip an occupied table.
// It never waits for a manual operation to finish.
func (m *Manager) IsManualBalanceRunning(groupID string) bool {
	m.RLock()
	defer m.RUnlock()
	_, running := m.manualBalanceTables[TableGroupPrefix(groupID)]
	return running
}

// RebalanceAffinityGroup previews or applies one complete table rebalance.
// Only manual requests for the same table exclude each other. Metadata and
// durable transaction conditions still arbitrate external target changes.
func (m *Manager) RebalanceAffinityGroup(ctx context.Context, groupID string, apply bool) ([]BalancePlan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := m.CheckAndGetAffinityGroupState(groupID); err != nil {
		return nil, err
	}
	tableID := TableGroupPrefix(groupID)
	m.Lock()
	rebalance := m.balanceRebalancer
	_, busy := m.manualBalanceTables[tableID]
	if rebalance == nil || busy {
		m.Unlock()
		if busy {
			return nil, ErrBalanceBusy
		}
		return nil, ErrBalanceNotReady
	}
	m.manualBalanceTables[tableID] = struct{}{}
	m.Unlock()
	defer func() {
		m.Lock()
		delete(m.manualBalanceTables, tableID)
		m.Unlock()
	}()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(m.ctx, cancel)
	defer stop()
	defer cancel()
	plans, err := rebalance(ctx, groupID, apply)
	if errs.ErrEtcdTxnConflict.Equal(err) || errors.Is(err, errs.ErrEtcdTxnConflict) {
		err = fmt.Errorf("%w: %v", ErrBalanceConflict, err)
	}
	return plans, err
}

// ErrBalanceBusy indicates another rebalance request owns the table.
var ErrBalanceBusy = errors.New("affinity rebalance is busy")

// ErrBalanceNotReady indicates the table or checker cannot safely rebalance yet.
var ErrBalanceNotReady = errors.New("affinity rebalance is not ready")

// ErrBalanceConflict indicates that a planning dependency changed before submission.
var ErrBalanceConflict = errors.New("affinity rebalance plan conflict")
