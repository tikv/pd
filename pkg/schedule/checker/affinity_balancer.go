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

package checker

import (
	"context"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/schedule/affinity"
)

const (
	affinityBalanceRetryInterval    = time.Second
	affinityBalanceMaxRetryInterval = 30 * time.Second
	// affinityBalanceIdleRetry is how long a table that had nothing to write
	// waits before it is planned again without a metadata change. Changes of
	// stores, placement rules and Regions are picked up then.
	affinityBalanceIdleRetry = 5 * time.Minute
	// affinityBalanceBusyRetry replaces affinityBalanceIdleRetry for a table
	// that may improve soon, because a group is moving to its target or a
	// store is temporarily blocked.
	affinityBalanceBusyRetry = 30 * time.Second
)

// affinityBalancer is the worker that balances the targets of partition
// tables. The affinity checker registers a table once a group of it is
// stable, and every tick plans the registered tables.
type affinityBalancer struct {
	checker *AffinityChecker
	mu      sync.Mutex
	tables  map[affinity.TableID]struct{}
	idle    map[affinity.TableID]balanceIdle
}

// balanceIdle records a table whose last plan wrote nothing. The table is
// not planned again until its version changes or the record expires.
type balanceIdle struct {
	version uint64
	until   time.Time
}

func newAffinityBalancer(ctx context.Context, checker *AffinityChecker) *affinityBalancer {
	b := &affinityBalancer{
		checker: checker,
		tables:  make(map[affinity.TableID]struct{}),
		idle:    make(map[affinity.TableID]balanceIdle),
	}
	go b.run(ctx)
	return b
}

func (b *affinityBalancer) enqueue(groupID string) {
	tableID, ok := affinity.TableIDForGroup(groupID)
	if !ok {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.tables[tableID] = struct{}{}
}

func (b *affinityBalancer) run(ctx context.Context) {
	interval := affinityBalanceRetryInterval
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if b.balance(ctx) {
				interval = affinityBalanceRetryInterval
			} else {
				interval = min(2*interval, affinityBalanceMaxRetryInterval)
			}
			timer.Reset(interval)
		}
	}
}

// balance plans every registered table that is not idle and reports whether
// any target was written.
func (b *affinityBalancer) balance(ctx context.Context) bool {
	allowed := b.checker.balanceAllowed()
	defer b.collectTableMetrics(!allowed)
	if !allowed {
		return false
	}
	manager := b.checker.affinityManager
	now := time.Now()
	b.mu.Lock()
	ids := make([]affinity.TableID, 0, len(b.tables))
	for id := range b.tables {
		version, exists := manager.GetBalanceVersion(id)
		if !exists {
			delete(b.tables, id)
			delete(b.idle, id)
			continue
		}
		if idle, ok := b.idle[id]; ok && idle.version == version && now.Before(idle.until) {
			continue
		}
		ids = append(ids, id)
	}
	b.mu.Unlock()
	if len(ids) == 0 {
		return false
	}
	slices.Sort(ids)
	// The tables of one batch share the counts. A table moves the targets it
	// writes in the counts, so that the later tables break ties against them.
	counts := manager.GetBalanceCounts()
	written := false
	for _, id := range ids {
		ok, err := b.step(ctx, id, counts)
		written = written || ok
		if ctx.Err() != nil {
			return written
		}
		if err != nil {
			log.Warn("failed to balance affinity table", zap.String("table-id", string(id)), zap.Error(err))
		}
	}
	return written
}

// step plans a table and writes the plan. It reports whether any target was
// written. A table that has nothing to write is recorded idle.
func (b *affinityBalancer) step(ctx context.Context, id affinity.TableID, counts affinity.BalanceCounts) (bool, error) {
	start := time.Now()
	result := "skipped"
	var res balancePlanResult
	var version uint64
	var groups, numWritten int
	var commitDuration time.Duration
	defer func() {
		duration := time.Since(start)
		recordAffinityBalanceStepMetrics(result, &res, duration, commitDuration, numWritten)
		if ce := log.L().Check(zap.DebugLevel, "planned affinity table"); ce != nil {
			ce.Write(zap.String("table-id", string(id)), zap.Uint64("version", version), zap.String("result", result),
				zap.Int("groups", groups), zap.Int("mutable", res.groupCounts[balanceGroupMutable]),
				zap.Int("fixed", res.groupCounts[balanceGroupFixed]), zap.Int("unready", res.groupCounts[balanceGroupUnready]),
				zap.Int("unsupported", res.groupCounts[balanceGroupUnsupported]),
				zap.Any("baseline", res.planner.Baseline), zap.Any("score", res.planner.Score), zap.Any("bound", res.planner.Bound),
				zap.String("stop", string(res.planner.Stop)), zap.Int64("work", res.work), zap.Int("visits", res.visits),
				zap.Int("rounds", len(res.stops)), zap.Duration("duration", duration))
		}
	}()
	c := b.checker
	if !c.balanceAllowed() || !c.affinityManager.IsBalanceTableEligible(id) {
		return false, nil
	}
	revision := c.ruleManager.GetRevision()
	snapshot := c.affinityManager.GetBalanceSnapshot(id, counts)
	if snapshot == nil {
		return false, nil
	}
	version, groups = snapshot.Version, len(snapshot.Groups)
	if balanceCreatingTable(snapshot, time.Now()) {
		result = affinityBalanceCreating
		return false, nil
	}
	result = affinityBalanceError
	res, err := c.planBalanceTargets(ctx, snapshot)
	if err != nil {
		return false, err
	}
	if len(res.targets) == 0 {
		result = affinityBalanceNoImprovement
		wait := affinityBalanceIdleRetry
		if res.retry {
			result = affinityBalanceBlocked
			wait = affinityBalanceBusyRetry
		}
		b.mu.Lock()
		b.idle[id] = balanceIdle{version: snapshot.Version, until: time.Now().Add(wait)}
		b.mu.Unlock()
		return false, nil
	}
	commitStart := time.Now()
	written, reason, err := c.commitBalancePlan(ctx, snapshot, res, revision)
	commitDuration = time.Since(commitStart)
	numWritten = len(written)
	moveBalanceCounts(counts, snapshot, res.targets, written)
	if len(written) > 0 {
		log.Info("balanced affinity table", zap.String("table-id", string(id)),
			zap.Int("planned", len(res.targets)), zap.Int("written", len(written)), zap.Uint64("version", version),
			zap.Any("baseline", res.planner.Baseline), zap.Any("score", res.planner.Score), zap.Any("bound", res.planner.Bound),
			zap.String("stop", string(res.planner.Stop)), zap.Int("changed", res.planner.Changed), zap.Duration("duration", time.Since(start)))
	} else {
		log.Info("affinity balance plan not written", zap.String("table-id", string(id)),
			zap.Int("planned", len(res.targets)), zap.String("reason", string(reason)))
	}
	if err == nil {
		result = affinityBalanceRejected
		if len(written) > 0 {
			result = affinityBalanceWritten
		}
	}
	return len(written) > 0, err
}

// moveBalanceCounts moves the written targets from their original stores to
// their new ones in counts.
func moveBalanceCounts(counts affinity.BalanceCounts, snapshot *affinity.BalanceSnapshot, targets []*affinity.Group, written []string) {
	writtenIDs := make(map[string]struct{}, len(written))
	for _, id := range written {
		writtenIDs[id] = struct{}{}
	}
	for _, target := range targets {
		if _, ok := writtenIDs[target.ID]; !ok {
			continue
		}
		original := snapshot.Group(target.ID)
		counts.Leader[original.LeaderStoreID]--
		counts.Leader[target.LeaderStoreID]++
		for _, id := range original.VoterStoreIDs {
			counts.Voter[id]--
		}
		for _, id := range target.VoterStoreIDs {
			counts.Voter[id]++
		}
	}
}
