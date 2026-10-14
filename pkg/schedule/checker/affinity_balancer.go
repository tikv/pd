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
	"bytes"
	"cmp"
	"context"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/affinity"
)

// A worker yields between complete Region checks or raw candidates. A single
// placement/best-location call runs normally; this is no internal work budget.
const affinityBalanceTurn = 10 * time.Millisecond

type affinityBalancer struct {
	checker *AffinityChecker
	mu      sync.Mutex
	tables  map[string]*affinityBalanceTask
	queue   chan string
}

type affinityBalanceTask struct {
	// Queue admission and completed results are protected by balancer.mu.
	queued      bool
	stable      bool
	version     affinity.BalanceVersion
	fingerprint uint64
	retryAfter  time.Time
	retryDelay  time.Duration
	// Only the worker reads or writes unfinished search progress.
	search *balanceSearch
}

type balanceSearchPhase uint8

const (
	balanceScanCurrent balanceSearchPhase = iota
	balanceFindLeader
	balanceFindVoter
	balanceScanFinal
	balanceVerifyFinal
)

type balanceSearch struct {
	snapshot    *affinity.BalanceSnapshot
	scorer      *affinity.BalanceScorer
	fingerprint uint64
	phase       balanceSearchPhase
	groupIndex  int
	scan        *balanceRegionScan
	candidates  balanceCandidates
	move        affinity.BalanceMove
	target      *affinity.Group
	blocked     bool
	moveBytes   uint64
	verified    []balanceCheckedRegion
	verifyIndex int
}

type balanceCheckedRegion struct {
	group  *affinity.GroupState
	region *core.RegionInfo
}

func newAffinityBalancer(ctx context.Context, checker *AffinityChecker) *affinityBalancer {
	balancer := &affinityBalancer{checker: checker, tables: make(map[string]*affinityBalanceTask), queue: make(chan string, 64)}
	go balancer.run(ctx)
	return balancer
}

func (b *affinityBalancer) enqueue(groupID string) {
	version, exists := b.checker.affinityManager.GetBalanceVersion(groupID)
	if !exists {
		return
	}
	tableID := affinity.TableGroupPrefix(groupID)
	b.mu.Lock()
	defer b.mu.Unlock()
	task := b.tables[tableID]
	if task == nil {
		task = &affinityBalanceTask{}
		b.tables[tableID] = task
	}
	if task.queued || task.stable && task.version == version ||
		task.version == version && time.Now().Before(task.retryAfter) {
		return
	}
	select {
	case b.queue <- tableID:
		task.queued = true
	default:
	}
}

func (b *affinityBalancer) run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.mu.Lock()
			for id, task := range b.tables {
				if _, exists := b.checker.affinityManager.GetBalanceVersion(id); !exists && !task.queued {
					delete(b.tables, id)
					continue
				}
				if !task.queued && !task.stable && !time.Now().Before(task.retryAfter) {
					select {
					case b.queue <- id:
						task.queued = true
					default:
					}
				}
			}
			b.mu.Unlock()
		case id := <-b.queue:
			b.mu.Lock()
			task := b.tables[id]
			b.mu.Unlock()
			if task == nil {
				continue
			}
			stable, resume, err := b.step(ctx, id, task)
			if err != nil && !errs.ErrEtcdTxnConflict.Equal(err) && ctx.Err() == nil {
				log.Warn("failed to balance affinity table", zap.String("table-id", id), zap.Error(err))
			}
			b.mu.Lock()
			task.queued, task.stable = false, stable && err == nil
			if task.search != nil {
				task.version, task.fingerprint = task.search.snapshot.Version, task.search.fingerprint
			}
			if !resume {
				task.search = nil
			}
			if resume && err == nil {
				task.retryDelay = 0
				select {
				case b.queue <- id:
					task.queued = true
				default:
					task.retryAfter = time.Now()
				}
			} else {
				task.retryDelay = min(max(time.Second, task.retryDelay*2), 30*time.Second)
				task.retryAfter = time.Now().Add(task.retryDelay)
			}
			b.mu.Unlock()
		}
	}
}

func sameBalanceTable(a, b affinity.BalanceVersion) bool {
	return a.Table == b.Table && a.Ranges == b.Ranges && a.Stores == b.Stores
}

func (b *affinityBalancer) prepareSearch(id string, task *affinityBalanceTask) (*balanceSearch, error) {
	manager := b.checker.affinityManager
	version, exists := manager.GetBalanceVersion(id)
	if !exists {
		task.search = nil
		return nil, nil
	}
	search := task.search
	if search != nil && search.snapshot.Version == version {
		return search, nil
	}
	snapshot := manager.GetBalanceSnapshot(id)
	if snapshot == nil {
		return nil, nil
	}
	fingerprint := b.checker.balanceInputFingerprint(snapshot)
	if !manager.BalanceSnapshotCurrent(snapshot) {
		return nil, errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	// Independent changes elsewhere in the cluster do not restart this table's
	// scan. Commit dependencies are recaptured before the final recheck.
	if search != nil && sameBalanceTable(search.snapshot.Version, snapshot.Version) && search.fingerprint == fingerprint {
		search.snapshot = snapshot
		if search.phase >= balanceScanFinal {
			search.phase, search.groupIndex, search.scan = balanceFindLeader, 0, nil
			if search.candidates.replace {
				search.phase = balanceFindVoter
			}
			scan := newBalanceRegionScan(snapshot.Ranges[search.target.ID])
			search.scan, search.verified = &scan, nil
		}
		return search, nil
	}
	if task.stable && sameBalanceTable(task.version, snapshot.Version) && task.fingerprint == fingerprint {
		return &balanceSearch{snapshot: snapshot, fingerprint: fingerprint, phase: balanceVerifyFinal}, nil
	}
	groups := make([]*affinity.Group, 0, len(snapshot.Groups))
	for _, group := range snapshot.Groups {
		groups = append(groups, &group.Group)
	}
	scorer, err := affinity.NewBalanceScorer(groups)
	if err != nil {
		return nil, err
	}
	search = &balanceSearch{snapshot: snapshot, scorer: scorer, fingerprint: fingerprint}
	search.candidates = newBalanceCandidates(snapshot, scorer, false)
	task.search = search
	return search, nil
}

// step searches for the first legal strict improvement, rather than constructing
// a legal action library or promising the best action in this turn.
func (b *affinityBalancer) step(ctx context.Context, id string, task *affinityBalanceTask) (stable, resume bool, err error) {
	c, manager := b.checker, b.checker.affinityManager
	if !c.balanceSchedulingAllowed() {
		return false, false, nil
	}
	search, err := b.prepareSearch(id, task)
	if err != nil || search == nil {
		return false, false, err
	}
	task.search = search
	snapshot := search.snapshot
	if search.scorer == nil || len(snapshot.Groups) <= 1 {
		err := manager.PrepareBalanceCommit(ctx, snapshot)
		return err == nil, false, err
	}
	deadline := time.Now().Add(affinityBalanceTurn)
	waiting := c.waitingAffinityRegions()
	for ctx.Err() == nil {
		if !manager.BalanceSnapshotCurrent(snapshot) {
			return false, false, errs.ErrEtcdTxnConflict.FastGenByArgs()
		}
		if time.Now().After(deadline) {
			return false, true, nil
		}
		switch search.phase {
		case balanceScanCurrent, balanceScanFinal:
			if search.groupIndex >= len(snapshot.Groups) {
				if search.phase == balanceScanCurrent {
					search.phase = balanceFindLeader
				} else {
					search.phase = balanceVerifyFinal
				}
				continue
			}
			group := snapshot.Groups[search.groupIndex]
			if group.BalancePolicy == affinity.BalancePolicyFixed {
				search.groupIndex++
				continue
			}
			if search.scan == nil {
				ranges := snapshot.Ranges[group.ID]
				if !group.AffinitySchedulingAllowed || len(ranges.KeyRanges) == 0 {
					return false, false, nil
				}
				scan := newBalanceRegionScan(ranges)
				search.scan = &scan
			}
			region, covered, done := search.scan.next(c.cluster)
			if !covered {
				return false, false, nil
			}
			if done {
				search.groupIndex++
				search.scan = nil
				continue
			}
			var target *affinity.Group
			if search.phase == balanceScanFinal && group.ID == search.target.ID {
				target = search.target
			}
			status := c.validateBalanceRegion(group, target, region, waiting)
			if status == balanceUnready {
				return false, false, nil
			}
			if status != balanceAllowed {
				search.blocked = search.blocked || status == balanceTemporarilyBlocked
				search.phase = balanceFindLeader
				if search.candidates.replace {
					search.phase = balanceFindVoter
				}
				search.target, search.scan = nil, nil
				search.verified = nil
				continue
			}
			if search.phase == balanceScanFinal {
				search.verified = append(search.verified, balanceCheckedRegion{group, region})
				if target != nil && search.move.FromStoreID != 0 {
					search.moveBytes += approximateBalanceRegionBytes(region)
				}
			}
		case balanceFindLeader, balanceFindVoter:
			if search.target == nil {
				move, found := search.candidates.next()
				if !found {
					if search.blocked {
						return false, false, nil
					}
					if search.phase == balanceFindLeader {
						search.phase = balanceFindVoter
						search.candidates = newBalanceCandidates(snapshot, search.scorer, true)
						continue
					}
					err := manager.PrepareBalanceCommit(ctx, snapshot)
					return err == nil, false, err
				}
				if _, improves := search.scorer.Evaluate(move); !improves {
					continue
				}
				search.move = move
				group := snapshot.Groups[search.candidates.group]
				target := group.Group
				target.LeaderStoreID, target.VoterStoreIDs = move.LeaderStoreID, slices.Clone(group.VoterStoreIDs)
				if move.FromStoreID != 0 {
					target.VoterStoreIDs[slices.Index(target.VoterStoreIDs, move.FromStoreID)] = move.ToStoreID
				}
				search.target = &target
				scan := newBalanceRegionScan(snapshot.Ranges[group.ID])
				search.scan = &scan
			}
			group := snapshot.Groups[search.candidates.group]
			status := c.balanceTargetExecutionStatus(&group.Group, search.target)
			if status != balanceAllowed {
				search.blocked = search.blocked || status == balanceTemporarilyBlocked
				search.target, search.scan = nil, nil
				continue
			}
			region, covered, done := search.scan.next(c.cluster)
			if !covered {
				return false, false, nil
			}
			if done {
				if err := manager.PrepareBalanceCommit(ctx, snapshot); err != nil {
					return false, false, err
				}
				search.phase, search.groupIndex, search.scan = balanceScanFinal, 0, nil
				search.verified, search.verifyIndex, search.moveBytes = nil, 0, 0
				continue
			}
			status = c.validateBalanceRegion(group, search.target, region, waiting)
			if status == balanceUnready {
				return false, false, nil
			}
			if status != balanceAllowed {
				search.blocked = search.blocked || status == balanceTemporarilyBlocked
				search.target, search.scan = nil, nil
			}
		case balanceVerifyFinal:
			if search.verifyIndex < len(search.verified) {
				checked := search.verified[search.verifyIndex]
				current := c.cluster.GetRegion(checked.region.GetID())
				if current == nil || current.GetRegionEpoch().GetVersion() != checked.region.GetRegionEpoch().GetVersion() ||
					current.GetRegionEpoch().GetConfVer() != checked.region.GetRegionEpoch().GetConfVer() ||
					!bytes.Equal(current.GetStartKey(), checked.region.GetStartKey()) || !bytes.Equal(current.GetEndKey(), checked.region.GetEndKey()) ||
					!c.balanceRegionReady(checked.group, current, waiting) {
					return false, false, nil
				}
				search.verifyIndex++
				continue
			}
			group := snapshot.Groups[search.candidates.group]
			if !c.balanceSchedulingAllowed() || c.balanceTargetExecutionStatus(&group.Group, search.target) != balanceAllowed {
				return false, false, nil
			}
			committed, err := manager.CommitBalanceTarget(ctx, snapshot, search.target)
			if committed {
				log.Debug("balanced affinity partition", zap.String("group-id", search.target.ID), zap.Uint64("move-bytes", search.moveBytes))
			}
			return false, false, err
		}
	}
	return false, false, ctx.Err()
}

// A concrete cursor enumerates raw single moves without retaining actions.
// Scoring occurs before any Region scan; the active group's small leader slice
// puts the original leader first and uses global counts to avoid an ID bias.
type balanceCandidates struct {
	snapshot                *affinity.BalanceSnapshot
	scorer                  *affinity.BalanceScorer
	replace                 bool
	group, from, to, leader int
	voters, leaders, stores []uint64
}

func newBalanceCandidates(snapshot *affinity.BalanceSnapshot, scorer *affinity.BalanceScorer, replace bool) balanceCandidates {
	stores := slices.Clone(snapshot.Stores)
	slices.SortFunc(stores, func(a, b uint64) int {
		if order := cmp.Compare(scorer.VoterCount(a), scorer.VoterCount(b)); order != 0 {
			return order
		}
		if order := cmp.Compare(snapshot.GlobalVoterCounts[a], snapshot.GlobalVoterCounts[b]); order != 0 {
			return order
		}
		return cmp.Compare(a, b)
	})
	return balanceCandidates{snapshot: snapshot, scorer: scorer, replace: replace, stores: stores}
}

func (p *balanceCandidates) next() (affinity.BalanceMove, bool) {
	for p.group < len(p.snapshot.Groups) {
		group := p.snapshot.Groups[p.group]
		if group.BalancePolicy == affinity.BalancePolicyFixed || len(group.VoterStoreIDs) == 0 {
			p.group++
			continue
		}
		if p.voters == nil {
			p.voters = slices.Clone(group.VoterStoreIDs)
			slices.Sort(p.voters)
			p.leaders = slices.Clone(p.voters)
			slices.SortFunc(p.leaders, func(a, b uint64) int {
				if a == b {
					return 0
				}
				if a == group.LeaderStoreID {
					return -1
				}
				if b == group.LeaderStoreID {
					return 1
				}
				if order := cmp.Compare(p.snapshot.GlobalLeaderCounts[a], p.snapshot.GlobalLeaderCounts[b]); order != 0 {
					return order
				}
				return cmp.Compare(a, b)
			})
		}
		if !p.replace && p.leader < len(p.leaders) {
			move := affinity.BalanceMove{GroupID: group.ID, LeaderStoreID: p.leaders[p.leader]}
			p.leader++
			return move, true
		}
		if p.replace && p.from < len(p.voters) && len(p.stores) > 0 {
			var leader uint64
			if p.leader < len(p.leaders) {
				leader = p.leaders[p.leader]
			} else {
				leader = p.stores[p.to]
			}
			move := affinity.BalanceMove{GroupID: group.ID, LeaderStoreID: leader, FromStoreID: p.voters[p.from], ToStoreID: p.stores[p.to]}
			p.leader++
			if p.leader > len(p.leaders) {
				p.leader = 0
				p.to++
				if p.to == len(p.stores) {
					p.to = 0
					p.from++
				}
			}
			return move, true
		}
		p.group++
		p.from, p.to, p.leader = 0, 0, 0
		p.voters, p.leaders = nil, nil
	}
	return affinity.BalanceMove{}, false
}
