// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package operator

import (
	"fmt"
	"sort"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule/filter"
	"github.com/pkg/errors"
)

// Builder is used to create operators. Usage:
//     op, err := NewBuilder(desc, cluster, region).
//                 RemovePeer(store1).
//                 AddPeer(peer1).
//                 SetLeader(store2).
//                 Build(kind)
// The generated Operator will choose the most appropriate execution order
// according to various constraints.
type Builder struct {
	// basic info
	desc        string
	cluster     Cluster
	regionID    uint64
	regionEpoch *metapb.RegionEpoch

	// operation record
	originPeers  peersMap
	originLeader uint64
	targetPeers  peersMap
	targetLeader uint64
	err          error

	// flags
	isLigthWeight bool

	// intermediate states
	currentPeers               peersMap
	currentLeader              uint64
	toAdd, toRemove, toPromote peersMap       // pending tasks.
	steps                      []OpStep       // generated steps.
	peerAddStep                map[uint64]int // record at which step a peer is created.
}

// NewBuilder creates a Builder.
func NewBuilder(desc string, cluster Cluster, region *core.RegionInfo) *Builder {
	var originPeers peersMap
	for _, p := range region.GetPeers() {
		originPeers.Set(p)
	}
	var err error
	if originPeers.Get(region.GetLeader().GetStoreId()) == nil {
		err = errors.Errorf("cannot build operator for region with no leader")
	}

	return &Builder{
		desc:         desc,
		cluster:      cluster,
		regionID:     region.GetID(),
		regionEpoch:  region.GetRegionEpoch(),
		originPeers:  originPeers,
		originLeader: region.GetLeader().GetStoreId(),
		targetPeers:  originPeers.Copy(),
		err:          err,
	}
}

// AddPeer records an add Peer operation in Builder. If p.Id is 0, the builder
// will allocate a new peer ID later.
func (b *Builder) AddPeer(p *metapb.Peer) *Builder {
	if b.err != nil {
		return b
	}
	if old := b.targetPeers.Get(p.GetStoreId()); old != nil {
		b.err = errors.Errorf("cannot add peer %s: already have peer %s", p, old)
	} else {
		b.targetPeers.Set(p)
	}
	return b
}

// RemovePeer records a remove peer operation in Builder.
func (b *Builder) RemovePeer(storeID uint64) *Builder {
	if b.err != nil {
		return b
	}
	if b.targetPeers.Get(storeID) == nil {
		b.err = errors.Errorf("cannot remove peer from %d: not found", storeID)
	} else if b.targetLeader == storeID {
		b.err = errors.Errorf("cannot remove peer from %d: peer is target leader", storeID)
	} else {
		b.targetPeers.Delete(storeID)
	}
	return b
}

// PromoteLearner records a promote learner operation in Builder.
func (b *Builder) PromoteLearner(storeID uint64) *Builder {
	if b.err != nil {
		return b
	}
	p := b.targetPeers.Get(storeID)
	if p == nil {
		b.err = errors.Errorf("cannot promote peer %d: not found", storeID)
	} else if !p.GetIsLearner() {
		b.err = errors.Errorf("cannot promote peer %d: not learner", storeID)
	} else {
		b.targetPeers.Set(&metapb.Peer{Id: p.GetId(), StoreId: p.GetStoreId()})
	}
	return b
}

// SetLeader records the target leader in Builder.
func (b *Builder) SetLeader(storeID uint64) *Builder {
	if b.err != nil {
		return b
	}
	p := b.targetPeers.Get(storeID)
	if p == nil {
		b.err = errors.Errorf("cannot transfer leader to %d: not found", storeID)
	} else if p.GetIsLearner() {
		b.err = errors.Errorf("cannot transfer leader to %d: not voter", storeID)
	} else {
		b.targetLeader = storeID
	}
	return b
}

// SetPeers resets the target peer list.
// If peer's ID is 0, the builder will allocate a new ID later.
// If current target leader does not exist in peers, it will be reset.
func (b *Builder) SetPeers(peers map[uint64]*metapb.Peer) *Builder {
	if b.err != nil {
		return b
	}
	b.targetPeers = peersMap{}
	for _, p := range peers {
		b.targetPeers.Set(p)
	}
	if _, ok := peers[b.targetLeader]; !ok {
		b.targetLeader = 0
	}
	return b
}

// SetLightWeight marks the region as light weight. It is used for scatter regions.
func (b *Builder) SetLightWeight() *Builder {
	b.isLigthWeight = true
	return b
}

// Build creates the Operator.
func (b *Builder) Build(kind OpKind) (*Operator, error) {
	if b.err != nil {
		return nil, b.err
	}

	brief, err := b.prepareBuild()
	if err != nil {
		return nil, err
	}

	kind, err = b.buildSteps(kind)
	if err != nil {
		return nil, err
	}

	return NewOperator(b.desc, brief, b.regionID, b.regionEpoch, kind, b.steps...), nil
}

// Initilize intermediate states.
func (b *Builder) prepareBuild() (string, error) {
	var voterCount int
	for _, p := range b.targetPeers.m {
		if !p.GetIsLearner() {
			voterCount++
		}
	}
	if voterCount == 0 {
		return "", errors.New("cannot create operator: target peers have no voter")
	}

	// Diff `originPeers` and `targetPeers` to initialize `toAdd`,
	// `toPromote`, `toRemove`.
	for _, o := range b.originPeers.m {
		n := b.targetPeers.Get(o.GetStoreId())
		// no peer in targets, or target is learner while old one is voter.
		if n == nil || (n.GetIsLearner() && !o.GetIsLearner()) {
			b.toRemove.Set(o)
			continue
		}
		if o.GetIsLearner() && !n.GetIsLearner() {
			b.toPromote.Set(n)
		}
	}
	for _, n := range b.targetPeers.m {
		o := b.originPeers.Get(n.GetStoreId())
		if o == nil || (n.GetIsLearner() && !o.GetIsLearner()) {
			// old peer not exists, or target is learner while old one is voter.
			if n.GetId() == 0 {
				// Allocate peer ID if need.
				t, err := b.cluster.AllocPeer(0)
				if err != nil {
					return "", err
				}
				n.Id = t.Id
			}
			b.toAdd.Set(n)
		}
	}

	b.currentPeers, b.currentLeader = b.originPeers.Copy(), b.originLeader
	return b.brief(), nil
}

// generate brief description of the operator.
func (b *Builder) brief() string {
	switch {
	case b.toAdd.Len() > 0 && b.toRemove.Len() > 0:
		op := "mv peer"
		if b.isLigthWeight {
			op = "mv light peer"
		}
		return fmt.Sprintf("%s: store %s to %s", op, b.toRemove, b.toAdd)
	case b.toAdd.Len() > 0:
		return fmt.Sprintf("add peer: store %s", b.toAdd)
	case b.toRemove.Len() > 0:
		return fmt.Sprintf("rm peer: store %s", b.toRemove)
	}
	if b.toPromote.Len() > 0 {
		return fmt.Sprintf("promote peer: store %s", b.toPromote)
	}
	if b.targetLeader != b.originLeader {
		return fmt.Sprintf("transfer leader: store %d to %d", b.originLeader, b.targetLeader)
	}
	return ""
}

func (b *Builder) buildSteps(kind OpKind) (OpKind, error) {
	for b.toAdd.Len() > 0 || b.toRemove.Len() > 0 || b.toPromote.Len() > 0 {
		plan := b.peerPlan()
		if plan.empty() {
			return kind, errors.New("fail to build operator: plan is empty")
		}
		if plan.leader1 != 0 && plan.leader1 != b.currentLeader {
			b.execTransferLeader(plan.leader1)
			kind |= OpLeader
		}
		if plan.addOrPromote != nil {
			if b.currentPeers.Get(plan.addOrPromote.GetStoreId()) != nil {
				b.execPromoteLearner(plan.addOrPromote)
			} else {
				b.execAddPeer(plan.addOrPromote)
				kind |= OpRegion
			}
		}
		if plan.leader2 != 0 && plan.leader2 != b.currentLeader {
			b.execTransferLeader(plan.leader2)
			kind |= OpLeader
		}
		if plan.remove != nil {
			b.execRemovePeer(plan.remove)
			kind |= OpRegion
		}
	}
	if b.targetLeader != 0 && b.currentLeader != b.targetLeader {
		if b.currentPeers.Get(b.targetLeader) != nil {
			b.execTransferLeader(b.targetLeader)
			kind |= OpLeader
		}
	}
	if len(b.steps) == 0 {
		return kind, errors.New("no operator step is built")
	}
	return kind, nil
}

func (b *Builder) execTransferLeader(id uint64) {
	p := b.currentPeers.Get(id)
	b.steps = append(b.steps, TransferLeader{FromStore: b.currentLeader, ToStore: p.GetStoreId()})
	b.currentLeader = p.GetStoreId()
}

func (b *Builder) execPromoteLearner(p *metapb.Peer) {
	b.steps = append(b.steps, PromoteLearner{ToStore: p.GetStoreId(), PeerID: p.GetId()})
	b.currentPeers.Set(&metapb.Peer{Id: p.GetId(), StoreId: p.GetStoreId()})
	b.toPromote.Delete(p.GetStoreId())
}

func (b *Builder) execAddPeer(p *metapb.Peer) {
	if b.isLigthWeight {
		b.steps = append(b.steps, AddLightLearner{ToStore: p.GetStoreId(), PeerID: p.GetId()})
	} else {
		b.steps = append(b.steps, AddLearner{ToStore: p.GetStoreId(), PeerID: p.GetId()})
	}
	if !p.GetIsLearner() {
		b.steps = append(b.steps, PromoteLearner{ToStore: p.GetStoreId(), PeerID: p.GetId()})
	}
	b.currentPeers.Set(p)
	if b.peerAddStep == nil {
		b.peerAddStep = make(map[uint64]int)
		b.peerAddStep[p.GetStoreId()] = len(b.steps)
	}
	b.toAdd.Delete(p.GetStoreId())
}

func (b *Builder) execRemovePeer(p *metapb.Peer) {
	b.steps = append(b.steps, RemovePeer{FromStore: p.GetStoreId()})
	b.currentPeers.Delete(p.GetStoreId())
	b.toRemove.Delete(p.GetStoreId())
}

// check if a peer can become leader.
func (b *Builder) allowLeader(peer *metapb.Peer) bool {
	if peer.GetStoreId() == b.currentLeader {
		return true
	}
	if peer.GetIsLearner() {
		return false
	}
	store := b.cluster.GetStore(peer.GetStoreId())
	if store == nil {
		return false
	}
	stateFilter := filter.StoreStateFilter{ActionScope: "operator-builder", TransferLeader: true}
	return !stateFilter.Target(b.cluster, store)
}

// stepPlan is exec step. It can be:
// 1. Add a peer. `addOrPromote` and `leader1` is set.
// 2. Promote a learner. `addOrPromote` and `leader1` is set.
// 3. Remove a peer. `remove` and `leader2` is set.
// 4. Replace a peer. All fields are set.
type stepPlan struct {
	addOrPromote *metapb.Peer
	leader1      uint64 // leader before add/promote peer
	remove       *metapb.Peer
	leader2      uint64 // leader before remove peer
}

func (p stepPlan) String() string {
	return fmt.Sprintf("stepPlan{addOrPromote={%s},remove={%s},leader1=%v,leader2=%v}", p.addOrPromote, p.remove, p.leader1, p.leader2)
}

func (p stepPlan) empty() bool {
	return p.addOrPromote == nil && p.remove == nil
}

func (b *Builder) peerPlan() stepPlan {
	// Replace has the highest priority because it does not change region's
	// voter/learner count.
	if p := b.planReplace(); !p.empty() {
		return p
	}
	if p := b.planRemovePeer(); !p.empty() {
		return p
	}
	if p := b.planAddPeer(); !p.empty() {
		return p
	}
	if p := b.planPromotePeer(); !p.empty() {
		return p
	}
	return stepPlan{}
}

func (b *Builder) planReplace() stepPlan {
	var best stepPlan
	for _, i := range b.toRemove.IDs() {
		r := b.toRemove.Get(i)
		for _, j := range b.toAdd.IDs() {
			if i == j {
				// Special case: remove voter and add learner on same store.
				// Cannot use replace. Need first removePeer then addPeer.
				continue
			}
			a := b.toAdd.Get(j)
			if r.GetIsLearner() == a.GetIsLearner() {
				best = b.planReplaceDetail(best, stepPlan{addOrPromote: a, remove: r})
			}
		}
		if !r.GetIsLearner() {
			for _, j := range b.toPromote.IDs() {
				a := b.toPromote.Get(j)
				best = b.planReplaceDetail(best, stepPlan{addOrPromote: a, remove: r})
			}
		}
	}
	return best
}

func (b *Builder) planReplaceDetail(best, next stepPlan) stepPlan {
	// Replace peer with the nearest one.
	if mb, mc := b.labelMatch(best), b.labelMatch(next); mb > mc {
		return best
	} else if mb < mc {
		best = stepPlan{}
	}

	// Brute force all possible leader combinations to find the best plan.
	for _, leader1 := range b.currentPeers.IDs() {
		if !b.allowLeader(b.currentPeers.Get(leader1)) {
			continue
		}
		next.leader1 = leader1
		for _, leader2 := range b.currentPeers.IDs() {
			if b.allowLeader(b.currentPeers.Get(leader2)) {
				next.leader2 = leader2
				best = b.comparePlan(best, next)
			}
		}
		if b.allowLeader(next.addOrPromote) {
			next.leader2 = next.addOrPromote.GetStoreId()
			best = b.comparePlan(best, next)
		}
	}
	return best
}

func (b *Builder) planRemovePeer() stepPlan {
	var best stepPlan
	for _, i := range b.toRemove.IDs() {
		r := b.toRemove.Get(i)
		for _, leader := range b.currentPeers.IDs() {
			if b.allowLeader(b.currentPeers.Get(leader)) {
				best = b.comparePlan(best, stepPlan{remove: r, leader2: leader})
			}
		}
	}
	return best
}

func (b *Builder) planAddPeer() stepPlan {
	var best stepPlan
	for _, i := range b.toAdd.IDs() {
		a := b.toAdd.Get(i)
		for _, leader := range b.currentPeers.IDs() {
			if b.allowLeader(b.currentPeers.Get(leader)) {
				best = b.comparePlan(best, stepPlan{addOrPromote: a, leader1: leader})
			}
		}
	}
	return best
}

func (b *Builder) planPromotePeer() stepPlan {
	for _, i := range b.toPromote.IDs() {
		p := b.toPromote.Get(i)
		return stepPlan{addOrPromote: p, leader1: b.currentLeader}
	}
	return stepPlan{}
}

// Pick the better plan from 2 candidates.
func (b *Builder) comparePlan(best, next stepPlan) stepPlan {
	if best.empty() {
		return next
	}
	fs := []func(stepPlan) int{
		b.avoidRemoveLeader,
		b.preferUpStoreAsLeader,
		b.preferOldPeerAsLeader,
		b.preferLessLeaderTransfer1,
		b.preferLessLeaderTransfer2,
		b.preferTargetLeader,
	}
	for _, t := range fs {
		if tb, tc := t(best), t(next); tb > tc {
			return best
		} else if tb < tc {
			return next
		}
	}
	return best
}

func (b *Builder) labelMatch(p stepPlan) int {
	if p.empty() {
		return 0
	}
	sx, sy := b.cluster.GetStore(p.addOrPromote.GetStoreId()), b.cluster.GetStore(p.remove.GetStoreId())
	if sx == nil || sy == nil {
		return 0
	}
	labels := b.cluster.GetLocationLabels()
	for i, l := range labels {
		if sx.GetLabelValue(l) != sy.GetLabelValue(l) {
			return i
		}
	}
	return len(labels)
}

func b2i(b bool) int { // revive:disable-line:flag-parameter
	if b {
		return 1
	}
	return 0
}

// If remove leader directly, tikv will need to wait for raft election timeout.
func (b *Builder) avoidRemoveLeader(p stepPlan) int {
	return b2i(p.remove == nil || p.leader2 != p.remove.GetStoreId())
}

// Avoid generating snapshots from offline stores.
func (b *Builder) preferUpStoreAsLeader(p stepPlan) int {
	if p.addOrPromote != nil && b.currentPeers.Get(p.addOrPromote.GetStoreId()) == nil {
		store := b.cluster.GetStore(p.leader1)
		return b2i(store != nil && store.IsUp())
	}
	return 1
}

// Newly created peer may reject the leader. See https://github.com/tikv/tikv/issues/3819
func (b *Builder) preferOldPeerAsLeader(p stepPlan) int {
	return -(b.peerAddStep[p.leader1] + b.peerAddStep[p.leader2])
}

// It is better to avoid transferring leader before adding/promoting a peer.
func (b *Builder) preferLessLeaderTransfer1(p stepPlan) int {
	return b2i(p.leader1 == b.currentLeader)
}

// It is better to avoid transferring leader before removing a peer.
func (b *Builder) preferLessLeaderTransfer2(p stepPlan) int {
	return b2i(p.leader1 == 0 && p.leader2 == b.currentLeader || p.leader1 != 0 && p.leader2 == p.leader1)
}

// It is better to transfer leader to the target leader.
func (b *Builder) preferTargetLeader(p stepPlan) int {
	return b2i(p.leader2 != 0 && p.leader2 == b.targetLeader || p.leader2 == 0 && p.leader1 == b.targetLeader)
}

// Peers indexed by storeID.
type peersMap struct {
	m map[uint64]*metapb.Peer
}

func (pm *peersMap) Len() int {
	return len(pm.m)
}

func (pm *peersMap) Get(id uint64) *metapb.Peer {
	return pm.m[id]
}

// IDs is used for iteration in order.
func (pm *peersMap) IDs() []uint64 {
	ids := make([]uint64, 0, len(pm.m))
	for id := range pm.m {
		ids = append(ids, id)
	}
	sort.Sort(u64Slice(ids))
	return ids
}

func (pm *peersMap) Set(p *metapb.Peer) {
	if pm.m == nil {
		pm.m = make(map[uint64]*metapb.Peer)
	}
	pm.m[p.GetStoreId()] = p
}

func (pm *peersMap) Delete(id uint64) {
	delete(pm.m, id)
}

func (pm peersMap) String() string {
	ids := make([]uint64, 0, len(pm.m))
	for _, p := range pm.m {
		ids = append(ids, p.GetStoreId())
	}
	return fmt.Sprintf("%v", ids)
}

func (pm *peersMap) Copy() (pm2 peersMap) {
	for _, p := range pm.m {
		pm2.Set(p)
	}
	return
}
