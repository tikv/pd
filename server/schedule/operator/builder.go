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
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule/filter"
	"github.com/pingcap/pd/v4/server/schedule/placement"
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
	rules       []*placement.Rule

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

	var rules []*placement.Rule
	if cluster.IsPlacementRulesEnabled() {
		fit := cluster.FitRegion(region)
		for _, rf := range fit.RuleFits {
			rules = append(rules, rf.Rule)
		}
		if len(rules) == 0 {
			err = errors.Errorf("cannot build operator for region match no placement rule")
		}
	}

	return &Builder{
		desc:         desc,
		cluster:      cluster,
		regionID:     region.GetID(),
		regionEpoch:  region.GetRegionEpoch(),
		rules:        rules,
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
//
// If peer's ID is 0, the builder will allocate a new ID later. If current
// target leader does not exist in peers, it will be reset.
func (b *Builder) SetPeers(peers map[uint64]*metapb.Peer) *Builder {
	if b.err != nil {
		return b
	}
	b.targetPeers = peersMap{}
	for k, p := range peers {
		if p.GetStoreId() != k {
			b.err = errors.Errorf("setPeers with mismatch storeID: %v", peers)
			return b
		}
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

// Initialize intermediate states.
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
				id, err := b.cluster.AllocID()
				if err != nil {
					return "", err
				}
				n.Id = id
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
	case b.toPromote.Len() > 0:
		return fmt.Sprintf("promote peer: store %s", b.toPromote)
	case b.targetLeader != b.originLeader:
		return fmt.Sprintf("transfer leader: store %d to %d", b.originLeader, b.targetLeader)
	}
	return ""
}

func (b *Builder) buildSteps(kind OpKind) (OpKind, error) {
	for b.toAdd.Len() > 0 || b.toRemove.Len() > 0 || b.toPromote.Len() > 0 {
		plan := b.peerPlan()
		if plan.empty() {
			return kind, errors.New("fail to build operator: plan is empty, maybe no valid leader")
		}
		if plan.leaderAdd != 0 && plan.leaderAdd != b.currentLeader {
			b.execTransferLeader(plan.leaderAdd)
			kind |= OpLeader
		}
		if plan.add != nil {
			b.execAddPeer(plan.add)
			kind |= OpRegion
		}
		if plan.promote != nil {
			b.execPromoteLearner(plan.promote)
		}
		if plan.leaderRemove != 0 && plan.leaderRemove != b.currentLeader {
			b.execTransferLeader(plan.leaderRemove)
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
	b.steps = append(b.steps, TransferLeader{FromStore: b.currentLeader, ToStore: id})
	b.currentLeader = id
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
	}
	b.peerAddStep[p.GetStoreId()] = len(b.steps)
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
	if !stateFilter.Target(b.cluster, store) {
		return false
	}
	if len(b.rules) == 0 {
		return true
	}
	for _, r := range b.rules {
		if (r.Role == placement.Leader || r.Role == placement.Voter) &&
			placement.MatchLabelConstraints(store, r.LabelConstraints) {
			return true
		}
	}
	return false
}

// stepPlan is exec step. It can be:
// 1. add voter + remove voter.
// 2. add learner + remove learner.
// 3. add learner + promote learner + remove voter.
// 4. promote learner.
// 5. remove voter/learner.
// 6. add voter/learner.
// Plan 1-3 (replace plans) do not change voter/learner count, so they have higher priority.
type stepPlan struct {
	leaderAdd    uint64 // leader before adding peer.
	add          *metapb.Peer
	promote      *metapb.Peer
	leaderRemove uint64 // leader before removing peer.
	remove       *metapb.Peer
}

func (p stepPlan) String() string {
	return fmt.Sprintf("stepPlan{leaderAdd=%v,add={%s},promote={%s},leaderRemove=%v,remove={%s}}",
		p.leaderAdd, p.add, p.promote, p.leaderRemove, p.remove)
}

func (p stepPlan) empty() bool {
	return p.promote == nil && p.add == nil && p.remove == nil
}

func (b *Builder) peerPlan() stepPlan {
	// Replace has the highest priority because it does not change region's
	// voter/learner count.
	if p := b.planReplace(); !p.empty() {
		return p
	}
	if p := b.planPromotePeer(); !p.empty() {
		return p
	}
	if p := b.planRemovePeer(); !p.empty() {
		return p
	}
	if p := b.planAddPeer(); !p.empty() {
		return p
	}
	return stepPlan{}
}

func (b *Builder) planReplace() stepPlan {
	var best stepPlan
	// add voter + remove voter OR add learner + remove learner.
	for _, i := range b.toAdd.IDs() {
		add := b.toAdd.Get(i)
		for _, j := range b.toRemove.IDs() {
			remove := b.toRemove.Get(j)
			if remove.GetIsLearner() == add.GetIsLearner() {
				best = b.planReplaceLeaders(best, stepPlan{add: add, remove: remove})
			}
		}
	}
	// add learner + promote learner + remove voter
	for _, i := range b.toPromote.IDs() {
		promote := b.toPromote.Get(i)
		for _, j := range b.toAdd.IDs() {
			if add := b.toAdd.Get(j); add.GetIsLearner() {
				for _, k := range b.toRemove.IDs() {
					if remove := b.toRemove.Get(k); !remove.GetIsLearner() && j != k {
						best = b.planReplaceLeaders(best, stepPlan{promote: promote, add: add, remove: remove})
					}
				}
			}
		}
	}
	return best
}

func (b *Builder) planReplaceLeaders(best, next stepPlan) stepPlan {
	// Brute force all possible leader combinations to find the best plan.
	for _, leaderAdd := range b.currentPeers.IDs() {
		if !b.allowLeader(b.currentPeers.Get(leaderAdd)) {
			continue
		}
		next.leaderAdd = leaderAdd
		for _, leaderRemove := range b.currentPeers.IDs() {
			if b.allowLeader(b.currentPeers.Get(leaderRemove)) && leaderRemove != next.remove.GetStoreId() {
				next.leaderRemove = leaderRemove
				best = b.comparePlan(best, next)
			}
		}
		if next.promote != nil && b.allowLeader(next.promote) && next.promote.GetStoreId() != next.remove.GetStoreId() {
			next.leaderRemove = next.promote.GetStoreId()
			best = b.comparePlan(best, next)
		}
		if next.add != nil && b.allowLeader(next.add) && next.add.GetStoreId() != next.remove.GetStoreId() {
			next.leaderRemove = next.add.GetStoreId()
			best = b.comparePlan(best, next)
		}
	}
	return best
}

func (b *Builder) planPromotePeer() stepPlan {
	for _, i := range b.toPromote.IDs() {
		p := b.toPromote.Get(i)
		return stepPlan{promote: p}
	}
	return stepPlan{}
}

func (b *Builder) planRemovePeer() stepPlan {
	var best stepPlan
	for _, i := range b.toRemove.IDs() {
		r := b.toRemove.Get(i)
		for _, leader := range b.currentPeers.IDs() {
			if b.allowLeader(b.currentPeers.Get(leader)) && leader != r.GetStoreId() {
				best = b.comparePlan(best, stepPlan{remove: r, leaderRemove: leader})
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
				best = b.comparePlan(best, stepPlan{add: a, leaderAdd: leader})
			}
		}
	}
	return best
}

// Pick the better plan from 2 candidates.
func (b *Builder) comparePlan(best, next stepPlan) stepPlan {
	if best.empty() {
		return next
	}
	fs := []func(stepPlan) int{
		b.preferReplaceByNearest, // 1. violate it affects replica safety.
		// 2-3 affects operator execution speed.
		b.preferUpStoreAsLeader, // 2. compare to 3, it is more likely to affect execution speed.
		b.preferOldPeerAsLeader, // 3. violate it may or may not affect execution speed.
		// 4-6 are less important as they are only trying to build the
		// operator with less leader transfer steps.
		b.preferAddOrPromoteTargetLeader, // 4. it is precondition of 5 so goes first.
		b.preferTargetLeader,             // 5. it may help 6 in later steps.
		b.preferLessLeaderTransfer,       // 6. trival optimization to make the operator more tidy.
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

func (b *Builder) labelMatch(x, y uint64) int {
	sx, sy := b.cluster.GetStore(x), b.cluster.GetStore(y)
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

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// return matched label count.
func (b *Builder) preferReplaceByNearest(p stepPlan) int {
	var m int
	if p.add != nil && p.remove != nil {
		m = b.labelMatch(p.add.GetStoreId(), p.remove.GetStoreId())
		if p.promote != nil { // add learner + promote learner + remove voter
			if m2 := b.labelMatch(p.promote.GetStoreId(), p.add.GetStoreId()); m2 < m {
				return m2
			}
		}
	}
	return m
}

// Avoid generating snapshots from offline stores.
func (b *Builder) preferUpStoreAsLeader(p stepPlan) int {
	if p.add != nil {
		store := b.cluster.GetStore(p.leaderAdd)
		return b2i(store != nil && store.IsUp())
	}
	return 1
}

// Newly created peer may reject the leader. See https://github.com/tikv/tikv/issues/3819
func (b *Builder) preferOldPeerAsLeader(p stepPlan) int {
	ret := -b.peerAddStep[p.leaderAdd]
	if p.add != nil && p.add.GetStoreId() == p.leaderRemove {
		ret -= len(b.steps) + 1
	} else {
		ret -= b.peerAddStep[p.leaderRemove]
	}
	return ret
}

// It is better to avoid transferring leader.
func (b *Builder) preferLessLeaderTransfer(p stepPlan) int {
	if p.leaderAdd == 0 || p.leaderAdd == b.currentLeader {
		// 3: current == leaderAdd == leaderRemove
		// 2: current == leaderAdd != leaderRemove
		return 2 + b2i(p.leaderRemove == 0 || p.leaderRemove == b.currentLeader)
	}
	// 1: current != leaderAdd == leaderRemove
	// 0: current != leaderAdd != leaderRemove
	return b2i(p.leaderRemove == 0 || p.leaderRemove == p.leaderAdd)
}

// It is better to transfer leader to the target leader.
func (b *Builder) preferTargetLeader(p stepPlan) int {
	return b2i(p.leaderRemove != 0 && p.leaderRemove == b.targetLeader || p.leaderRemove == 0 && p.leaderAdd == b.targetLeader)
}

// It is better to add target leader as early as possible.
func (b *Builder) preferAddOrPromoteTargetLeader(p stepPlan) int {
	addTarget := p.add != nil && !p.add.GetIsLearner() && p.add.GetStoreId() == b.targetLeader
	promoteTarget := p.promote != nil && p.promote.GetStoreId() == b.targetLeader
	return b2i(addTarget || promoteTarget)
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
