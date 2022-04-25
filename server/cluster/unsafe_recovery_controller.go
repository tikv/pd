// Copyright 2021 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/btree"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
)

type unsafeRecoveryStage int

const (
	storeRequestInterval = time.Second * 60
)

// Stage transition graph: for more details, please check `unsafeRecoveryController.HandleStoreHeartbeat()`
//   +-----------+
//   |           |
//   |   idle    |
//   |           |
//   +-----------+
//         |
//         |
//         |
//         v
//   +-----------+             +-----------+               +-----------+
//   |           |------------>|           |               |           |
//   |  collect  |             |  force    |               |  failed   |
//   |  report   |      +------|  leader   |-------+------>|           |
//   |           |      |      |           |       |       +-----------+
//   +-----------+      |      +-----------+       |
//                      |         |     ^          |
//                      |         |     |          |
//                      |         |     |          |
//                      |         v     |          |
//                      |      +-----------+       |
//                      |      |           |       |
//                      |      |  demote   |       |
//                      +------|  voter    |-------+
//                      |      |           |       |
//                      |      +-----------+       |
//                      |         |     ^          |
//                      |         |     |          |
//                      |         v     |          |
//                      |      +-----------+       |
//   +-----------+      |      |           |       |
//   |           |      |      |  create   |       |
//   | finished  |      |      |  region   |-------+
//   |           |<-----+------|           |
//   +-----------+             +-----------+
//
const (
	idle unsafeRecoveryStage = iota
	collectReport
	forceLeader
	demoteFailedVoter
	createEmptyRegion
	finished
	failed
)

type unsafeRecoveryController struct {
	sync.RWMutex

	cluster      *RaftCluster
	stage        unsafeRecoveryStage
	failedStores map[uint64]interface{}

	// collected reports from store, if not reported yet, it would be nil
	storeReports      map[uint64]*pdpb.StoreReport
	numStoresReported int

	storePlanExpires   map[uint64]time.Time
	storeRecoveryPlans map[uint64]*pdpb.RecoveryPlan

	output []string
	err    error
}

func newUnsafeRecoveryController(cluster *RaftCluster) *unsafeRecoveryController {
	return &unsafeRecoveryController{
		cluster:            cluster,
		stage:              idle,
		failedStores:       make(map[uint64]interface{}),
		storeReports:       make(map[uint64]*pdpb.StoreReport),
		numStoresReported:  0,
		storePlanExpires:   make(map[uint64]time.Time),
		storeRecoveryPlans: make(map[uint64]*pdpb.RecoveryPlan),
		output:             make([]string, 0),
		err:                nil,
	}
}

func (u *unsafeRecoveryController) reset() {
	u.stage = idle
	u.failedStores = make(map[uint64]interface{})
	u.storeReports = make(map[uint64]*pdpb.StoreReport)
	u.numStoresReported = 0
	u.storePlanExpires = make(map[uint64]time.Time)
	u.storeRecoveryPlans = make(map[uint64]*pdpb.RecoveryPlan)
	u.output = make([]string, 0)
	u.err = nil
}

// RemoveFailedStores removes failed stores from the cluster.
func (u *unsafeRecoveryController) RemoveFailedStores(failedStores map[uint64]interface{}) error {
	u.Lock()
	defer u.Unlock()

	if u.stage != idle {
		return errors.Errorf("Another request is working in progress")
	}

	// validate the stores and mark the store as tombstone forcibly
	for failedStore := range failedStores {
		store := u.cluster.GetStore(failedStore)
		if store == nil {
			return errors.Errorf("Store %v doesn't exist", failedStore)
		} else if (store.IsPreparing() || store.IsServing()) && !store.IsDisconnected() {
			return errors.Errorf("Store %v is up and connected", failedStore)
		}
	}
	for failedStore := range failedStores {
		err := u.cluster.BuryStore(failedStore, true)
		if err != nil && errors.ErrorNotEqual(err, errs.ErrStoreNotFound.FastGenByArgs(failedStore)) {
			return err
		}
	}

	u.reset()
	for _, s := range u.cluster.GetStores() {
		// TODO: check TiFlash
		if s.IsRemoved() || s.IsPhysicallyDestroyed() || core.IsStoreContainLabel(s.GetMeta(), core.EngineKey, core.EngineTiFlash) {
			continue
		}
		if _, exists := failedStores[s.GetID()]; exists {
			continue
		}
		u.storeReports[s.GetID()] = nil
		// generate empty plan to request peer report
		u.getRecoveryPlan(s.GetID())
	}

	u.failedStores = failedStores
	u.changeStage(collectReport)
	return nil
}

// HandleStoreHeartbeat handles the store heartbeat requests and checks whether the stores need to
// send detailed report back.
func (u *unsafeRecoveryController) HandleStoreHeartbeat(heartbeat *pdpb.StoreHeartbeatRequest, resp *pdpb.StoreHeartbeatResponse) {
	u.Lock()
	defer u.Unlock()

	if u.stage == idle || u.stage == finished || u.stage == failed {
		// no recovery in progress, do nothing
		return
	}

	allCollected := false
	allCollected, u.err = u.collectReport(heartbeat, resp)
	if u.err != nil {
		u.changeStage(failed)
		return
	}

	if allCollected {
		hasPlan := true
		switch u.stage {
		case collectReport:
			if u.generateForceLeaderPlan() {
				u.changeStage(forceLeader)
			} else if u.generateCreateEmptyRegionPlan() {
				u.changeStage(createEmptyRegion)
			} else {
				hasPlan = false
			}
		case forceLeader:
			if u.generateDemoteFailedVoterPlan() {
				u.changeStage(demoteFailedVoter)
			} else if u.generateCreateEmptyRegionPlan() {
				u.changeStage(createEmptyRegion)
			} else {
				hasPlan = false
			}
		case demoteFailedVoter:
			// may still have plan to do, recheck again
			if u.generateForceLeaderPlan() {
				u.changeStage(forceLeader)
			} else if u.generateDemoteFailedVoterPlan() {
				u.changeStage(demoteFailedVoter)
			} else if u.generateCreateEmptyRegionPlan() {
				u.changeStage(createEmptyRegion)
			} else {
				hasPlan = false
			}
		case createEmptyRegion:
			if u.generateCreateEmptyRegionPlan() {
				u.changeStage(createEmptyRegion)
			} else {
				hasPlan = false
			}
		default:
			panic("unreachable")
		}

		if !hasPlan {
			if u.err == nil {
				u.changeStage(finished)
			} else {
				u.changeStage(failed)
			}
			return
		}
	}

	u.dispatchPlan(heartbeat, resp)
}

/// It dispatches recovery plan if any.
func (u *unsafeRecoveryController) dispatchPlan(heartbeat *pdpb.StoreHeartbeatRequest, resp *pdpb.StoreHeartbeatResponse) {
	storeID := heartbeat.Stats.StoreId
	now := time.Now()

	if expire, requested := u.storePlanExpires[storeID]; !requested || expire.Before(now) {
		if requested {
			log.Info(fmt.Sprintf("Unsafe Recovery store %d recovery plan execution timeout, retry", storeID))
		}
		// Dispatch the recovery plan to the store, and the plan may be empty.
		resp.RecoveryPlan = u.getRecoveryPlan(storeID)
		u.storePlanExpires[storeID] = now.Add(storeRequestInterval)
	}
}

// It collects and checks if store reports have been fully collected.
func (u *unsafeRecoveryController) collectReport(heartbeat *pdpb.StoreHeartbeatRequest, resp *pdpb.StoreHeartbeatResponse) (bool, error) {
	if heartbeat.StoreReport == nil {
		return false, nil
	}

	storeID := heartbeat.Stats.StoreId
	if _, isFailedStore := u.failedStores[storeID]; isFailedStore {
		return false, errors.Errorf("Receive heartbeat from failed store %d", storeID)
	}

	if report, exists := u.storeReports[storeID]; exists {
		// if receive duplicated report from the same TiKV, use the latest one
		u.storeReports[storeID] = heartbeat.StoreReport
		if report == nil {
			u.numStoresReported++
			if u.numStoresReported == len(u.storeReports) {
				return true, nil
			}
		}
	}

	return false, nil
}

func (u *unsafeRecoveryController) GetStage() unsafeRecoveryStage {
	u.RLock()
	defer u.RUnlock()
	return u.stage
}

func (u *unsafeRecoveryController) changeStage(stage unsafeRecoveryStage) {
	u.stage = stage

	var output []string
	switch u.stage {
	case idle:
	case collectReport:
		stores := ""
		count := 0
		for store := range u.failedStores {
			count += 1
			stores += fmt.Sprintf("%d", store)
			if count != len(u.failedStores) {
				stores += ", "
			}
		}
		output = append(output, fmt.Sprintf("Unsafe recovery enters collect report stage: failed stores %s", stores))
	case forceLeader:
		output = append(output, "Unsafe recovery enters force leader stage")
		output = append(output, u.getForceLeaderPlanDigest()...)
	case demoteFailedVoter:
		output = append(output, "Unsafe recovery enters demote failed voter stage")
		output = append(output, u.getDemoteFailedVoterPlanDigest()...)
	case createEmptyRegion:
		output = append(output, "Unsafe recovery enters create empty region stage")
		output = append(output, u.getCreateEmptyRegionPlanDigest()...)
	case finished:
		output = append(output, "Unsafe recovery finished")
	case failed:
		output = append(output, fmt.Sprintf("Unsafe recovery failed: %v", u.err))
	}

	u.output = append(u.output, output...)
	for _, o := range output {
		log.Info(o)
	}

	// reset store reports to nil instead of delete, because it relays on the item
	// to decide which store it needs to collect the report from.
	for k := range u.storeReports {
		u.storeReports[k] = nil
	}
	u.numStoresReported = 0
}

func (u *unsafeRecoveryController) getForceLeaderPlanDigest() []string {
	var output []string
	for storeID, plan := range u.storeRecoveryPlans {
		forceLeaders := plan.GetForceLeader()
		if forceLeaders == nil {
			continue
		}
		regions := ""
		for i, regionID := range forceLeaders.GetEnterForceLeaders() {
			regions += fmt.Sprintf("%d", regionID)
			if i != len(forceLeaders.GetEnterForceLeaders())-1 {
				regions += ", "
			}
		}
		output = append(output, fmt.Sprintf(" - store %d", storeID))
		output = append(output, fmt.Sprintf("   - force leader on regions: %s", regions))
	}
	return output
}

func (u *unsafeRecoveryController) getDemoteFailedVoterPlanDigest() []string {
	var output []string
	for storeID, plan := range u.storeRecoveryPlans {
		if plan.GetDemotes() == nil {
			continue
		}
		output = append(output, fmt.Sprintf(" - store %d", storeID))
		for _, demote := range plan.GetDemotes() {
			peers := ""
			for _, peer := range demote.GetFailedVoters() {
				peers += fmt.Sprintf("{%v}", peer)
				if peer != demote.GetFailedVoters()[len(demote.GetFailedVoters())-1] {
					peers += ", "
				}
			}
			output = append(output, fmt.Sprintf("   - region %d demotes peers %s", demote.GetRegionId(), peers))
		}
		for _, tombstone := range plan.GetTombstones() {
			output = append(output, fmt.Sprintf("   - tombstone the peer of region %d", tombstone))
		}
	}
	return output
}

func (u *unsafeRecoveryController) getCreateEmptyRegionPlanDigest() []string {
	var output []string
	for storeID, plan := range u.storeRecoveryPlans {
		if plan.GetCreates() == nil {
			continue
		}
		output = append(output, fmt.Sprintf(" - store %d", storeID))
		for _, region := range plan.GetCreates() {
			output = append(output, fmt.Sprintf("   - create region %v", core.RegionToHexMeta(region)))
		}
	}
	return output
}

func (u *unsafeRecoveryController) canElectLeader(region *metapb.Region) bool {
	hasQuorum := func(voters []*metapb.Peer) bool {
		numFailedVoters := 0
		numLiveVoters := 0

		for _, peer := range region.Peers {
			if peer.Role == metapb.PeerRole_Learner {
				continue
			}
			if _, ok := u.failedStores[peer.StoreId]; ok {
				numFailedVoters += 1
			} else {
				numLiveVoters += 1
			}
		}
		return numFailedVoters < numLiveVoters
	}

	// consider joint consensus
	var incomingVoters []*metapb.Peer
	var outgoingVoters []*metapb.Peer

	for _, peer := range region.Peers {
		if peer.Role == metapb.PeerRole_Learner {
			continue
		}
		if peer.Role == metapb.PeerRole_Voter || peer.Role == metapb.PeerRole_IncomingVoter {
			incomingVoters = append(incomingVoters, peer)
		} else if peer.Role == metapb.PeerRole_Voter || peer.Role == metapb.PeerRole_DemotingVoter {
			outgoingVoters = append(outgoingVoters, peer)
		}
	}

	return hasQuorum(incomingVoters) && hasQuorum(outgoingVoters)
}

func (u *unsafeRecoveryController) getFailedPeers(region *metapb.Region) []*metapb.Peer {
	var failedPeers []*metapb.Peer
	for _, peer := range region.Peers {
		if _, ok := u.failedStores[peer.StoreId]; ok {
			failedPeers = append(failedPeers, peer)
		}
	}
	return failedPeers
}

var _ btree.Item = &regionItem{}

type regionItem struct {
	report  *pdpb.PeerReport
	storeID uint64
}

// TODO!!!!!!!!!!!!
// 5. make region tree generic
// 6. consider Tiflash
// 7. consider commit merge
// 8. add test cases
// 9. [DONE] add abort

// Less returns true if the region start key is less than the other.
func (r *regionItem) Less(other btree.Item) bool {
	left := r.Region().GetStartKey()
	right := other.(*regionItem).Region().GetStartKey()
	return bytes.Compare(left, right) < 0
}

func (r *regionItem) Contains(key []byte) bool {
	start, end := r.Region().GetStartKey(), r.Region().GetEndKey()
	return bytes.Compare(key, start) >= 0 && (len(end) == 0 || bytes.Compare(key, end) < 0)
}

func (r *regionItem) Region() *metapb.Region {
	return r.report.GetRegionState().GetRegion()
}

func (r *regionItem) IsEpochStale(other *regionItem) bool {
	re := r.Region().GetRegionEpoch()
	oe := other.Region().GetRegionEpoch()
	return re.GetVersion() < oe.GetVersion() || re.GetConfVer() < oe.GetConfVer()
}

func (r *regionItem) IsStale(origin *regionItem) bool {
	if r.Region().GetId() != origin.Region().GetId() {
		panic("should compare peers of same region")
	}

	// compare region epoch, last log term, last log index and commit index in order
	if r.IsEpochStale(origin) {
		return true
	}
	re := r.Region().GetRegionEpoch()
	oe := origin.Region().GetRegionEpoch()
	if re.GetVersion() == oe.GetVersion() && re.GetConfVer() == oe.GetConfVer() {
		return r.IsRaftStale(origin)
	}
	return false
}

func (r *regionItem) IsRaftStale(origin *regionItem) bool {
	rs := r.report.GetRaftState()
	os := origin.report.GetRaftState()
	if rs.GetHardState().GetTerm() < os.GetHardState().GetTerm() {
		return true
	} else if rs.GetHardState().GetTerm() == os.GetHardState().GetTerm() {
		if rs.GetLastIndex() < os.GetLastIndex() {
			return true
		} else if rs.GetLastIndex() == os.GetLastIndex() {
			return rs.GetHardState().GetCommit() < os.GetHardState().GetCommit()
		}
	}
	return false
}

const (
	defaultBTreeDegree = 64
)

type regionTree struct {
	regions map[uint64]*regionItem
	tree    *btree.BTree
}

func newRegionTree() *regionTree {
	return &regionTree{
		regions: make(map[uint64]*regionItem),
		tree:    btree.New(defaultBTreeDegree),
	}
}

func (t *regionTree) contains(regionID uint64) bool {
	_, ok := t.regions[regionID]
	return ok
}

// getOverlaps gets the regions which are overlapped with the specified region range.
func (t *regionTree) getOverlaps(item *regionItem) []*regionItem {
	// note that find() gets the last item that is less or equal than the region.
	// in the case: |_______a_______|_____b_____|___c___|
	// new region is     |______d______|
	// find() will return regionItem of region_a
	// and both startKey of region_a and region_b are less than endKey of region_d,
	// thus they are regarded as overlapped regions.
	result := t.find(item)
	if result == nil {
		result = item
	}

	end := item.Region().GetEndKey()
	var overlaps []*regionItem
	t.tree.AscendGreaterOrEqual(result, func(i btree.Item) bool {
		over := i.(*regionItem)
		if len(end) > 0 && bytes.Compare(end, over.Region().GetStartKey()) <= 0 {
			return false
		}
		overlaps = append(overlaps, over)
		return true
	})
	return overlaps
}

// find is a helper function to find an item that contains the regions start key.
func (t *regionTree) find(item *regionItem) *regionItem {
	var result *regionItem
	t.tree.DescendLessOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem)
		return false
	})

	if result == nil || !result.Contains(item.Region().GetStartKey()) {
		return nil
	}

	return result
}

// update updates the tree with the region.
// It finds and deletes all the overlapped regions first, and then
// insert the new region.
func (t *regionTree) update(item *regionItem) bool {
	overlaps := t.getOverlaps(item)

	foundSelf := false
	for _, old := range overlaps {
		if item.Region().GetId() == old.Region().GetId() {
			foundSelf = true
			// not only check epoch for peers of the same region
			if item.IsStale(old) {
				return false
			}
		} else if item.IsEpochStale(old) {
			return false
		}
	}

	if !foundSelf {
		// the ranges are not overlapped before and after, so use hash map
		// to get the origin region info
		if origin := t.regions[item.Region().GetId()]; origin != nil {
			if item.IsStale(origin) {
				return false
			}
			if origin.IsStale(item) {
				t.tree.Delete(origin)
			}
		}
	}
	for _, old := range overlaps {
		t.tree.Delete(old)
	}
	t.regions[item.Region().GetId()] = item
	t.tree.ReplaceOrInsert(item)
	return true
}

func (u *unsafeRecoveryController) getRecoveryPlan(storeID uint64) *pdpb.RecoveryPlan {
	if _, exists := u.storeRecoveryPlans[storeID]; !exists {
		u.storeRecoveryPlans[storeID] = &pdpb.RecoveryPlan{}
	}
	return u.storeRecoveryPlans[storeID]
}

func (u *unsafeRecoveryController) buildUpFromReports() (*regionTree, map[uint64][]*regionItem) {
	// clean up previous plan
	u.storePlanExpires = make(map[uint64]time.Time)
	u.storeRecoveryPlans = make(map[uint64]*pdpb.RecoveryPlan)

	newestRegionTree := newRegionTree()
	peersMap := make(map[uint64][]*regionItem)
	// Go through all the peer reports to build up the newest region tree
	for storeID, storeReport := range u.storeReports {
		for _, peerReport := range storeReport.PeerReports {
			item := &regionItem{report: peerReport, storeID: storeID}
			peersMap[item.Region().GetId()] = append(peersMap[item.Region().GetId()], item)
			newestRegionTree.update(item)
		}
	}
	return newestRegionTree, peersMap
}

func (u *unsafeRecoveryController) generateForceLeaderPlan() bool {
	newestRegionTree, peersMap := u.buildUpFromReports()
	hasPlan := false

	selectLeader := func(peersMap map[uint64][]*regionItem, region *metapb.Region) *regionItem {
		var leader *regionItem
		for _, peer := range peersMap[region.GetId()] {
			if leader == nil || leader.IsRaftStale(peer) {
				leader = peer
			}
		}
		return leader
	}

	// Check the regions in newest Region Tree to see if it can still elect leader
	// considering the failed stores
	newestRegionTree.tree.Ascend(func(item btree.Item) bool {
		region := item.(*regionItem).Region()
		if !u.canElectLeader(region) {
			// the peer with largest log index/term may have lower commit/apply index, namely, lower epoch version
			// so find which peer should to be the leader instead of using peer info in the region tree.
			leader := selectLeader(peersMap, region)
			storeRecoveryPlan := u.getRecoveryPlan(leader.storeID)
			if storeRecoveryPlan.ForceLeader == nil {
				storeRecoveryPlan.ForceLeader = &pdpb.ForceLeader{}
				for store := range u.failedStores {
					storeRecoveryPlan.ForceLeader.FailedStores = append(storeRecoveryPlan.ForceLeader.FailedStores, store)
				}
			}
			storeRecoveryPlan.ForceLeader.EnterForceLeaders = append(storeRecoveryPlan.ForceLeader.EnterForceLeaders, region.GetId())
			hasPlan = true
		}
		return true
	})

	// TODO: need to resolve the case 2
	// it's hard to distinguish it with unfinished split region
	// and it's rare, so won't do it now

	return hasPlan
}

func (u *unsafeRecoveryController) generateDemoteFailedVoterPlan() bool {
	newestRegionTree, peersMap := u.buildUpFromReports()
	hasPlan := false

	findForceLeader := func(peersMap map[uint64][]*regionItem, region *metapb.Region) *regionItem {
		var leader *regionItem
		for _, peer := range peersMap[region.GetId()] {
			if peer.report.IsForceLeader {
				leader = peer
				break
			}
		}
		return leader
	}

	// Check the regions in newest Region Tree to see if it can still elect leader
	// considering the failed stores
	newestRegionTree.tree.Ascend(func(item btree.Item) bool {
		region := item.(*regionItem).Region()
		if !u.canElectLeader(region) {
			leader := findForceLeader(peersMap, region)
			if leader == nil {
				// can't find the force leader, maybe a newly split region, skip
				return true
			}
			storeRecoveryPlan := u.getRecoveryPlan(leader.storeID)
			storeRecoveryPlan.Demotes = append(storeRecoveryPlan.Demotes,
				&pdpb.DemoteFailedVoters{
					RegionId:     region.GetId(),
					FailedVoters: u.getFailedPeers(leader.Region()),
				},
			)
			hasPlan = true
		}
		return true
	})

	// Tombstone the peers of region not presented in the newest region tree
	for storeID, storeReport := range u.storeReports {
		for _, peerReport := range storeReport.PeerReports {
			regionID := peerReport.GetRegionState().Region.Id
			if !newestRegionTree.contains(regionID) {
				if u.canElectLeader(peerReport.GetRegionState().Region) {
					// find invalid peer but it has quorum
					continue
				} else {
					// the peer is not in the valid regions, should be deleted directly
					storeRecoveryPlan := u.getRecoveryPlan(storeID)
					storeRecoveryPlan.Tombstones = append(storeRecoveryPlan.Tombstones, regionID)
				}
			}
		}
	}
	return hasPlan
}

func (u *unsafeRecoveryController) generateCreateEmptyRegionPlan() bool {
	newestRegionTree, _ := u.buildUpFromReports()
	hasPlan := false

	createRegion := func(startKey, endKey []byte, storeID uint64) (*metapb.Region, error) {
		regionID, err := u.cluster.GetAllocator().Alloc()
		if err != nil {
			return nil, err
		}
		peerID, err := u.cluster.GetAllocator().Alloc()
		if err != nil {
			return nil, err
		}
		return &metapb.Region{
			Id:          regionID,
			StartKey:    startKey,
			EndKey:      endKey,
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:       []*metapb.Peer{{Id: peerID, StoreId: storeID, Role: metapb.PeerRole_Voter}},
		}, nil
	}

	// There may be ranges that are covered by no one. Find these empty ranges, create new
	// regions that cover them and evenly distribute newly created regions among all stores.
	lastEnd := []byte("")
	var lastStoreID uint64
	newestRegionTree.tree.Ascend(func(item btree.Item) bool {
		region := item.(*regionItem).Region()
		storeID := item.(*regionItem).storeID
		if !bytes.Equal(region.StartKey, lastEnd) {
			newRegion, err := createRegion(lastEnd, region.StartKey, storeID)
			if err != nil {
				u.err = err
				return false
			}
			storeRecoveryPlan := u.getRecoveryPlan(storeID)
			storeRecoveryPlan.Creates = append(storeRecoveryPlan.Creates, newRegion)
			hasPlan = true
		}
		lastEnd = region.EndKey
		lastStoreID = storeID
		return true
	})
	if u.err != nil {
		return false
	}
	if !bytes.Equal(lastEnd, []byte("")) {
		newRegion, err := createRegion(lastEnd, []byte(""), lastStoreID)
		if err != nil {
			u.err = err
			return false
		}
		storeRecoveryPlan := u.getRecoveryPlan(lastStoreID)
		storeRecoveryPlan.Creates = append(storeRecoveryPlan.Creates, newRegion)
		hasPlan = true
	}
	return hasPlan
}

func (u *unsafeRecoveryController) getReportStatus() []string {
	var status []string
	if u.numStoresReported != len(u.storeReports) {
		status = append(status, fmt.Sprintf("Collecting reports from alive stores(%d/%d):", u.numStoresReported, len(u.storeReports)))
		var reported, unreported, undispatched string
		for storeID, report := range u.storeReports {
			if report == nil {
				if _, requested := u.storePlanExpires[storeID]; !requested {
					undispatched += strconv.FormatUint(storeID, 10) + ", "
				} else {
					unreported += strconv.FormatUint(storeID, 10) + ", "
				}
			} else {
				reported += strconv.FormatUint(storeID, 10) + ", "
			}
		}
		status = append(status, " - Stores that have not dispatched plan: "+undispatched)
		status = append(status, " - Stores that have reported to PD: "+reported)
		status = append(status, " - Stores that have not reported to PD: "+unreported)
	} else {
		status = append(status, fmt.Sprintf("Collected reports from all %d alive stores", len(u.storeReports)))
	}
	return status
}

// Show returns the current status of ongoing unsafe recover operation.
func (u *unsafeRecoveryController) Show() []string {
	u.RLock()
	defer u.RUnlock()

	if u.stage == idle {
		return []string{"No on-going recovery."}
	}
	status := u.output
	if u.stage != finished {
		status = append(status, u.getReportStatus()...)
	}
	return status
}

// History returns the history logs of the current unsafe recover operation.
func (u *unsafeRecoveryController) History() []string {
	u.RLock()
	defer u.RUnlock()
	return []string{""}
}
