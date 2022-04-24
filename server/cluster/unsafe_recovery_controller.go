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

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/btree"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"go.uber.org/zap"
)

type unsafeRecoveryStage int

const (
	storeRequestInterval = time.Second * 60
)

const (
	idle unsafeRecoveryStage = iota
	collectReport
	forceLeader
	removePeer
	createEmptyRegion
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

	executionResults map[uint64]bool              // Execution results for tracking purpose
	executionReports map[uint64]*pdpb.StoreReport // Execution reports for tracking purpose
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
		executionResults:   make(map[uint64]bool),
		executionReports:   make(map[uint64]*pdpb.StoreReport),
	}
}

func (u *unsafeRecoveryController) reset() {
	u.stage = idle
	u.failedStores = make(map[uint64]interface{})
	u.storeReports = make(map[uint64]*pdpb.StoreReport)
	u.numStoresReported = 0
	u.storePlanExpires = make(map[uint64]time.Time)
	u.storeRecoveryPlans = make(map[uint64]*pdpb.RecoveryPlan)
	u.executionResults = make(map[uint64]bool)
	u.executionReports = make(map[uint64]*pdpb.StoreReport)
}

func (u *unsafeRecoveryController) log(format string, a ...any) {
	msg := fmt.Sprintf(format, a...)
	u.output = append(u.output, msg)
	log.Info(msg)
}

// RemoveFailedStores removes failed stores from the cluster.
func (u *unsafeRecoveryController) RemoveFailedStores(failedStores map[uint64]interface{}) error {
	u.Lock()
	defer u.Unlock()

	if len(failedStores) == 0 {
		return errors.Errorf("No store specified")
	}
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

	u.log("Unsafe recovery, remove failed stores: %v", failedStores)
	u.failedStores = failedStores
	u.stage = collectReport
	return nil
}

// HandleStoreHeartbeat handles the store heartbeat requests and checks whether the stores need to
// send detailed report back.
func (u *unsafeRecoveryController) HandleStoreHeartbeat(heartbeat *pdpb.StoreHeartbeatRequest, resp *pdpb.StoreHeartbeatResponse) {
	u.Lock()
	defer u.Unlock()

	switch u.stage {
	case idle:
		// no recovery in progress, do nothing
		return
	case collectReport:
		if u.collectReport(heartbeat, resp) {
			if !u.generateForceLeaderPlan() {
				u.finishRecovery()
				return
			} else {
				u.changeStage(forceLeader)
			}
		}
		u.dispatchPlan(heartbeat, resp)
	case forceLeader:
		if u.collectReport(heartbeat, resp) {
			if !u.generateRemovePeerPlan() {
				u.finishRecovery()
				return
			} else {
				u.changeStage(removePeer)
			}
		}
		u.dispatchPlan(heartbeat, resp)
	case removePeer:
		if u.collectReport(heartbeat, resp) {
			// may still have plan to do, recheck again
			if u.generateForceLeaderPlan() {
				u.changeStage(forceLeader)
			} else if u.generateRemovePeerPlan() {
				u.changeStage(removePeer)
			} else if u.generateCreateEmptyRegionPlan() {
				u.changeStage(createEmptyRegion)
			} else {
				u.finishRecovery()
				return
			}
		}
		u.dispatchPlan(heartbeat, resp)
	case createEmptyRegion:
		if u.collectReport(heartbeat, resp) {
			if u.generateCreateEmptyRegionPlan() {
				u.changeStage(createEmptyRegion)
			} else {
				u.finishRecovery()
				return
			}
		}
		u.dispatchPlan(heartbeat, resp)
	}
}

/// It dispatches recovery plan if any.
func (u *unsafeRecoveryController) dispatchPlan(heartbeat *pdpb.StoreHeartbeatRequest, resp *pdpb.StoreHeartbeatResponse) {
	// Lock should be held
	storeID := heartbeat.Stats.StoreId
	now := time.Now()

	if expire, requested := u.storePlanExpires[storeID]; !requested || expire.Before(now) {
		// Dispatch the recovery plan to the store, and the plan may be empty.
		resp.RecoveryPlan = u.getRecoveryPlan(storeID)
		u.storePlanExpires[storeID] = now.Add(storeRequestInterval)
	}
}

// It collects and checks if store reports have been fully collected.
func (u *unsafeRecoveryController) collectReport(heartbeat *pdpb.StoreHeartbeatRequest, resp *pdpb.StoreHeartbeatResponse) bool {
	// Lock should be held
	if heartbeat.StoreReport == nil {
		return false
	}

	storeID := heartbeat.Stats.StoreId
	if _, isFailedStore := u.failedStores[storeID]; isFailedStore {
		// This should be unreachable.
		// TODO: abort the process directly
		return false
	}

	if _, find := u.storeReports[storeID]; !find {
		// No need to collect the report of the store
		return false
	}

	if report, exists := u.storeReports[storeID]; exists && report == nil { // if receive duplicated report from same TiKV, just ignore
		u.storeReports[storeID] = heartbeat.StoreReport
		u.numStoresReported++
		if u.numStoresReported == len(u.storeReports) {
			return true
		}
	}

	return false
}

func (u *unsafeRecoveryController) finishRecovery() {
	log.Info("Recover finished.")
	for _, history := range u.historyLocked() {
		log.Info(history)
	}
	u.changeStage(idle)
}

func (u *unsafeRecoveryController) GetStage() unsafeRecoveryStage {
	u.Lock()
	defer u.Unlock()
	return u.stage
}

func (u *unsafeRecoveryController) changeStage(stage unsafeRecoveryStage) {
	u.stage = stage
	// reset store reports to nil instead of delete, because it relays on the item
	// to decide which store it needs to collect the report from.
	for k := range u.storeReports {
		u.storeReports[k] = nil
	}
	u.numStoresReported = 0
}

func (u *unsafeRecoveryController) canElectLeader(region *metapb.Region) bool {
	hasQuorum := func(voters []*metapb.Peer) bool {
		numFailedVoters := 0
		numLiveVoters := 0

		for _, peer := range region.Peers {
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
// 1. [DONE] make sure propose on the origin forced leader
// 2. test add apply recovery plan
// 3. construct accumulative log
// 4. [DONE] separate create empty region
// 5. make region tree generic
// 6. consider Tiflash

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
	storeRecoveryPlan, exists := u.storeRecoveryPlans[storeID]
	if !exists {
		u.storeRecoveryPlans[storeID] = &pdpb.RecoveryPlan{}
		storeRecoveryPlan = u.storeRecoveryPlans[storeID]
	}
	return storeRecoveryPlan
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

func (u *unsafeRecoveryController) generateRemovePeerPlan() bool {
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
				// can't find the force leader, skip
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

	// should be same
	for storeID, storeReport := range u.storeReports {
		for _, peerReport := range storeReport.PeerReports {
			regionID := peerReport.GetRegionState().Region.Id
			if !newestRegionTree.contains(regionID) {
				if u.canElectLeader(peerReport.GetRegionState().Region) {
					log.Warn("find invalid peer but it has quorum")
				} else {
					// the peer is not in the valid regions, should be deleted directly
					storeRecoveryPlan := u.getRecoveryPlan(storeID)
					storeRecoveryPlan.Tombstones = append(storeRecoveryPlan.Tombstones, regionID)
				}
			}
		}
	}

	log.Info("Plan generated")
	for store, plan := range u.storeRecoveryPlans {
		log.Info("Store plan", zap.String("store", strconv.FormatUint(store, 10)), zap.String("plan", proto.MarshalTextString(plan)))
	}
	return hasPlan
}

func (u *unsafeRecoveryController) generateCreateEmptyRegionPlan() bool {
	newestRegionTree, _ := u.buildUpFromReports()
	hasPlan := false

	createRegion := func(startKey, endKey []byte, storeID uint64) *metapb.Region {
		regionID, err := u.cluster.GetAllocator().Alloc()
		if err != nil {
			panic("can't alloc region id")
		}
		peerID, err := u.cluster.GetAllocator().Alloc()
		if err != nil {
			panic("can't alloc peer id")
		}
		return &metapb.Region{
			Id:          regionID,
			StartKey:    startKey,
			EndKey:      endKey,
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:       []*metapb.Peer{{Id: peerID, StoreId: storeID, Role: metapb.PeerRole_Voter}},
		}
	}

	// There may be ranges that are covered by no one. Find these empty ranges, create new
	// regions that cover them and evenly distribute newly created regions among all stores.
	lastEnd := []byte("")
	var lastStoreID uint64
	newestRegionTree.tree.Ascend(func(item btree.Item) bool {
		region := item.(*regionItem).Region()
		storeID := item.(*regionItem).storeID
		if !bytes.Equal(region.StartKey, lastEnd) {
			newRegion := createRegion(lastEnd, region.StartKey, storeID)
			storeRecoveryPlan := u.getRecoveryPlan(storeID)
			storeRecoveryPlan.Creates = append(storeRecoveryPlan.Creates, newRegion)
			hasPlan = true
		}
		lastEnd = region.EndKey
		lastStoreID = storeID
		return true
	})
	if !bytes.Equal(lastEnd, []byte("")) {
		newRegion := createRegion(lastEnd, []byte(""), lastStoreID)
		storeRecoveryPlan := u.getRecoveryPlan(lastStoreID)
		storeRecoveryPlan.Creates = append(storeRecoveryPlan.Creates, newRegion)
		hasPlan = true
	}

	log.Info("Plan generated")
	for store, plan := range u.storeRecoveryPlans {
		log.Info("Store plan", zap.String("store", strconv.FormatUint(store, 10)), zap.String("plan", proto.MarshalTextString(plan)))
	}
	return hasPlan
}

func getStoreDigest(storeReport *pdpb.StoreReport) string {
	if storeReport == nil {
		return "nil"
	}
	var result string
	for _, peerReport := range storeReport.PeerReports {
		result += core.RegionToHexMeta(peerReport.RegionState.Region).String() + ", "
	}
	return result
}

// Show returns the current status of ongoing unsafe recover operation.
func (u *unsafeRecoveryController) Show() []string {
	u.RLock()
	defer u.RUnlock()
	// switch u.stage {
	// case idle:
	// 	return []string{"No on-going operation."}
	// case collectReport:
	// 	var status []string
	// 	status = append(status, fmt.Sprintf("Collecting cluster info from all alive stores, %d/%d.", u.numStoresReported, len(u.storeReports)))
	// 	var reported, unreported string
	// 	for storeID, report := range u.storeReports {
	// 		if report == nil {
	// 			unreported += strconv.FormatUint(storeID, 10) + ","
	// 		} else {
	// 			reported += strconv.FormatUint(storeID, 10) + ","
	// 		}
	// 	}
	// 	status = append(status, "Stores that have reported to PD: "+reported)
	// 	status = append(status, "Stores that have not reported to PD: "+unreported)
	// 	return status
	// case forceLeader:
	// 	// TODO:
	// case recovering:
	// 	var status []string
	// 	status = append(status, fmt.Sprintf("Waiting for recover commands being applied, %d/%d", u.numStoresReported, len(u.storeRecoveryPlans)))
	// 	status = append(status, "Recovery plan:")
	// 	for storeID, plan := range u.storeRecoveryPlans {
	// 		planDigest := "Store " + strconv.FormatUint(storeID, 10) + ", creates: "
	// 		for _, create := range plan.Creates {
	// 			planDigest += getRegionDigest(create) + ", "
	// 		}
	// 		planDigest += "; removes: "
	// 		for _, remove := range plan.Removes {
	// 			var peers string
	// 			for _, peer := range remove.Peers {
	// 				peers += "(" + getPeerDigest(peer) + "), "
	// 			}
	// 			planDigest += fmt.Sprintf("region %d {%s}", remove.RegionId, peers) + ", "
	// 		}
	// 		planDigest += "; deletes: "
	// 		for _, deletion := range plan.Tombstones {
	// 			planDigest += strconv.FormatUint(deletion, 10) + ", "
	// 		}
	// 		status = append(status, planDigest)
	// 	}
	// 	status = append(status, "Execution progess:")
	// 	for storeID, applied := range u.executionResults {
	// 		if !applied {
	// 			status = append(status, strconv.FormatUint(storeID, 10)+"not yet applied, last report: "+getStoreDigest(u.executionReports[storeID]))
	// 		}
	// 	}
	// 	return status
	// }
	return []string{"Undefined status"}
}

// History returns the history logs of the current unsafe recover operation.
func (u *unsafeRecoveryController) History() []string {
	u.RLock()
	defer u.RUnlock()
	return u.historyLocked()
}
func (u *unsafeRecoveryController) historyLocked() []string {
	// if u.stage <= idle {
	// 	return []string{"No unsafe recover has been triggered since PD restarted."}
	// }
	// var history []string
	// if u.stage >= collectingClusterInfo {
	// 	history = append(history, "Store reports collection:")
	// 	for storeID, report := range u.storeReports {
	// 		if report == nil {
	// 			history = append(history, "Store "+strconv.FormatUint(storeID, 10)+": waiting for report.")
	// 		} else {
	// 			history = append(history, "Store "+strconv.FormatUint(storeID, 10)+": "+getStoreDigest(report))
	// 		}
	// 	}
	// }
	// if u.stage >= recovering {
	// 	history = append(history, "Recovery plan:")
	// 	for storeID, plan := range u.storeRecoveryPlans {
	// 		planDigest := "Store " + strconv.FormatUint(storeID, 10) + ", creates: "
	// 		for _, create := range plan.Creates {
	// 			planDigest += getRegionDigest(create) + ", "
	// 		}
	// 		planDigest += "; removes: "
	// 		for _, remove := range plan.Removes {
	// 			var peers string
	// 			for _, peer := range remove.Peers {
	// 				peers += "(" + getPeerDigest(peer) + "), "
	// 			}
	// 			planDigest += fmt.Sprintf("region %d {%s}", remove.RegionId, peers) + ", "
	// 		}
	// 		planDigest += "; deletes: "
	// 		for _, deletion := range plan.Tombstones {
	// 			planDigest += strconv.FormatUint(deletion, 10) + ", "
	// 		}
	// 		history = append(history, planDigest)
	// 	}
	// 	history = append(history, "Execution progress:")
	// 	for storeID, applied := range u.executionResults {
	// 		executionDigest := "Store " + strconv.FormatUint(storeID, 10)
	// 		if !applied {
	// 			executionDigest += "not yet finished, "
	// 		} else {
	// 			executionDigest += "finished, "
	// 		}
	// 		executionDigest += getStoreDigest(u.executionReports[storeID])
	// 		history = append(history, executionDigest)
	// 	}
	// }
	// return history
	return []string{"Undefined status"}
}
