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
	ready unsafeRecoveryStage = iota
	collectingClusterInfo
	forceLeader
	recovering
)

type unsafeRecoveryController struct {
	sync.RWMutex

	cluster      *RaftCluster
	stage        unsafeRecoveryStage
	failedStores map[uint64]interface{}

	storeReportExpires map[uint64]time.Time
	storeReports       map[uint64]*pdpb.StoreReport // collected reports from store, if not reported yet, it would be nil
	numStoresReported  int

	storePlanExpires   map[uint64]time.Time
	storeRecoveryPlans map[uint64]*pdpb.RecoveryPlan // StoreRecoveryPlan proto

	executionResults map[uint64]bool              // Execution results for tracking purpose
	executionReports map[uint64]*pdpb.StoreReport // Execution reports for tracking purpose
}

func newUnsafeRecoveryController(cluster *RaftCluster) *unsafeRecoveryController {
	return &unsafeRecoveryController{
		cluster:            cluster,
		stage:              ready,
		failedStores:       make(map[uint64]interface{}),
		storeReportExpires: make(map[uint64]time.Time),
		storeReports:       make(map[uint64]*pdpb.StoreReport),
		numStoresReported:  0,
		storePlanExpires:   make(map[uint64]time.Time),
		storeRecoveryPlans: make(map[uint64]*pdpb.RecoveryPlan),
		executionResults:   make(map[uint64]bool),
		executionReports:   make(map[uint64]*pdpb.StoreReport),
	}
}

func (u *unsafeRecoveryController) reset() {
	u.stage = ready
	u.failedStores = make(map[uint64]interface{})
	u.storeReportExpires = make(map[uint64]time.Time)
	u.storeReports = make(map[uint64]*pdpb.StoreReport)
	u.numStoresReported = 0
	u.storePlanExpires = make(map[uint64]time.Time)
	u.storeRecoveryPlans = make(map[uint64]*pdpb.RecoveryPlan)
	u.executionResults = make(map[uint64]bool)
	u.executionReports = make(map[uint64]*pdpb.StoreReport)
}

// RemoveFailedStores removes failed stores from the cluster.
func (u *unsafeRecoveryController) RemoveFailedStores(failedStores map[uint64]interface{}) error {
	u.Lock()
	defer u.Unlock()

	if len(failedStores) == 0 {
		return errors.Errorf("No store specified")
	}
	if u.stage != ready {
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
		if s.IsRemoved() || s.IsPhysicallyDestroyed() || core.IsStoreContainLabel(s.GetMeta(), core.EngineKey, core.EngineTiFlash) {
			continue
		}
		if _, exists := failedStores[s.GetID()]; exists {
			continue
		}
		u.storeReports[s.GetID()] = nil
	}
	u.failedStores = failedStores
	u.stage = collectingClusterInfo
	return nil
}

// HandleStoreHeartbeat handles the store heartbeat requests and checks whether the stores need to
// send detailed report back.
func (u *unsafeRecoveryController) HandleStoreHeartbeat(heartbeat *pdpb.StoreHeartbeatRequest, resp *pdpb.StoreHeartbeatResponse) {
	u.Lock()
	defer u.Unlock()

	switch u.stage {
	case ready:
		// no recovery in progress, do nothing
		return
	case collectingClusterInfo:
		if u.dispatchPlanAndCollectReport(heartbeat, resp) {
			go func() {
				if !u.generateForceLeaderPlan() {
					u.finishRecovery()
				} else {
					u.changeStage(forceLeader)
				}
			}()
		}
	case forceLeader:
		if u.dispatchPlanAndCollectReport(heartbeat, resp) {
			go func() {
				if !u.generateRecoveryPlan() {
					u.finishRecovery()
				} else {
					u.changeStage(recovering)
				}
			}()
		}
	case recovering:
		if u.dispatchPlanAndCollectReport(heartbeat, resp) {
			go func() {
				if u.generateForceLeaderPlan() {
					// still have plan to do
					u.changeStage(forceLeader)
					return
				}
				if u.generateRecoveryPlan() {
					u.changeStage(recovering)
					// still have plan to do
					return
				}
				u.finishRecovery()
			}()
		}
	}
}

// It dispatches recovery plan if any, collects and checks if store reports have been fully collected.
func (u *unsafeRecoveryController) dispatchPlanAndCollectReport(heartbeat *pdpb.StoreHeartbeatRequest, resp *pdpb.StoreHeartbeatResponse) bool {
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

	now := time.Now()
	if expire, requested := u.storePlanExpires[storeID]; !requested || expire.Before(now) {
		// Dispatch the recovery plan to the store, and the plan may be empty.
		resp.Plan = u.getRecoveryPlan(storeID)
		u.storePlanExpires[storeID] = now.Add(storeRequestInterval)
	}

	if heartbeat.StoreReport == nil {
		expire, requested := u.storeReportExpires[storeID]
		now := time.Now()
		if !requested || expire.Before(now) {
			// Inform the store to send detailed report in the next heartbeat.
			resp.RequireDetailedReport = true
			u.storeReportExpires[storeID] = now.Add(storeRequestInterval)
		}
	} else if report, exists := u.storeReports[storeID]; exists && report == nil {
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
	for _, history := range u.History() {
		log.Info(history)
	}
	u.changeStage(ready)
}

func (u *unsafeRecoveryController) changeStage(stage unsafeRecoveryStage) {
	u.Lock()
	defer u.Unlock()

	u.stage = stage
	u.storePlanExpires = make(map[uint64]time.Time)
	u.storeRecoveryPlans = make(map[uint64]*pdpb.RecoveryPlan)
	u.storeReportExpires = make(map[uint64]time.Time)
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

	// compare region epoch, commit index, term and last index in order
	if r.IsEpochStale(origin) {
		return true
	}
	re := r.Region().GetRegionEpoch()
	oe := origin.Region().GetRegionEpoch()
	if re.GetVersion() == oe.GetVersion() && re.GetConfVer() == oe.GetConfVer() {
		rs := r.report.GetRaftState()
		os := origin.report.GetRaftState()
		if rs.GetHardState().GetCommit() < os.GetHardState().GetCommit() {
			return true
		} else if rs.GetHardState().GetCommit() == os.GetHardState().GetCommit() {
			if rs.GetHardState().GetTerm() < os.GetHardState().GetTerm() {
				return true
			} else if rs.GetHardState().GetTerm() == os.GetHardState().GetTerm() {
				if rs.GetLastIndex() < os.GetLastIndex() {
					return true
				}
			}
		}
	}
	return false
}

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
// insert the region.
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

func (u *unsafeRecoveryController) generateForceLeaderPlan() bool {
	u.Lock()
	defer u.Unlock()

	newestRegionTree := newRegionTree()
	// Go through all the peer reports to build up the newest region tree
	for storeID, storeReport := range u.storeReports {
		for _, peerReport := range storeReport.PeerReports {
			newestRegionTree.update(&regionItem{report: peerReport, storeID: storeID})
		}
	}

	hasPlan := false
	// Check the regions in newest Region Tree to see if it can still elect leader
	// considering the failed stores
	newestRegionTree.tree.Ascend(func(item btree.Item) bool {
		region := item.(*regionItem).Region()
		storeID := item.(*regionItem).storeID
		if !u.canElectLeader(region) {
			storeRecoveryPlan := u.getRecoveryPlan(storeID)
			storeRecoveryPlan.EnterForceLeaders = append(storeRecoveryPlan.EnterForceLeaders, region.GetId())
			hasPlan = true
		}
		return true
	})

	// TODO: need to resolve the case 2
	// it's hard to distinguish it with unfinished split region
	// and it's rare, so won't do it now

	return hasPlan
}

func (u *unsafeRecoveryController) generateRecoveryPlan() bool {
	u.Lock()
	defer u.Unlock()

	newestRegionTree := newRegionTree()
	// Go through all the peer reports to build up the newest region tree
	for storeID, storeReport := range u.storeReports {
		for _, peerReport := range storeReport.PeerReports {
			newestRegionTree.update(&regionItem{report: peerReport, storeID: storeID})
		}
	}

	hasPlan := false
	// Check the regions in newest Region Tree to see if it can still elect leader
	// considering the failed stores
	newestRegionTree.tree.Ascend(func(item btree.Item) bool {
		region := item.(*regionItem).Region()
		storeID := item.(*regionItem).storeID
		if !u.canElectLeader(region) {
			storeRecoveryPlan := u.getRecoveryPlan(storeID)
			storeRecoveryPlan.Removes = append(storeRecoveryPlan.Removes,
				&pdpb.RemovePeers{
					RegionId: region.GetId(),
					Peers:    u.getFailedPeers(region),
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
					storeRecoveryPlan.Deletes = append(storeRecoveryPlan.Deletes, regionID)
				}
			}
		}
	}

	// There may be ranges that are covered by no one. Find these empty ranges, create new
	// regions that cover them and evenly distribute newly created regions among all stores.
	lastEnd := []byte("")
	var creates []*metapb.Region
	newestRegionTree.tree.Ascend(func(item btree.Item) bool {
		region := item.(*regionItem).Region()
		if !bytes.Equal(region.StartKey, lastEnd) {
			newRegion := &metapb.Region{}
			newRegion.StartKey = lastEnd
			newRegion.EndKey = region.StartKey
			newRegion.Id, _ = u.cluster.GetAllocator().Alloc()
			newRegion.RegionEpoch = &metapb.RegionEpoch{ConfVer: 1, Version: 1}
			creates = append(creates, newRegion)
		}
		lastEnd = region.EndKey
		return true
	})
	if !bytes.Equal(lastEnd, []byte("")) {
		newRegion := &metapb.Region{}
		newRegion.StartKey = lastEnd
		newRegion.Id, _ = u.cluster.GetAllocator().Alloc()
		creates = append(creates, newRegion)
	}
	var allStores []uint64
	for storeID := range u.storeReports {
		allStores = append(allStores, storeID)
	}
	for idx, create := range creates {
		storeID := allStores[idx%len(allStores)]
		peerID, _ := u.cluster.GetAllocator().Alloc()
		create.Peers = []*metapb.Peer{{Id: peerID, StoreId: storeID, Role: metapb.PeerRole_Voter}}
		storeRecoveryPlan := u.getRecoveryPlan(storeID)
		storeRecoveryPlan.Creates = append(storeRecoveryPlan.Creates, create)
	}

	log.Info("Plan generated")
	for store, plan := range u.storeRecoveryPlans {
		log.Info("Store plan", zap.String("store", strconv.FormatUint(store, 10)), zap.String("plan", proto.MarshalTextString(plan)))
	}
	return hasPlan
}

func getPeerDigest(peer *metapb.Peer) string {
	return strconv.FormatUint(peer.Id, 10) + ", " + strconv.FormatUint(peer.StoreId, 10) + ", " + peer.Role.String()
}

func getRegionDigest(region *metapb.Region) string {
	if region == nil {
		return "nil"
	}
	regionID := strconv.FormatUint(region.Id, 10)
	regionStartKey := core.HexRegionKeyStr(region.StartKey)
	regionEndKey := core.HexRegionKeyStr(region.EndKey)
	var peers string
	for _, peer := range region.Peers {
		peers += "(" + getPeerDigest(peer) + "), "
	}
	return fmt.Sprintf("region %s [%s, %s) {%s}", regionID, regionStartKey, regionEndKey, peers)
}

func getStoreDigest(storeReport *pdpb.StoreReport) string {
	if storeReport == nil {
		return "nil"
	}
	var result string
	for _, peerReport := range storeReport.PeerReports {
		result += getRegionDigest(peerReport.RegionState.Region) + ", "
	}
	return result
}

// Show returns the current status of ongoing unsafe recover operation.
func (u *unsafeRecoveryController) Show() []string {
	u.RLock()
	defer u.RUnlock()
	switch u.stage {
	case ready:
		return []string{"No on-going operation."}
	case collectingClusterInfo:
		var status []string
		status = append(status, fmt.Sprintf("Collecting cluster info from all alive stores, %d/%d.", u.numStoresReported, len(u.storeReports)))
		var reported, unreported string
		for storeID, report := range u.storeReports {
			if report == nil {
				unreported += strconv.FormatUint(storeID, 10) + ","
			} else {
				reported += strconv.FormatUint(storeID, 10) + ","
			}
		}
		status = append(status, "Stores that have reported to PD: "+reported)
		status = append(status, "Stores that have not reported to PD: "+unreported)
		return status
	case forceLeader:
		// TODO:
	case recovering:
		var status []string
		status = append(status, fmt.Sprintf("Waiting for recover commands being applied, %d/%d", u.numStoresReported, len(u.storeRecoveryPlans)))
		status = append(status, "Recovery plan:")
		for storeID, plan := range u.storeRecoveryPlans {
			planDigest := "Store " + strconv.FormatUint(storeID, 10) + ", creates: "
			for _, create := range plan.Creates {
				planDigest += getRegionDigest(create) + ", "
			}
			planDigest += "; removes: "
			for _, remove := range plan.Removes {
				var peers string
				for _, peer := range remove.Peers {
					peers += "(" + getPeerDigest(peer) + "), "
				}
				planDigest += fmt.Sprintf("region %d {%s}", remove.RegionId, peers) + ", "
			}
			planDigest += "; deletes: "
			for _, deletion := range plan.Deletes {
				planDigest += strconv.FormatUint(deletion, 10) + ", "
			}
			status = append(status, planDigest)
		}
		status = append(status, "Execution progess:")
		for storeID, applied := range u.executionResults {
			if !applied {
				status = append(status, strconv.FormatUint(storeID, 10)+"not yet applied, last report: "+getStoreDigest(u.executionReports[storeID]))
			}
		}
		return status
	}
	return []string{"Undefined status"}
}

// History returns the history logs of the current unsafe recover operation.
func (u *unsafeRecoveryController) History() []string {
	u.RLock()
	defer u.RUnlock()
	if u.stage <= ready {
		return []string{"No unsafe recover has been triggered since PD restarted."}
	}
	var history []string
	if u.stage >= collectingClusterInfo {
		history = append(history, "Store reports collection:")
		for storeID, report := range u.storeReports {
			if report == nil {
				history = append(history, "Store "+strconv.FormatUint(storeID, 10)+": waiting for report.")
			} else {
				history = append(history, "Store "+strconv.FormatUint(storeID, 10)+": "+getStoreDigest(report))
			}
		}
	}
	if u.stage >= recovering {
		history = append(history, "Recovery plan:")
		for storeID, plan := range u.storeRecoveryPlans {
			planDigest := "Store " + strconv.FormatUint(storeID, 10) + ", creates: "
			for _, create := range plan.Creates {
				planDigest += getRegionDigest(create) + ", "
			}
			planDigest += "; removes: "
			for _, remove := range plan.Removes {
				var peers string
				for _, peer := range remove.Peers {
					peers += "(" + getPeerDigest(peer) + "), "
				}
				planDigest += fmt.Sprintf("region %d {%s}", remove.RegionId, peers) + ", "
			}
			planDigest += "; deletes: "
			for _, deletion := range plan.Deletes {
				planDigest += strconv.FormatUint(deletion, 10) + ", "
			}
			history = append(history, planDigest)
		}
		history = append(history, "Execution progress:")
		for storeID, applied := range u.executionResults {
			executionDigest := "Store " + strconv.FormatUint(storeID, 10)
			if !applied {
				executionDigest += "not yet finished, "
			} else {
				executionDigest += "finished, "
			}
			executionDigest += getStoreDigest(u.executionReports[storeID])
			history = append(history, executionDigest)
		}
	}
	return history
}
