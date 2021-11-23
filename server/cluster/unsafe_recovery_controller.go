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
	"sort"
	"strconv"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/google/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/server/core"
	"go.uber.org/zap"
)

type unsafeRecoveryStage int

const (
	ready unsafeRecoveryStage = iota
	collectingClusterInfo
	recovering
	finished
)

type unsafeRecoveryController struct {
	sync.RWMutex

	cluster               *RaftCluster
	stage                 unsafeRecoveryStage
	failedStores          map[uint64]string
	storeReports          map[uint64]*pdpb.StoreReport // Store info proto
	numStoresReported     int
	storeRecoveryPlans    map[uint64]*pdpb.RecoveryPlan // StoreRecoveryPlan proto
	executionResults      map[uint64]bool               // Execution reports for tracking purpose
	executionReports      map[uint64]*pdpb.StoreReport  // Execution reports for tracking purpose
	numStoresPlanExecuted int
}

func newUnsafeRecoveryController(cluster *RaftCluster) *unsafeRecoveryController {
	return &unsafeRecoveryController{
		cluster:               cluster,
		stage:                 ready,
		failedStores:          make(map[uint64]string),
		storeReports:          make(map[uint64]*pdpb.StoreReport),
		numStoresReported:     0,
		storeRecoveryPlans:    make(map[uint64]*pdpb.RecoveryPlan),
		executionResults:      make(map[uint64]bool),
		executionReports:      make(map[uint64]*pdpb.StoreReport),
		numStoresPlanExecuted: 0,
	}
}

// RemoveFailedStores removes failed stores from the cluster.
func (u *unsafeRecoveryController) RemoveFailedStores(failedStores map[uint64]string) error {
	u.Lock()
	defer u.Unlock()
	if len(failedStores) == 0 {
		return errors.Errorf("No store specified")
	}
	if u.stage != ready && u.stage != finished {
		return errors.Errorf("Another request is working in progress")
	}
	u.reset()
	u.failedStores = failedStores
	for _, s := range u.cluster.GetStores() {
		if s.IsTombstone() || s.IsPhysicallyDestroyed() || core.IsStoreContainLabel(s.GetMeta(), core.EngineKey, core.EngineTiFlash) {
			continue
		}
		if _, exists := failedStores[s.GetID()]; exists {
			continue
		}
		u.storeReports[s.GetID()] = nil
	}
	u.stage = collectingClusterInfo
	return nil
}

// HandleStoreHeartbeat handles the store heartbeat requests and checks whether the stores need to
// send detailed report back.
func (u *unsafeRecoveryController) HandleStoreHeartbeat(heartbeat *pdpb.StoreHeartbeatRequest, resp *pdpb.StoreHeartbeatResponse) {
	u.Lock()
	defer u.Unlock()
	if len(u.failedStores) == 0 {
		return
	}
	switch u.stage {
	case collectingClusterInfo:
		if heartbeat.StoreReport == nil {
			if _, failedStore := u.failedStores[heartbeat.Stats.StoreId]; !failedStore {
				// Inform the store to send detailed report in the next heartbeat.
				resp.RequireDetailedReport = true
			}
		} else if report, exist := u.storeReports[heartbeat.Stats.StoreId]; exist && report == nil {
			u.storeReports[heartbeat.Stats.StoreId] = heartbeat.StoreReport
			u.numStoresReported++
			if u.numStoresReported == len(u.storeReports) {
				log.Info("Reports have been fully collected, generating plan...")
				go u.generateRecoveryPlan()
			}
		}
	case recovering:
		if plan, tasked := u.storeRecoveryPlans[heartbeat.Stats.StoreId]; tasked {
			if heartbeat.StoreReport == nil {
				// Sends the recovering plan to the store for execution.
				resp.Plan = plan
			} else if !u.isPlanExecuted(heartbeat.Stats.StoreId, heartbeat.StoreReport) {
				resp.Plan = plan
				u.executionReports[heartbeat.Stats.StoreId] = heartbeat.StoreReport
			} else {
				u.executionResults[heartbeat.Stats.StoreId] = true
				u.executionReports[heartbeat.Stats.StoreId] = heartbeat.StoreReport
				u.numStoresPlanExecuted++
				if u.numStoresPlanExecuted == len(u.storeRecoveryPlans) {
					log.Info("Recover finished.")
					go func() {
						for _, history := range u.History() {
							log.Info(history)
						}
					}()
					u.stage = finished
				}
			}
		}
	}
}

func (u *unsafeRecoveryController) reset() {
	u.stage = ready
	u.failedStores = make(map[uint64]string)
	u.storeReports = make(map[uint64]*pdpb.StoreReport)
	u.numStoresReported = 0
	u.storeRecoveryPlans = make(map[uint64]*pdpb.RecoveryPlan)
	u.executionResults = make(map[uint64]bool)
	u.executionReports = make(map[uint64]*pdpb.StoreReport)
	u.numStoresPlanExecuted = 0
}

func (u *unsafeRecoveryController) isPlanExecuted(storeID uint64, report *pdpb.StoreReport) bool {
	targetRegions := make(map[uint64]*metapb.Region)
	toBeRemovedRegions := make(map[uint64]bool)
	for _, create := range u.storeRecoveryPlans[storeID].Creates {
		targetRegions[create.Id] = create
	}
	for _, update := range u.storeRecoveryPlans[storeID].Updates {
		targetRegions[update.Id] = update
	}
	for _, del := range u.storeRecoveryPlans[storeID].Deletes {
		toBeRemovedRegions[del] = true
	}
	numFinished := 0
	for _, peerReport := range report.PeerReports {
		region := peerReport.RegionState.Region
		if _, ok := toBeRemovedRegions[region.Id]; ok {
			return false
		} else if target, ok := targetRegions[region.Id]; ok {
			if bytes.Equal(target.StartKey, region.StartKey) && bytes.Equal(target.EndKey, region.EndKey) && !u.containsFailedPeers(region) {
				numFinished += 1
			} else {
				return false
			}
		}
	}
	return numFinished == len(targetRegions)
}

type regionItem struct {
	region *metapb.Region
}

func (r regionItem) Less(other btree.Item) bool {
	return bytes.Compare(r.region.StartKey, other.(regionItem).region.StartKey) < 0
}

func (u *unsafeRecoveryController) canElectLeader(region *metapb.Region) bool {
	numFailedVoters := 0
	numLiveVoters := 0
	for _, peer := range region.Peers {
		if peer.Role != metapb.PeerRole_Voter && peer.Role != metapb.PeerRole_IncomingVoter {
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

func (u *unsafeRecoveryController) containsFailedPeers(region *metapb.Region) bool {
	for _, peer := range region.Peers {
		if _, ok := u.failedStores[peer.StoreId]; ok {
			return true
		}
	}
	return false
}

func keepOneReplica(storeID uint64, region *metapb.Region) {
	var newPeerList []*metapb.Peer
	for _, peer := range region.Peers {
		if peer.StoreId == storeID {
			if peer.Role != metapb.PeerRole_Voter {
				peer.Role = metapb.PeerRole_Voter
			}
			newPeerList = append(newPeerList, peer)
		}
	}
	region.Peers = newPeerList
}

type peerStorePair struct {
	peer    *pdpb.PeerReport
	storeID uint64
}

func getOverlapRanges(tree *btree.BTree, region *metapb.Region) []*metapb.Region {
	var overlapRanges []*metapb.Region
	tree.DescendLessOrEqual(regionItem{region}, func(item btree.Item) bool {
		if bytes.Compare(item.(regionItem).region.StartKey, region.StartKey) < 0 && bytes.Compare(item.(regionItem).region.EndKey, region.StartKey) > 0 {
			overlapRanges = append(overlapRanges, item.(regionItem).region)
		}
		return false
	})

	tree.AscendGreaterOrEqual(regionItem{region}, func(item btree.Item) bool {
		if len(region.EndKey) != 0 && bytes.Compare(item.(regionItem).region.StartKey, region.EndKey) > 0 {
			return false
		}
		overlapRanges = append(overlapRanges, item.(regionItem).region)
		return true
	})
	return overlapRanges
}

func (u *unsafeRecoveryController) generateRecoveryPlan() {
	u.Lock()
	defer u.Unlock()
	newestRegionReports := make(map[uint64]*pdpb.PeerReport)
	var allPeerReports []*peerStorePair
	for storeID, storeReport := range u.storeReports {
		for _, peerReport := range storeReport.PeerReports {
			allPeerReports = append(allPeerReports, &peerStorePair{peerReport, storeID})
			regionID := peerReport.RegionState.Region.Id
			if existing, ok := newestRegionReports[regionID]; ok {
				if existing.RegionState.Region.RegionEpoch.Version >= peerReport.RegionState.Region.RegionEpoch.Version &&
					existing.RegionState.Region.RegionEpoch.ConfVer >= peerReport.RegionState.Region.RegionEpoch.Version &&
					existing.RaftState.LastIndex >= peerReport.RaftState.LastIndex {
					continue
				}
			}
			newestRegionReports[regionID] = peerReport
		}
	}
	recoveredRanges := btree.New(2)
	healthyRegions := make(map[uint64]*pdpb.PeerReport)
	inUseRegions := make(map[uint64]bool)
	for _, report := range newestRegionReports {
		region := report.RegionState.Region
		// TODO(v01dstar): Whether the group can elect a leader should not merely rely on failed stores / peers, since it is possible that all reported peers are stale.
		if u.canElectLeader(report.RegionState.Region) {
			healthyRegions[region.Id] = report
			inUseRegions[region.Id] = true
			recoveredRanges.ReplaceOrInsert(regionItem{report.RegionState.Region})
		}
	}
	sort.SliceStable(allPeerReports, func(i, j int) bool {
		return allPeerReports[i].peer.RegionState.Region.RegionEpoch.Version > allPeerReports[j].peer.RegionState.Region.RegionEpoch.Version
	})
	for _, peerStorePair := range allPeerReports {
		region := peerStorePair.peer.RegionState.Region
		storeID := peerStorePair.storeID
		lastEnd := region.StartKey
		reachedTheEnd := false
		var creates []*metapb.Region
		var update *metapb.Region
		for _, overlapRegion := range getOverlapRanges(recoveredRanges, region) {
			if bytes.Compare(lastEnd, overlapRegion.StartKey) < 0 {
				newRegion := proto.Clone(region).(*metapb.Region)
				keepOneReplica(storeID, newRegion)
				newRegion.StartKey = lastEnd
				newRegion.EndKey = overlapRegion.StartKey
				if _, inUse := inUseRegions[region.Id]; inUse {
					newRegion.Id, _ = u.cluster.AllocID()
					creates = append(creates, newRegion)
				} else {
					inUseRegions[region.Id] = true
					update = newRegion
				}
				recoveredRanges.ReplaceOrInsert(regionItem{newRegion})
				if len(overlapRegion.EndKey) == 0 {
					reachedTheEnd = true
					break
				}
				lastEnd = overlapRegion.EndKey
			} else if len(overlapRegion.EndKey) == 0 {
				reachedTheEnd = true
				break
			} else if bytes.Compare(overlapRegion.EndKey, lastEnd) > 0 {
				lastEnd = overlapRegion.EndKey
			}
		}
		if !reachedTheEnd && (bytes.Compare(lastEnd, region.EndKey) < 0 || len(region.EndKey) == 0) {
			newRegion := proto.Clone(region).(*metapb.Region)
			keepOneReplica(storeID, newRegion)
			newRegion.StartKey = lastEnd
			newRegion.EndKey = region.EndKey
			if _, inUse := inUseRegions[region.Id]; inUse {
				newRegion.Id, _ = u.cluster.AllocID()
				creates = append(creates, newRegion)
			} else {
				inUseRegions[region.Id] = true
				update = newRegion
			}
			recoveredRanges.ReplaceOrInsert(regionItem{newRegion})
		}
		if len(creates) != 0 || update != nil {
			storeRecoveryPlan, exists := u.storeRecoveryPlans[storeID]
			if !exists {
				u.storeRecoveryPlans[storeID] = &pdpb.RecoveryPlan{}
				storeRecoveryPlan = u.storeRecoveryPlans[storeID]
			}
			storeRecoveryPlan.Creates = append(storeRecoveryPlan.Creates, creates...)
			if update != nil {
				storeRecoveryPlan.Updates = append(storeRecoveryPlan.Updates, update)
			}
		} else if _, healthy := healthyRegions[region.Id]; !healthy {
			// If this peer contributes nothing to the recovered ranges, and it does not belong to a healthy region, delete it.
			storeRecoveryPlan, exists := u.storeRecoveryPlans[storeID]
			if !exists {
				u.storeRecoveryPlans[storeID] = &pdpb.RecoveryPlan{}
				storeRecoveryPlan = u.storeRecoveryPlans[storeID]
			}
			storeRecoveryPlan.Deletes = append(storeRecoveryPlan.Deletes, region.Id)
		}
	}
	// There may be ranges that are covered by no one. Find these empty ranges, create new regions that cover them and evenly distribute newly created regions among all stores.
	lastEnd := []byte("")
	var creates []*metapb.Region
	recoveredRanges.Ascend(func(item btree.Item) bool {
		region := item.(regionItem).region
		if !bytes.Equal(region.StartKey, lastEnd) {
			newRegion := &metapb.Region{}
			newRegion.StartKey = lastEnd
			newRegion.EndKey = region.StartKey
			newRegion.Id, _ = u.cluster.AllocID()
			newRegion.RegionEpoch = &metapb.RegionEpoch{ConfVer: 1, Version: 1}
			creates = append(creates, newRegion)
		}
		lastEnd = region.EndKey
		return true
	})
	if !bytes.Equal(lastEnd, []byte("")) {
		newRegion := &metapb.Region{}
		newRegion.StartKey = lastEnd
		newRegion.Id, _ = u.cluster.AllocID()
		creates = append(creates, newRegion)
	}
	var allStores []uint64
	for storeID := range u.storeReports {
		allStores = append(allStores, storeID)
	}
	for idx, create := range creates {
		storeID := allStores[idx%len(allStores)]
		peerID, _ := u.cluster.AllocID()
		create.Peers = []*metapb.Peer{{Id: peerID, StoreId: storeID, Role: metapb.PeerRole_Voter}}
		storeRecoveryPlan, exists := u.storeRecoveryPlans[storeID]
		if !exists {
			u.storeRecoveryPlans[storeID] = &pdpb.RecoveryPlan{}
			storeRecoveryPlan = u.storeRecoveryPlans[storeID]
		}
		storeRecoveryPlan.Creates = append(storeRecoveryPlan.Creates, create)
	}
	log.Info("Plan generated")
	if len(u.storeRecoveryPlans) == 0 {
		log.Info("Nothing to do")
		u.stage = finished
		return
	}
	for store, plan := range u.storeRecoveryPlans {
		log.Info("Store plan", zap.String("store", strconv.FormatUint(store, 10)), zap.String("plan", proto.MarshalTextString(plan)))
	}
	u.stage = recovering
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
	case recovering:
		var status []string
		status = append(status, fmt.Sprintf("Waiting for recover commands being applied, %d/%d", u.numStoresPlanExecuted, len(u.storeRecoveryPlans)))
		status = append(status, "Recovery plan:")
		for storeID, plan := range u.storeRecoveryPlans {
			planDigest := "Store " + strconv.FormatUint(storeID, 10) + ", creates: "
			for _, create := range plan.Creates {
				planDigest += getRegionDigest(create) + ", "
			}
			planDigest += "; updates: "
			for _, update := range plan.Updates {
				planDigest += getRegionDigest(update) + ", "
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
	case finished:
		return []string{"Last recovery has finished."}
	}
	return []string{"Undefined status"}
}

// History returns the history logs of the current unsafe recover operation.
func (u *unsafeRecoveryController) History() []string {
	u.RLock()
	defer u.RUnlock()
	if u.stage <= ready {
		return []string{"No unasfe recover has been triggered since PD restarted."}
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
			planDigest += "; updates: "
			for _, update := range plan.Updates {
				planDigest += getRegionDigest(update) + ", "
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
