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
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/btree"
	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/pkg/syncutil"
	"github.com/tikv/pd/server/core"
	"go.uber.org/zap"
)

type unsafeRecoveryStage int

const (
	storeRequestInterval = time.Second * 40
)

// Stage transition graph: for more details, please check `unsafeRecoveryController.HandleStoreHeartbeat()`
//                      +-----------+           +-----------+
//  +-----------+       |           |           |           |
//  |           |       |  collect  |           | tombstone |
//  |   idle    |------>|  Report   |-----+---->|  tiflash  |-----+
//  |           |       |           |     |     |  learner  |     |
//  +-----------+       +-----------+     |     |           |     |
//                                        |     +-----------+     |
//                                        |           |           |
//                                        |           |           |
//                                        |           v           |
//                                        |     +-----------+     |
//                                        |     |           |     |
//                                        |     |   force   |     |
//                                        |     | LeaderFor |-----+
//                                        |     |CommitMerge|     |
//                                        |     |           |     |
//                                        |     +-----------+     |
//                                        |           |           |
//                                        |           |           |
//                                        |           v           |
//                                        |     +-----------+     |     +-----------+
//                                        |     |           |     |     |           |        +-----------+
//                                        |     |  force    |     |     | exitForce |        |           |
//                                        |     |  Leader   |-----+---->|  Leader   |------->|  failed   |
//                                        |     |           |     |     |           |        |           |
//                                        |     +-----------+     |     +-----------+        +-----------+
//                                        |           |           |
//                                        |           |           |
//                                        |           v           |
//                                        |     +-----------+     |
//                                        |     |           |     |
//                                        |     |  demote   |     |
//                                        +-----|  Voter    |-----|
//                                              |           |     |
//                                              +-----------+     |
//                                                    |           |
//                                                    |           |
//                                                    v           |
//                      +-----------+           +-----------+     |
//  +-----------+       |           |           |           |     |
//  |           |       | exitForce |           |  create   |     |
//  | finished  |<------|  Leader   |<----------|  Region   |-----+
//  |           |       |           |           |           |
//  +-----------+       +-----------+           +-----------+
//
const (
	idle unsafeRecoveryStage = iota
	collectReport
	tombstoneTiFlashLearner
	forceLeaderForCommitMerge
	forceLeader
	demoteFailedVoter
	createEmptyRegion
	exitForceLeader
	finished
	failed
)

type unsafeRecoveryController struct {
	syncutil.RWMutex

	cluster *RaftCluster
	stage   unsafeRecoveryStage
	// the round of recovery, which is an increasing number to identify the reports of each round
	step         uint64
	failedStores map[uint64]struct{}
	timeout      time.Time

	// collected reports from store, if not reported yet, it would be nil
	storeReports      map[uint64]*pdpb.StoreReport
	numStoresReported int

	storePlanExpires   map[uint64]time.Time
	storeRecoveryPlans map[uint64]*pdpb.RecoveryPlan

	// accumulated output for the whole recovery process
	output              []StageOutput
	affectedTableIDs    map[int64]struct{}
	affectedMetaRegions map[uint64]struct{}
	err                 error
}

// StageOutput is the information for one stage of the recovery process.
type StageOutput struct {
	Info    string              `json:"info,omitempty"`
	Time    string              `json:"time,omitempty"`
	Actions map[string][]string `json:"actions,omitempty"`
	Details []string            `json:"details,omitempty"`
}

func newUnsafeRecoveryController(cluster *RaftCluster) *unsafeRecoveryController {
	u := &unsafeRecoveryController{
		cluster: cluster,
	}
	u.reset()
	return u
}

func (u *unsafeRecoveryController) reset() {
	u.stage = idle
	u.step = 0
	u.failedStores = make(map[uint64]struct{})
	u.storeReports = make(map[uint64]*pdpb.StoreReport)
	u.numStoresReported = 0
	u.storePlanExpires = make(map[uint64]time.Time)
	u.storeRecoveryPlans = make(map[uint64]*pdpb.RecoveryPlan)
	u.output = make([]StageOutput, 0)
	u.affectedTableIDs = make(map[int64]struct{}, 0)
	u.affectedMetaRegions = make(map[uint64]struct{}, 0)
	u.err = nil
}

// IsRunning returns whether there is ongoing unsafe recovery process. If yes, further unsafe
// recovery requests, schedulers, checkers, AskSplit and AskBatchSplit requests are blocked.
func (u *unsafeRecoveryController) IsRunning() bool {
	u.RLock()
	defer u.RUnlock()
	return u.stage != idle && u.stage != finished && u.stage != failed
}

// RemoveFailedStores removes failed stores from the cluster.
func (u *unsafeRecoveryController) RemoveFailedStores(failedStores map[uint64]struct{}, timeout uint64) error {
	if u.IsRunning() {
		return errs.ErrUnsafeRecoveryIsRunning.FastGenByArgs()
	}
	u.Lock()
	defer u.Unlock()

	if len(failedStores) == 0 {
		return errs.ErrUnsafeRecoveryInvalidInput.FastGenByArgs("no store specified")
	}

	// validate the stores and mark the store as tombstone forcibly
	for failedStore := range failedStores {
		store := u.cluster.GetStore(failedStore)
		if store == nil {
			return errs.ErrUnsafeRecoveryInvalidInput.FastGenByArgs(fmt.Sprintf("store %v doesn't exist", failedStore))
		} else if (store.IsPreparing() || store.IsServing()) && !store.IsDisconnected() {
			return errs.ErrUnsafeRecoveryInvalidInput.FastGenByArgs(fmt.Sprintf("store %v is up and connected", failedStore))
		}
	}
	for failedStore := range failedStores {
		err := u.cluster.BuryStore(failedStore, true)
		if err != nil && !errors.ErrorEqual(err, errs.ErrStoreNotFound.FastGenByArgs(failedStore)) {
			return err
		}
	}

	u.reset()
	for _, s := range u.cluster.GetStores() {
		if s.IsRemoved() || s.IsPhysicallyDestroyed() {
			continue
		}
		if _, exists := failedStores[s.GetID()]; exists {
			continue
		}
		u.storeReports[s.GetID()] = nil
	}

	u.timeout = time.Now().Add(time.Duration(timeout) * time.Second)
	u.failedStores = failedStores
	u.changeStage(collectReport)
	return nil
}

// Show returns the current status of ongoing unsafe recover operation.
func (u *unsafeRecoveryController) Show() []StageOutput {
	u.Lock()
	defer u.Unlock()

	if u.stage == idle {
		return []StageOutput{{Info: "No on-going recovery."}}
	}
	u.checkTimeout()
	status := u.output
	if u.stage != finished && u.stage != failed {
		status = append(status, u.getReportStatus())
	}
	return status
}

func (u *unsafeRecoveryController) getReportStatus() StageOutput {
	var status StageOutput
	status.Time = time.Now().Format("2006-01-02 15:04:05.000")
	if u.numStoresReported != len(u.storeReports) {
		status.Info = fmt.Sprintf("Collecting reports from alive stores(%d/%d)", u.numStoresReported, len(u.storeReports))
		var reported, unreported, undispatched string
		for storeID, report := range u.storeReports {
			str := strconv.FormatUint(storeID, 10) + ", "
			if report == nil {
				if _, requested := u.storePlanExpires[storeID]; !requested {
					undispatched += str
				} else {
					unreported += str
				}
			} else {
				reported += str
			}
		}
		status.Details = append(status.Details, "Stores that have not dispatched plan: "+strings.Trim(undispatched, ", "))
		status.Details = append(status.Details, "Stores that have reported to PD: "+strings.Trim(reported, ", "))
		status.Details = append(status.Details, "Stores that have not reported to PD: "+strings.Trim(unreported, ", "))
	} else {
		status.Info = fmt.Sprintf("Collected reports from all %d alive stores", len(u.storeReports))
	}
	return status
}

func (u *unsafeRecoveryController) checkTimeout() bool {
	if u.stage == finished || u.stage == failed {
		return false
	}

	if time.Now().After(u.timeout) {
		u.err = errors.Errorf("Exceeds timeout %v", u.timeout)
		return u.handleErr()
	}
	return false
}

// HandleStoreHeartbeat handles the store heartbeat requests and checks whether the stores need to
// send detailed report back.
func (u *unsafeRecoveryController) HandleStoreHeartbeat(heartbeat *pdpb.StoreHeartbeatRequest, resp *pdpb.StoreHeartbeatResponse) {
	if !u.IsRunning() {
		// no recovery in progress, do nothing
		return
	}
	u.Lock()
	defer u.Unlock()

	if u.checkTimeout() {
		return
	}

	allCollected, err := u.collectReport(heartbeat)
	if err != nil {
		u.err = err
		if u.handleErr() {
			return
		}
	}

	if allCollected {
		newestRegionTree, peersMap, err := u.buildUpFromReports()
		if err != nil {
			u.err = err
			if u.handleErr() {
				return
			}
		}

		// clean up previous plan
		u.storePlanExpires = make(map[uint64]time.Time)
		u.storeRecoveryPlans = make(map[uint64]*pdpb.RecoveryPlan)

		stage := u.stage
		reCheck := false
		for {
			switch stage {
			case collectReport:
				fallthrough
			case tombstoneTiFlashLearner:
				if u.generateTombstoneTiFlashLearnerPlan(newestRegionTree, peersMap) {
					u.changeStage(tombstoneTiFlashLearner)
					break
				}
				fallthrough
			case forceLeaderForCommitMerge:
				if u.generateForceLeaderPlan(newestRegionTree, peersMap, true) {
					u.changeStage(forceLeaderForCommitMerge)
					break
				}
				fallthrough
			case forceLeader:
				if u.generateForceLeaderPlan(newestRegionTree, peersMap, false) {
					u.changeStage(forceLeader)
					break
				}
				fallthrough
			case demoteFailedVoter:
				if u.generateDemoteFailedVoterPlan(newestRegionTree, peersMap) {
					u.changeStage(demoteFailedVoter)
					break
				} else if !reCheck {
					reCheck = true
					stage = tombstoneTiFlashLearner
					continue
				}
				fallthrough
			case createEmptyRegion:
				if u.generateCreateEmptyRegionPlan(newestRegionTree, peersMap) {
					u.changeStage(createEmptyRegion)
					break
				}
				fallthrough
			case exitForceLeader:
				// no need to generate plan, empty recovery plan triggers exit force leader on TiKV side
				if u.generateExitForceLeaderPlan() {
					u.changeStage(exitForceLeader)
				}
			default:
				panic("unreachable")
			}

			hasPlan := len(u.storeRecoveryPlans) != 0
			if u.err != nil {
				if u.handleErr() {
					return
				}
			} else if !hasPlan {
				u.changeStage(finished)
				return
			}
			break
		}
	}

	u.dispatchPlan(heartbeat, resp)
}

func (u *unsafeRecoveryController) handleErr() bool {
	if u.err != nil {
		if u.stage == exitForceLeader {
			u.changeStage(failed)
			return true
		}
		u.storePlanExpires = make(map[uint64]time.Time)
		u.storeRecoveryPlans = make(map[uint64]*pdpb.RecoveryPlan)
		u.timeout = time.Now().Add(storeRequestInterval)
		u.changeStage(exitForceLeader)
	}
	return false
}

/// It dispatches recovery plan if any.
func (u *unsafeRecoveryController) dispatchPlan(heartbeat *pdpb.StoreHeartbeatRequest, resp *pdpb.StoreHeartbeatResponse) {
	storeID := heartbeat.Stats.StoreId
	now := time.Now()

	if reported, exist := u.storeReports[storeID]; reported != nil || !exist {
		// the plan has been executed, no need to dispatch again
		// or no need to displan plan to this store(e.g. Tiflash)
		return
	}

	if expire, dispatched := u.storePlanExpires[storeID]; !dispatched || expire.Before(now) {
		if dispatched {
			log.Info("Unsafe recovery store recovery plan execution timeout, retry", zap.Uint64("store-id", storeID))
		}
		// Dispatch the recovery plan to the store, and the plan may be empty.
		resp.RecoveryPlan = u.getRecoveryPlan(storeID)
		resp.RecoveryPlan.Step = u.step
		u.storePlanExpires[storeID] = now.Add(storeRequestInterval)
	}
}

// It collects and checks if store reports have been fully collected.
func (u *unsafeRecoveryController) collectReport(heartbeat *pdpb.StoreHeartbeatRequest) (bool, error) {
	storeID := heartbeat.Stats.StoreId
	if _, isFailedStore := u.failedStores[storeID]; isFailedStore {
		return false, errors.Errorf("Receive heartbeat from failed store %d", storeID)
	}

	if heartbeat.StoreReport == nil {
		return false, nil
	}

	if heartbeat.StoreReport.GetStep() != u.step {
		log.Info("Unsafe recovery receives invalid store report",
			zap.Uint64("store-id", storeID), zap.Uint64("expected-step", u.step), zap.Uint64("obtained-step", heartbeat.StoreReport.GetStep()))
		// invalid store report, ignore
		return false, nil
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

// Gets the stage of the current unsafe recovery.
func (u *unsafeRecoveryController) GetStage() unsafeRecoveryStage {
	u.RLock()
	defer u.RUnlock()
	return u.stage
}

func (u *unsafeRecoveryController) changeStage(stage unsafeRecoveryStage) {
	u.stage = stage

	var output StageOutput
	output.Time = time.Now().Format("2006-01-02 15:04:05.000")
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
		// TODO: clean up existing operators
		output.Info = fmt.Sprintf("Unsafe recovery enters collect report stage: failed stores %s", stores)
	case tombstoneTiFlashLearner:
		output.Info = "Unsafe recovery enters tombstone TiFlash learner stage"
		output.Actions = u.getTombstoneTiFlashLearnerDigest()
	case forceLeaderForCommitMerge:
		output.Info = "Unsafe recovery enters force leader for commit merge stage"
		output.Actions = u.getForceLeaderPlanDigest()
	case forceLeader:
		output.Info = "Unsafe recovery enters force leader stage"
		output.Actions = u.getForceLeaderPlanDigest()
	case demoteFailedVoter:
		output.Info = "Unsafe recovery enters demote failed voter stage"
		output.Actions = u.getDemoteFailedVoterPlanDigest()
	case createEmptyRegion:
		output.Info = "Unsafe recovery enters create empty region stage"
		output.Actions = u.getCreateEmptyRegionPlanDigest()
	case exitForceLeader:
		output.Info = "Unsafe recovery enters exit force leader stage"
		if u.err != nil {
			output.Details = append(output.Details, fmt.Sprintf("triggered by error: %v", u.err.Error()))
		}
	case finished:
		if u.step > 1 {
			// == 1 means no operation has done, no need to invalid cache
			u.cluster.DropCacheAllRegion()
		}
		output.Info = "Unsafe recovery finished"
		output.Details = u.getAffectedTableDigest()
		u.storePlanExpires = make(map[uint64]time.Time)
		u.storeRecoveryPlans = make(map[uint64]*pdpb.RecoveryPlan)
	case failed:
		output.Info = fmt.Sprintf("Unsafe recovery failed: %v", u.err)
		output.Details = u.getAffectedTableDigest()
		if u.numStoresReported != len(u.storeReports) {
			// in collecting reports, print out which stores haven't reported yet
			output.Details = append(output.Details, u.getReportStatus().Details...)
		}
		u.storePlanExpires = make(map[uint64]time.Time)
		u.storeRecoveryPlans = make(map[uint64]*pdpb.RecoveryPlan)
	}

	u.output = append(u.output, output)
	data, err := json.Marshal(output)
	if err != nil {
		u.err = err
		return
	}
	log.Info(string(data))

	// reset store reports to nil instead of delete, because it relays on the item
	// to decide which store it needs to collect the report from.
	for k := range u.storeReports {
		u.storeReports[k] = nil
	}
	u.numStoresReported = 0
	u.step += 1
}

func (u *unsafeRecoveryController) getForceLeaderPlanDigest() map[string][]string {
	outputs := make(map[string][]string)
	for storeID, plan := range u.storeRecoveryPlans {
		forceLeaders := plan.GetForceLeader()
		if forceLeaders != nil {
			regions := ""
			for i, regionID := range forceLeaders.GetEnterForceLeaders() {
				regions += fmt.Sprintf("%d", regionID)
				if i != len(forceLeaders.GetEnterForceLeaders())-1 {
					regions += ", "
				}
			}
			outputs[fmt.Sprintf("store %d", storeID)] = []string{fmt.Sprintf("force leader on regions: %s", regions)}
		}
	}
	return outputs
}

func (u *unsafeRecoveryController) getDemoteFailedVoterPlanDigest() map[string][]string {
	outputs := make(map[string][]string)
	for storeID, plan := range u.storeRecoveryPlans {
		if len(plan.GetDemotes()) == 0 && len(plan.GetTombstones()) == 0 {
			continue
		}
		output := []string{}
		for _, demote := range plan.GetDemotes() {
			peers := ""
			for _, peer := range demote.GetFailedVoters() {
				peers += fmt.Sprintf("{ %v}, ", peer) // the extra space is intentional
			}
			output = append(output, fmt.Sprintf("region %d demotes peers %s", demote.GetRegionId(), strings.Trim(peers, ", ")))
		}
		for _, tombstone := range plan.GetTombstones() {
			output = append(output, fmt.Sprintf("tombstone the peer of region %d", tombstone))
		}
		outputs[fmt.Sprintf("store %d", storeID)] = output
	}
	return outputs
}

func (u *unsafeRecoveryController) getTombstoneTiFlashLearnerDigest() map[string][]string {
	outputs := make(map[string][]string)
	for storeID, plan := range u.storeRecoveryPlans {
		if len(plan.GetTombstones()) == 0 {
			continue
		}
		output := []string{}
		for _, tombstone := range plan.GetTombstones() {
			output = append(output, fmt.Sprintf("tombstone the peer of region %d", tombstone))
		}
		outputs[fmt.Sprintf("store %d", storeID)] = output
	}
	return outputs
}

func (u *unsafeRecoveryController) getCreateEmptyRegionPlanDigest() map[string][]string {
	outputs := make(map[string][]string)
	for storeID, plan := range u.storeRecoveryPlans {
		if plan.GetCreates() == nil {
			continue
		}
		output := []string{}
		for _, region := range plan.GetCreates() {
			info := logutil.RedactStringer(core.RegionToHexMeta(region)).String()
			// avoid json escape character to make the output readable
			info = strings.ReplaceAll(info, "<", "{ ") // the extra space is intentional
			info = strings.ReplaceAll(info, ">", "}")
			output = append(output, fmt.Sprintf("create region %d: %v", region.GetId(), info))
		}
		outputs[fmt.Sprintf("store %d", storeID)] = output
	}
	return outputs
}

func (u *unsafeRecoveryController) getAffectedTableDigest() []string {
	var details []string
	if len(u.affectedMetaRegions) != 0 {
		regions := ""
		for r := range u.affectedMetaRegions {
			regions += fmt.Sprintf("%d, ", r)
		}
		details = append(details, "affected meta regions: "+strings.Trim(regions, ", "))
	}
	if len(u.affectedTableIDs) != 0 {
		tables := ""
		for t := range u.affectedTableIDs {
			tables += fmt.Sprintf("%d, ", t)
		}
		details = append(details, "affected table ids: "+strings.Trim(tables, ", "))
	}
	return details
}

func (u *unsafeRecoveryController) recordAffectedRegion(region *metapb.Region) {
	isMeta, tableID := codec.Key(region.StartKey).MetaOrTable()
	if isMeta {
		u.affectedMetaRegions[region.GetId()] = struct{}{}
	} else if tableID != 0 {
		u.affectedTableIDs[tableID] = struct{}{}
	}
}

func (u *unsafeRecoveryController) canElectLeader(region *metapb.Region, onlyIncoming bool) bool {
	hasQuorum := func(voters []*metapb.Peer) bool {
		numFailedVoters := 0
		numLiveVoters := 0

		for _, voter := range voters {
			if _, ok := u.failedStores[voter.StoreId]; ok {
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
		if peer.Role == metapb.PeerRole_Voter || peer.Role == metapb.PeerRole_IncomingVoter {
			incomingVoters = append(incomingVoters, peer)
		}
		if peer.Role == metapb.PeerRole_Voter || peer.Role == metapb.PeerRole_DemotingVoter {
			outgoingVoters = append(outgoingVoters, peer)
		}
	}

	return hasQuorum(incomingVoters) && (onlyIncoming || hasQuorum(outgoingVoters))
}

func (u *unsafeRecoveryController) getFailedPeers(region *metapb.Region) []*metapb.Peer {
	// if it can form a quorum after exiting the joint state, then no need to demotes any peer
	if u.canElectLeader(region, true) {
		return nil
	}

	var failedPeers []*metapb.Peer
	for _, peer := range region.Peers {
		if peer.Role == metapb.PeerRole_Learner || peer.Role == metapb.PeerRole_DemotingVoter {
			continue
		}
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

func (r *regionItem) IsInitialized() bool {
	return len(r.Region().Peers) != 0
}

func (r *regionItem) IsEpochStale(other *regionItem) bool {
	re := r.Region().GetRegionEpoch()
	oe := other.Region().GetRegionEpoch()
	return re.GetVersion() < oe.GetVersion() || re.GetConfVer() < oe.GetConfVer()
}

func (r *regionItem) IsRaftStale(origin *regionItem, u *unsafeRecoveryController) bool {
	cmps := []func(a, b *regionItem) int{
		func(a, b *regionItem) int {
			return int(a.report.GetRaftState().GetHardState().GetTerm()) - int(b.report.GetRaftState().GetHardState().GetTerm())
		},
		func(a, b *regionItem) int {
			return int(a.report.GetRaftState().GetLastIndex()) - int(b.report.GetRaftState().GetLastIndex())
		},
		func(a, b *regionItem) int {
			return int(a.report.GetRaftState().GetHardState().GetCommit()) - int(b.report.GetRaftState().GetHardState().GetCommit())
		},
		func(a, b *regionItem) int {
			if u.cluster.GetStore(a.storeID).IsTiFlash() {
				return -1
			}
			if u.cluster.GetStore(b.storeID).IsTiFlash() {
				return 1
			}
			// better use voter rather than learner
			for _, peer := range a.Region().GetPeers() {
				if peer.StoreId == a.storeID {
					if peer.Role == metapb.PeerRole_DemotingVoter || peer.Role == metapb.PeerRole_Learner {
						return -1
					}
				}
			}
			return 0
		},
	}

	for _, cmp := range cmps {
		if v := cmp(r, origin); v != 0 {
			return v < 0
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

func (t *regionTree) size() int {
	return t.tree.Len()
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

// Insert the peer report of one region int the tree.
// It finds and deletes all the overlapped regions first, and then
// insert the new region.
func (t *regionTree) insert(item *regionItem) (bool, error) {
	overlaps := t.getOverlaps(item)

	if t.contains(item.Region().GetId()) {
		// it's ensured by the `buildUpFromReports` that only insert the latest peer of one region.
		return false, errors.Errorf("region %v shouldn't be updated twice", item.Region().GetId())
	}

	for _, old := range overlaps {
		// it's ensured by the `buildUpFromReports` that peers are inserted in epoch descending order.
		if old.IsEpochStale(item) {
			return false, errors.Errorf("region %v's epoch shouldn't be staler than old ones %v", item, old)
		}
	}
	if len(overlaps) != 0 {
		return false, nil
	}

	t.regions[item.Region().GetId()] = item
	t.tree.ReplaceOrInsert(item)
	return true, nil
}

func (u *unsafeRecoveryController) getRecoveryPlan(storeID uint64) *pdpb.RecoveryPlan {
	if _, exists := u.storeRecoveryPlans[storeID]; !exists {
		u.storeRecoveryPlans[storeID] = &pdpb.RecoveryPlan{}
	}
	return u.storeRecoveryPlans[storeID]
}

func (u *unsafeRecoveryController) buildUpFromReports() (*regionTree, map[uint64][]*regionItem, error) {
	peersMap := make(map[uint64][]*regionItem)
	// Go through all the peer reports to build up the newest region tree
	for storeID, storeReport := range u.storeReports {
		for _, peerReport := range storeReport.PeerReports {
			item := &regionItem{report: peerReport, storeID: storeID}
			peersMap[item.Region().GetId()] = append(peersMap[item.Region().GetId()], item)
		}
	}

	// find the report of the leader
	newestPeerReports := make([]*regionItem, 0, len(peersMap))
	for _, peers := range peersMap {
		var latest *regionItem
		for _, peer := range peers {
			if latest == nil || latest.IsEpochStale(peer) {
				latest = peer
			}
		}
		if !latest.IsInitialized() {
			// ignore the uninitialized peer
			continue
		}
		newestPeerReports = append(newestPeerReports, latest)
	}

	// sort in descending order of epoch
	sort.SliceStable(newestPeerReports, func(i, j int) bool {
		return newestPeerReports[j].IsEpochStale(newestPeerReports[i])
	})

	newestRegionTree := newRegionTree()
	for _, peer := range newestPeerReports {
		_, err := newestRegionTree.insert(peer)
		if err != nil {
			return nil, nil, err
		}
	}
	return newestRegionTree, peersMap, nil
}

func (u *unsafeRecoveryController) selectLeader(peersMap map[uint64][]*regionItem, region *metapb.Region) *regionItem {
	var leader *regionItem
	for _, peer := range peersMap[region.GetId()] {
		if leader == nil || leader.IsRaftStale(peer, u) {
			leader = peer
		}
	}
	return leader
}

func (u *unsafeRecoveryController) generateTombstoneTiFlashLearnerPlan(newestRegionTree *regionTree, peersMap map[uint64][]*regionItem) bool {
	if u.err != nil {
		return false
	}
	hasPlan := false

	newestRegionTree.tree.Ascend(func(item btree.Item) bool {
		region := item.(*regionItem).Region()
		if !u.canElectLeader(region, false) {
			leader := u.selectLeader(peersMap, region)
			if leader == nil {
				u.err = errors.Errorf("can't select leader for region %d: %v", region.GetId(), logutil.RedactStringer(core.RegionToHexMeta(region)))
				return false
			}
			storeID := leader.storeID
			if u.cluster.GetStore(storeID).IsTiFlash() {
				// tombstone the tiflash learner, as it can't be leader
				storeRecoveryPlan := u.getRecoveryPlan(storeID)
				storeRecoveryPlan.Tombstones = append(storeRecoveryPlan.Tombstones, region.GetId())
				u.recordAffectedRegion(region)
				hasPlan = true
			}
		}
		return true
	})
	return hasPlan
}

func (u *unsafeRecoveryController) generateForceLeaderPlan(newestRegionTree *regionTree, peersMap map[uint64][]*regionItem, forCommitMerge bool) bool {
	if u.err != nil {
		return false
	}
	hasPlan := false

	hasForceLeader := func(region *metapb.Region) bool {
		for _, peer := range peersMap[region.GetId()] {
			if peer.report.IsForceLeader {
				return true
			}
		}
		return false
	}

	// Check the regions in newest Region Tree to see if it can still elect leader
	// considering the failed stores
	newestRegionTree.tree.Ascend(func(item btree.Item) bool {
		report := item.(*regionItem).report
		region := item.(*regionItem).Region()
		if !u.canElectLeader(region, false) {
			if hasForceLeader(region) {
				// already is a force leader, skip
				return true
			}
			if forCommitMerge && !report.HasCommitMerge {
				// check force leader only for ones has commit merge to avoid the case that
				// target region can't catch up log for the source region due to force leader
				// propose an empty raft log on being leader
				return true
			} else if !forCommitMerge && report.HasCommitMerge {
				u.err = errors.Errorf("unexpected commit merge state for report %v", report)
				return false
			}
			// the peer with largest log index/term may have lower commit/apply index, namely, lower epoch version
			// so find which peer should to be the leader instead of using peer info in the region tree.
			leader := u.selectLeader(peersMap, region)
			if leader == nil {
				u.err = errors.Errorf("can't select leader for region %d: %v", region.GetId(), logutil.RedactStringer(core.RegionToHexMeta(region)))
				return false
			}
			storeRecoveryPlan := u.getRecoveryPlan(leader.storeID)
			if storeRecoveryPlan.ForceLeader == nil {
				storeRecoveryPlan.ForceLeader = &pdpb.ForceLeader{}
				for store := range u.failedStores {
					storeRecoveryPlan.ForceLeader.FailedStores = append(storeRecoveryPlan.ForceLeader.FailedStores, store)
				}
			}
			storeRecoveryPlan.ForceLeader.EnterForceLeaders = append(storeRecoveryPlan.ForceLeader.EnterForceLeaders, region.GetId())
			u.recordAffectedRegion(leader.Region())
			hasPlan = true
		}
		return true
	})

	if hasPlan {
		for storeID := range u.storeReports {
			plan := u.getRecoveryPlan(storeID)
			if plan.ForceLeader == nil {
				// Fill an empty force leader plan to the stores that doesn't have any force leader plan
				// to avoid exiting existing force leaders.
				plan.ForceLeader = &pdpb.ForceLeader{}
			}
		}
	}

	return hasPlan
}

func (u *unsafeRecoveryController) generateDemoteFailedVoterPlan(newestRegionTree *regionTree, peersMap map[uint64][]*regionItem) bool {
	if u.err != nil {
		return false
	}
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
		if !u.canElectLeader(region, false) {
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
			u.recordAffectedRegion(leader.Region())
			hasPlan = true
		}
		return true
	})

	// Tombstone the peers of region not presented in the newest region tree
	for storeID, storeReport := range u.storeReports {
		for _, peerReport := range storeReport.PeerReports {
			region := peerReport.GetRegionState().Region
			if !newestRegionTree.contains(region.GetId()) {
				if !u.canElectLeader(region, false) {
					// the peer is not in the valid regions, should be deleted directly
					storeRecoveryPlan := u.getRecoveryPlan(storeID)
					storeRecoveryPlan.Tombstones = append(storeRecoveryPlan.Tombstones, region.GetId())
					u.recordAffectedRegion(region)
					hasPlan = true
				}
			}
		}
	}
	return hasPlan
}

func (u *unsafeRecoveryController) generateCreateEmptyRegionPlan(newestRegionTree *regionTree, peersMap map[uint64][]*regionItem) bool {
	if u.err != nil {
		return false
	}
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

	getRandomStoreID := func() uint64 {
		for storeID := range u.storeReports {
			if !u.cluster.GetStore(storeID).IsTiFlash() {
				return storeID
			}
		}
		return 0
	}

	// There may be ranges that are covered by no one. Find these empty ranges, create new
	// regions that cover them and evenly distribute newly created regions among all stores.
	lastEnd := []byte("")
	var lastStoreID uint64
	newestRegionTree.tree.Ascend(func(item btree.Item) bool {
		region := item.(*regionItem).Region()
		storeID := item.(*regionItem).storeID
		if !bytes.Equal(region.StartKey, lastEnd) {
			if u.cluster.GetStore(storeID).IsTiFlash() {
				storeID = getRandomStoreID()
				// can't create new region on tiflash store, choose a random one
				if storeID == 0 {
					u.err = errors.New("can't find available store(exclude tiflash) to create new region")
					return false
				}
			}
			newRegion, err := createRegion(lastEnd, region.StartKey, storeID)
			if err != nil {
				u.err = err
				return false
			}
			// paranoid check: shouldn't overlap with any of the peers
			for _, peers := range peersMap {
				for _, peer := range peers {
					if !peer.IsInitialized() {
						continue
					}
					if (bytes.Compare(newRegion.StartKey, peer.Region().StartKey) <= 0 &&
						(len(newRegion.EndKey) == 0 || bytes.Compare(peer.Region().StartKey, newRegion.EndKey) < 0)) ||
						((len(peer.Region().EndKey) == 0 || bytes.Compare(newRegion.StartKey, peer.Region().EndKey) < 0) &&
							(len(newRegion.EndKey) == 0 || (len(peer.Region().EndKey) != 0 && bytes.Compare(peer.Region().EndKey, newRegion.EndKey) <= 0))) {
						u.err = errors.Errorf(
							"Find overlap peer %v with newly created empty region %v",
							logutil.RedactStringer(core.RegionToHexMeta(peer.Region())),
							logutil.RedactStringer(core.RegionToHexMeta(newRegion)),
						)
						return false
					}
				}
			}
			storeRecoveryPlan := u.getRecoveryPlan(storeID)
			storeRecoveryPlan.Creates = append(storeRecoveryPlan.Creates, newRegion)
			u.recordAffectedRegion(newRegion)
			hasPlan = true
		}
		lastEnd = region.EndKey
		lastStoreID = storeID
		return true
	})
	if u.err != nil {
		return false
	}

	if !bytes.Equal(lastEnd, []byte("")) || newestRegionTree.size() == 0 {
		if lastStoreID == 0 {
			// the last store id is invalid, so choose a random one
			lastStoreID = getRandomStoreID()
			if lastStoreID == 0 {
				u.err = errors.New("can't find available store(exclude tiflash) to create new region")
				return false
			}
		}
		newRegion, err := createRegion(lastEnd, []byte(""), lastStoreID)
		if err != nil {
			u.err = err
			return false
		}
		storeRecoveryPlan := u.getRecoveryPlan(lastStoreID)
		storeRecoveryPlan.Creates = append(storeRecoveryPlan.Creates, newRegion)
		u.recordAffectedRegion(newRegion)
		hasPlan = true
	}
	return hasPlan
}

func (u *unsafeRecoveryController) generateExitForceLeaderPlan() bool {
	hasPlan := false
	for storeID, storeReport := range u.storeReports {
		for _, peerReport := range storeReport.PeerReports {
			if peerReport.IsForceLeader {
				_ = u.getRecoveryPlan(storeID)
				hasPlan = true
				break
			}
		}
	}
	return hasPlan
}
