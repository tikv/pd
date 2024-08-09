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

package unsaferecovery

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
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.uber.org/zap"
)

// stage is the stage of unsafe recovery.
type stage int

const (
	storeRequestInterval = time.Second * 40
)

// Stage transition graph: for more details, please check `Controller.HandleStoreHeartbeat()`
//
//	                    +-----------+           +-----------+
//	+-----------+       |           |           |           |
//	|           |       |  Collect  |           | Tombstone |
//	|   Idle    |------>|  Report   |-----+---->|  Tiflash  |-----+
//	|           |       |           |     |     |  Learner  |     |
//	+-----------+       +-----------+     |     |           |     |
//	                                      |     +-----------+     |
//	                                      |           |           |
//	                                      |           |           |
//	                                      |           v           |
//	                                      |     +-----------+     |
//	                                      |     |           |     |
//	                                      |     |   Force   |     |
//	                                      |     | LeaderFor |-----+
//	                                      |     |CommitMerge|     |
//	                                      |     |           |     |
//	                                      |     +-----------+     |
//	                                      |           |           |
//	                                      |           |           |
//	                                      |           v           |
//	                                      |     +-----------+     |     +-----------+
//	                                      |     |           |     |     |           |        +-----------+
//	                                      |     |  Force    |     |     | exitForce |        |           |
//	                                      |     |  Leader   |-----+---->|  Leader   |------->|  Failed   |
//	                                      |     |           |     |     |           |        |           |
//	                                      |     +-----------+     |     +-----------+        +-----------+
//	                                      |           |           |
//	                                      |           |           |
//	                                      |           v           |
//	                                      |     +-----------+     |
//	                                      |     |           |     |
//	                                      |     |  Demote   |     |
//	                                      +-----|  Voter    |-----|
//	                                            |           |     |
//	                                            +-----------+     |
//	                                                  |           |
//	                                                  |           |
//	                                                  v           |
//	                    +-----------+           +-----------+     |
//	+-----------+       |           |           |           |     |
//	|           |       | ExitForce |           |  Create   |     |
//	| Finished  |<------|  Leader   |<----------|  Region   |-----+
//	|           |       |           |           |           |
//	+-----------+       +-----------+           +-----------+
const (
	Idle stage = iota
	CollectReport
	TombstoneTiFlashLearner
	ForceLeaderForCommitMerge
	ForceLeader
	DemoteFailedVoter
	CreateEmptyRegion
	ExitForceLeader
	Finished
	Failed
)

type cluster interface {
	core.StoreSetInformer

	ResetRegionCache()
	AllocID() (uint64, error)
	BuryStore(storeID uint64, forceBury bool) error
	GetSchedulerConfig() sc.SchedulerConfigProvider
}

// Controller is used to control the unsafe recovery process.
type Controller struct {
	syncutil.RWMutex

	cluster cluster
	stage   stage
	// the round of recovery, which is an increasing number to identify the reports of each round
	step         uint64
	failedStores map[uint64]struct{}
	timeout      time.Time
	autoDetect   bool

	// collected reports from store, if not reported yet, it would be nil
	storeReports      map[uint64]*pdpb.StoreReport
	numStoresReported int

	storePlanExpires   map[uint64]time.Time
	storeRecoveryPlans map[uint64]*pdpb.RecoveryPlan

	// accumulated output for the whole recovery process
	output []StageOutput
	// exposed to the outside for testing
	AffectedTableIDs    map[int64]struct{}
	affectedMetaRegions map[uint64]struct{}
	newlyCreatedRegions map[uint64]struct{}
	err                 error
}

// StageOutput is the information for one stage of the recovery process.
type StageOutput struct {
	Info    string              `json:"info,omitempty"`
	Time    string              `json:"time,omitempty"`
	Actions map[string][]string `json:"actions,omitempty"`
	Details []string            `json:"details,omitempty"`
}

// NewController creates a new Controller.
func NewController(cluster cluster) *Controller {
	u := &Controller{
		cluster: cluster,
	}
	u.reset()
	return u
}

func (u *Controller) reset() {
	u.stage = Idle
	u.step = 0
	u.failedStores = make(map[uint64]struct{})
	u.storeReports = make(map[uint64]*pdpb.StoreReport)
	u.numStoresReported = 0
	u.storePlanExpires = make(map[uint64]time.Time)
	u.storeRecoveryPlans = make(map[uint64]*pdpb.RecoveryPlan)
	u.output = make([]StageOutput, 0)
	u.AffectedTableIDs = make(map[int64]struct{}, 0)
	u.affectedMetaRegions = make(map[uint64]struct{}, 0)
	u.newlyCreatedRegions = make(map[uint64]struct{}, 0)
	u.err = nil
}

// IsRunning returns whether there is ongoing unsafe recovery process. If yes, further unsafe
// recovery requests, schedulers, checkers, AskSplit and AskBatchSplit requests are blocked.
func (u *Controller) IsRunning() bool {
	u.RLock()
	defer u.RUnlock()
	return isRunning(u.stage)
}

func isRunning(s stage) bool {
	return s != Idle && s != Finished && s != Failed
}

// RemoveFailedStores removes Failed stores from the cluster.
func (u *Controller) RemoveFailedStores(failedStores map[uint64]struct{}, timeout uint64, autoDetect bool) error {
	u.Lock()
	defer u.Unlock()

	if isRunning(u.stage) {
		return errs.ErrUnsafeRecoveryIsRunning.FastGenByArgs()
	}

	if !autoDetect {
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
	u.autoDetect = autoDetect
	u.changeStage(CollectReport)
	return nil
}

// Show returns the current status of ongoing unsafe recover operation.
func (u *Controller) Show() []StageOutput {
	u.Lock()
	defer u.Unlock()

	if u.stage == Idle {
		return []StageOutput{{Info: "No on-going recovery."}}
	}
	if err := u.checkTimeout(); err != nil {
		u.handleErr(err)
	}
	status := u.output
	if u.stage != Finished && u.stage != Failed {
		status = append(status, u.getReportStatus())
	}
	return status
}

func (u *Controller) getReportStatus() StageOutput {
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

func (u *Controller) checkTimeout() error {
	if u.stage == Finished || u.stage == Failed {
		return nil
	}

	if time.Now().After(u.timeout) {
		return errors.Errorf("Exceeds timeout %v", u.timeout)
	}
	return nil
}

// handleErr handles the error occurred during the unsafe recovery process.
func (u *Controller) handleErr(err error) bool {
	// Keep the earliest error.
	if u.err == nil {
		u.err = err
	}
	if u.stage == ExitForceLeader {
		// We already tried to exit force leader, and it still Failed.
		// We turn into Failed stage directly. TiKV will step down force leader
		// automatically after being for a long time.
		u.changeStage(Failed)
		return true
	}
	// When encountering an error for the first time, we will try to exit force
	// leader before turning into Failed stage to avoid the leaking force leaders
	// blocks reads and writes.
	u.storePlanExpires = make(map[uint64]time.Time)
	u.storeRecoveryPlans = make(map[uint64]*pdpb.RecoveryPlan)
	u.timeout = time.Now().Add(storeRequestInterval * 2)
	// empty recovery plan would trigger exit force leader
	u.changeStage(ExitForceLeader)
	return false
}

// HandleStoreHeartbeat handles the store heartbeat requests and checks whether the stores need to
// send detailed report back.
func (u *Controller) HandleStoreHeartbeat(heartbeat *pdpb.StoreHeartbeatRequest, resp *pdpb.StoreHeartbeatResponse) {
	u.Lock()
	defer u.Unlock()

	if !isRunning(u.stage) {
		// no recovery in progress, do nothing
		return
	}

	done, err := func() (bool, error) {
		if err := u.checkTimeout(); err != nil {
			return false, err
		}

		allCollected, err := u.CollectReport(heartbeat)
		if err != nil {
			return false, err
		}

		if allCollected {
			newestRegionTree, peersMap, err := u.buildUpFromReports()
			if err != nil {
				return false, err
			}

			return u.generatePlan(newestRegionTree, peersMap)
		}
		return false, nil
	}()

	if done || (err != nil && u.handleErr(err)) {
		return
	}
	u.dispatchPlan(heartbeat, resp)
}

func (u *Controller) generatePlan(newestRegionTree *regionTree, peersMap map[uint64][]*regionItem) (bool, error) {
	// clean up previous plan
	u.storePlanExpires = make(map[uint64]time.Time)
	u.storeRecoveryPlans = make(map[uint64]*pdpb.RecoveryPlan)

	stage := u.stage
	reCheck := false
	hasPlan := false
	var err error
	for {
		switch stage {
		case CollectReport:
			fallthrough
		case TombstoneTiFlashLearner:
			if hasPlan, err = u.generateTombstoneTiFlashLearnerPlan(newestRegionTree, peersMap); hasPlan && err == nil {
				u.changeStage(TombstoneTiFlashLearner)
				break
			}
			if err != nil {
				break
			}
			fallthrough
		case ForceLeaderForCommitMerge:
			if hasPlan, err = u.generateForceLeaderPlan(newestRegionTree, peersMap, true); hasPlan && err == nil {
				u.changeStage(ForceLeaderForCommitMerge)
				break
			}
			if err != nil {
				break
			}
			fallthrough
		case ForceLeader:
			if hasPlan, err = u.generateForceLeaderPlan(newestRegionTree, peersMap, false); hasPlan && err == nil {
				u.changeStage(ForceLeader)
				break
			}
			if err != nil {
				break
			}
			fallthrough
		case DemoteFailedVoter:
			if hasPlan = u.generateDemoteFailedVoterPlan(newestRegionTree, peersMap); hasPlan {
				u.changeStage(DemoteFailedVoter)
				break
			} else if !reCheck {
				reCheck = true
				stage = TombstoneTiFlashLearner
				continue
			}
			fallthrough
		case CreateEmptyRegion:
			if hasPlan, err = u.generateCreateEmptyRegionPlan(newestRegionTree, peersMap); hasPlan && err == nil {
				u.changeStage(CreateEmptyRegion)
				break
			}
			if err != nil {
				break
			}
			fallthrough
		case ExitForceLeader:
			if hasPlan = u.generateExitForceLeaderPlan(); hasPlan {
				u.changeStage(ExitForceLeader)
			}
		default:
			panic("unreachable")
		}
		break
	}

	if err == nil && !hasPlan {
		if u.err != nil {
			u.changeStage(Failed)
		} else {
			u.changeStage(Finished)
		}
		return true, nil
	}
	return false, err
}

// It dispatches recovery plan if any.
func (u *Controller) dispatchPlan(heartbeat *pdpb.StoreHeartbeatRequest, resp *pdpb.StoreHeartbeatResponse) {
	storeID := heartbeat.Stats.StoreId
	now := time.Now()

	if reported, exist := u.storeReports[storeID]; reported != nil || !exist {
		// the plan has been executed, no need to dispatch again
		// or no need to dispatch plan to this store(e.g. Tiflash)
		return
	}

	if expire, dispatched := u.storePlanExpires[storeID]; !dispatched || expire.Before(now) {
		if dispatched {
			log.Info("unsafe recovery store recovery plan execution timeout, retry", zap.Uint64("store-id", storeID))
		}
		// Dispatch the recovery plan to the store, and the plan may be empty.
		resp.RecoveryPlan = u.getRecoveryPlan(storeID)
		resp.RecoveryPlan.Step = u.step
		u.storePlanExpires[storeID] = now.Add(storeRequestInterval)
	}
}

// CollectReport collects and checks if store reports have been fully collected.
func (u *Controller) CollectReport(heartbeat *pdpb.StoreHeartbeatRequest) (bool, error) {
	storeID := heartbeat.Stats.StoreId
	if _, isFailedStore := u.failedStores[storeID]; isFailedStore {
		return false, errors.Errorf("Receive heartbeat from Failed store %d", storeID)
	}

	if heartbeat.StoreReport == nil {
		return false, nil
	}

	if heartbeat.StoreReport.GetStep() != u.step {
		log.Info("unsafe recovery receives invalid store report",
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

// GetStage gets the stage of the current unsafe recovery.
func (u *Controller) GetStage() stage {
	u.RLock()
	defer u.RUnlock()
	return u.stage
}

func (u *Controller) changeStage(stage stage) {
	// If the running stage changes, update the scheduling allowance status to add or remove "online-unsafe-recovery" halt.
	if running := isRunning(stage); running != isRunning(u.stage) {
		u.cluster.GetSchedulerConfig().SetSchedulingAllowanceStatus(running, "online-unsafe-recovery")
	}
	u.stage = stage

	var output StageOutput
	output.Time = time.Now().Format("2006-01-02 15:04:05.000")
	switch u.stage {
	case Idle:
	case CollectReport:
		// TODO: clean up existing operators
		output.Info = "Unsafe recovery enters collect report stage"
		if u.autoDetect {
			output.Details = append(output.Details, "auto detect mode with no specified Failed stores")
		} else {
			stores := ""
			count := 0
			for store := range u.failedStores {
				count += 1
				stores += fmt.Sprintf("%d", store)
				if count != len(u.failedStores) {
					stores += ", "
				}
			}
			output.Details = append(output.Details, fmt.Sprintf("Failed stores %s", stores))
		}

	case TombstoneTiFlashLearner:
		output.Info = "Unsafe recovery enters tombstone TiFlash learner stage"
		output.Actions = u.getTombstoneTiFlashLearnerDigest()
	case ForceLeaderForCommitMerge:
		output.Info = "Unsafe recovery enters force leader for commit merge stage"
		output.Actions = u.getForceLeaderPlanDigest()
	case ForceLeader:
		output.Info = "Unsafe recovery enters force leader stage"
		output.Actions = u.getForceLeaderPlanDigest()
	case DemoteFailedVoter:
		output.Info = "Unsafe recovery enters demote Failed voter stage"
		output.Actions = u.getDemoteFailedVoterPlanDigest()
	case CreateEmptyRegion:
		output.Info = "Unsafe recovery enters create empty region stage"
		output.Actions = u.getCreateEmptyRegionPlanDigest()
	case ExitForceLeader:
		output.Info = "Unsafe recovery enters exit force leader stage"
		if u.err != nil {
			output.Details = append(output.Details, fmt.Sprintf("triggered by error: %v", u.err.Error()))
		}
	case Finished:
		if u.step > 1 {
			// == 1 means no operation has done, no need to invalid cache
			u.cluster.ResetRegionCache()
		}
		output.Info = "Unsafe recovery Finished"
		output.Details = u.getAffectedTableDigest()
		u.storePlanExpires = make(map[uint64]time.Time)
		u.storeRecoveryPlans = make(map[uint64]*pdpb.RecoveryPlan)
	case Failed:
		output.Info = fmt.Sprintf("Unsafe recovery Failed: %v", u.err)
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
		log.Error("unsafe recovery fail to marshal json object", zap.Error(err))
	} else {
		log.Info(string(data))
	}

	// reset store reports to nil instead of delete, because it relays on the item
	// to decide which store it needs to collect the report from.
	for k := range u.storeReports {
		u.storeReports[k] = nil
	}
	u.numStoresReported = 0
	u.step += 1
}

func (u *Controller) getForceLeaderPlanDigest() map[string][]string {
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

func (u *Controller) getDemoteFailedVoterPlanDigest() map[string][]string {
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

func (u *Controller) getTombstoneTiFlashLearnerDigest() map[string][]string {
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

func (u *Controller) getCreateEmptyRegionPlanDigest() map[string][]string {
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

func (u *Controller) getAffectedTableDigest() []string {
	var details []string
	if len(u.affectedMetaRegions) != 0 {
		regions := ""
		for r := range u.affectedMetaRegions {
			regions += fmt.Sprintf("%d, ", r)
		}
		details = append(details, "affected meta regions: "+strings.Trim(regions, ", "))
	}
	if len(u.AffectedTableIDs) != 0 {
		tables := ""
		for t := range u.AffectedTableIDs {
			tables += fmt.Sprintf("%d, ", t)
		}
		details = append(details, "affected table ids: "+strings.Trim(tables, ", "))
	}
	if len(u.newlyCreatedRegions) != 0 {
		regions := ""
		for r := range u.newlyCreatedRegions {
			regions += fmt.Sprintf("%d, ", r)
		}
		details = append(details, "newly created empty regions: "+strings.Trim(regions, ", "))
	} else {
		details = append(details, "no newly created empty regions")
	}
	return details
}

func (u *Controller) recordAffectedRegion(region *metapb.Region) {
	isMeta, tableID := codec.Key(region.StartKey).MetaOrTable()
	if isMeta {
		u.affectedMetaRegions[region.GetId()] = struct{}{}
	} else if tableID != 0 {
		u.AffectedTableIDs[tableID] = struct{}{}
	}
}

func (u *Controller) isFailed(peer *metapb.Peer) bool {
	_, isFailed := u.failedStores[peer.StoreId]
	_, isLive := u.storeReports[peer.StoreId]
	if isFailed || (u.autoDetect && !isLive) {
		return true
	}
	return false
}

func (u *Controller) canElectLeader(region *metapb.Region, onlyIncoming bool) bool {
	hasQuorum := func(voters []*metapb.Peer) bool {
		numFailedVoters := 0
		numLiveVoters := 0

		for _, voter := range voters {
			if u.isFailed(voter) {
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

func (u *Controller) getFailedPeers(region *metapb.Region) []*metapb.Peer {
	// if it can form a quorum after exiting the joint state, then no need to demotes any peer
	if u.canElectLeader(region, true) {
		return nil
	}

	var failedPeers []*metapb.Peer
	for _, peer := range region.Peers {
		if u.isFailed(peer) {
			failedPeers = append(failedPeers, peer)
		}
	}
	return failedPeers
}

type regionItem struct {
	report  *pdpb.PeerReport
	storeID uint64
}

// Less returns true if the region start key is less than the other.
func (r *regionItem) Less(other *regionItem) bool {
	left := r.region().GetStartKey()
	right := other.region().GetStartKey()
	return bytes.Compare(left, right) < 0
}

func (r *regionItem) contains(key []byte) bool {
	start, end := r.region().GetStartKey(), r.region().GetEndKey()
	return bytes.Compare(key, start) >= 0 && (len(end) == 0 || bytes.Compare(key, end) < 0)
}

func (r *regionItem) region() *metapb.Region {
	return r.report.GetRegionState().GetRegion()
}

func (r *regionItem) isInitialized() bool {
	return len(r.region().Peers) != 0
}

func (r *regionItem) isEpochStale(other *regionItem) bool {
	re := r.region().GetRegionEpoch()
	oe := other.region().GetRegionEpoch()
	return re.GetVersion() < oe.GetVersion() || (re.GetVersion() == oe.GetVersion() && re.GetConfVer() < oe.GetConfVer())
}

func (r *regionItem) isRaftStale(origin *regionItem, u *Controller) bool {
	cmps := []func(a, b *regionItem) int{
		func(a, b *regionItem) int {
			return int(a.report.GetRaftState().GetHardState().GetTerm()) - int(b.report.GetRaftState().GetHardState().GetTerm())
		},
		// choose the peer has maximum applied index or last index.
		func(a, b *regionItem) int {
			maxIdxA := typeutil.MaxUint64(a.report.GetRaftState().GetLastIndex(), a.report.AppliedIndex)
			maxIdxB := typeutil.MaxUint64(b.report.GetRaftState().GetLastIndex(), b.report.AppliedIndex)
			return int(maxIdxA - maxIdxB)
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
			for _, peer := range a.region().GetPeers() {
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
	tree    *btree.BTreeG[*regionItem]
}

func newRegionTree() *regionTree {
	return &regionTree{
		regions: make(map[uint64]*regionItem),
		tree:    btree.NewG[*regionItem](defaultBTreeDegree),
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

	end := item.region().GetEndKey()
	var overlaps []*regionItem
	t.tree.AscendGreaterOrEqual(result, func(i *regionItem) bool {
		over := i
		if len(end) > 0 && bytes.Compare(end, over.region().GetStartKey()) <= 0 {
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
	t.tree.DescendLessOrEqual(item, func(i *regionItem) bool {
		result = i
		return false
	})

	if result == nil || !result.contains(item.region().GetStartKey()) {
		return nil
	}

	return result
}

// Insert the peer report of one region int the tree.
// It finds and deletes all the overlapped regions first, and then
// insert the new region.
func (t *regionTree) insert(item *regionItem) (bool, error) {
	overlaps := t.getOverlaps(item)

	if t.contains(item.region().GetId()) {
		// it's ensured by the `buildUpFromReports` that only insert the latest peer of one region.
		return false, errors.Errorf("region %v shouldn't be updated twice", item.region().GetId())
	}

	for _, newer := range overlaps {
		log.Info("unsafe recovery found overlap regions", logutil.ZapRedactStringer("newer-region-meta", core.RegionToHexMeta(newer.region())), logutil.ZapRedactStringer("older-region-meta", core.RegionToHexMeta(item.region())))
		// it's ensured by the `buildUpFromReports` that peers are inserted in epoch descending order.
		if newer.isEpochStale(item) {
			return false, errors.Errorf("region %v's epoch shouldn't be staler than old ones %v", item, newer)
		}
	}
	if len(overlaps) != 0 {
		return false, nil
	}

	t.regions[item.region().GetId()] = item
	t.tree.ReplaceOrInsert(item)
	return true, nil
}

func (u *Controller) getRecoveryPlan(storeID uint64) *pdpb.RecoveryPlan {
	if _, exists := u.storeRecoveryPlans[storeID]; !exists {
		u.storeRecoveryPlans[storeID] = &pdpb.RecoveryPlan{}
	}
	return u.storeRecoveryPlans[storeID]
}

func (u *Controller) buildUpFromReports() (*regionTree, map[uint64][]*regionItem, error) {
	peersMap := make(map[uint64][]*regionItem)
	// Go through all the peer reports to build up the newest region tree
	for storeID, storeReport := range u.storeReports {
		for _, peerReport := range storeReport.PeerReports {
			item := &regionItem{report: peerReport, storeID: storeID}
			peersMap[item.region().GetId()] = append(peersMap[item.region().GetId()], item)
		}
	}

	// find the report of the leader
	newestPeerReports := make([]*regionItem, 0, len(peersMap))
	for _, peers := range peersMap {
		var latest *regionItem
		for _, peer := range peers {
			if latest == nil || latest.isEpochStale(peer) {
				latest = peer
			}
		}
		if !latest.isInitialized() {
			// ignore the uninitialized peer
			continue
		}
		newestPeerReports = append(newestPeerReports, latest)
	}

	// sort in descending order of epoch
	sort.SliceStable(newestPeerReports, func(i, j int) bool {
		return newestPeerReports[j].isEpochStale(newestPeerReports[i])
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

func (u *Controller) selectLeader(peersMap map[uint64][]*regionItem, region *metapb.Region) *regionItem {
	var leader *regionItem
	for _, peer := range peersMap[region.GetId()] {
		if leader == nil || leader.isRaftStale(peer, u) {
			leader = peer
		}
	}
	return leader
}

func (u *Controller) generateTombstoneTiFlashLearnerPlan(newestRegionTree *regionTree, peersMap map[uint64][]*regionItem) (bool, error) {
	if u.err != nil {
		return false, nil
	}
	hasPlan := false

	var err error
	newestRegionTree.tree.Ascend(func(item *regionItem) bool {
		region := item.region()
		if !u.canElectLeader(region, false) {
			leader := u.selectLeader(peersMap, region)
			if leader == nil {
				err = errors.Errorf("can't select leader for region %d: %v", region.GetId(), logutil.RedactStringer(core.RegionToHexMeta(region)))
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
	return hasPlan, err
}

func (u *Controller) generateForceLeaderPlan(newestRegionTree *regionTree, peersMap map[uint64][]*regionItem, forCommitMerge bool) (bool, error) {
	if u.err != nil {
		return false, nil
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

	var err error
	// Check the regions in newest Region Tree to see if it can still elect leader
	// considering the Failed stores
	newestRegionTree.tree.Ascend(func(item *regionItem) bool {
		report := item.report
		region := item.region()
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
				err = errors.Errorf("unexpected commit merge state for report %v", report)
				return false
			}
			// the peer with largest log index/term may have lower commit/apply index, namely, lower epoch version
			// so find which peer should to be the leader instead of using peer info in the region tree.
			leader := u.selectLeader(peersMap, region)
			if leader == nil {
				err = errors.Errorf("can't select leader for region %d: %v", region.GetId(), logutil.RedactStringer(core.RegionToHexMeta(region)))
				return false
			}
			storeRecoveryPlan := u.getRecoveryPlan(leader.storeID)
			if storeRecoveryPlan.ForceLeader == nil {
				storeRecoveryPlan.ForceLeader = &pdpb.ForceLeader{}
				for store := range u.failedStores {
					storeRecoveryPlan.ForceLeader.FailedStores = append(storeRecoveryPlan.ForceLeader.FailedStores, store)
				}
			}
			if u.autoDetect {
				// For auto detect, the failedStores is empty. So need to add the detected Failed store to the list
				for _, peer := range u.getFailedPeers(leader.region()) {
					found := false
					for _, store := range storeRecoveryPlan.ForceLeader.FailedStores {
						if store == peer.StoreId {
							found = true
							break
						}
					}
					if !found {
						storeRecoveryPlan.ForceLeader.FailedStores = append(storeRecoveryPlan.ForceLeader.FailedStores, peer.StoreId)
					}
				}
			}
			storeRecoveryPlan.ForceLeader.EnterForceLeaders = append(storeRecoveryPlan.ForceLeader.EnterForceLeaders, region.GetId())
			u.recordAffectedRegion(leader.region())
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

	return hasPlan, err
}

func (u *Controller) generateDemoteFailedVoterPlan(newestRegionTree *regionTree, peersMap map[uint64][]*regionItem) bool {
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
	// considering the Failed stores
	newestRegionTree.tree.Ascend(func(item *regionItem) bool {
		region := item.region()
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
					FailedVoters: u.getFailedPeers(leader.region()),
				},
			)
			u.recordAffectedRegion(leader.region())
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

func (u *Controller) generateCreateEmptyRegionPlan(newestRegionTree *regionTree, peersMap map[uint64][]*regionItem) (bool, error) {
	if u.err != nil {
		return false, nil
	}
	hasPlan := false

	createRegion := func(startKey, endKey []byte, storeID uint64) (*metapb.Region, error) {
		regionID, err := u.cluster.AllocID()
		if err != nil {
			return nil, err
		}
		peerID, err := u.cluster.AllocID()
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

	var err error
	// There may be ranges that are covered by no one. Find these empty ranges, create new
	// regions that cover them and evenly distribute newly created regions among all stores.
	lastEnd := []byte("")
	var lastStoreID uint64
	newestRegionTree.tree.Ascend(func(item *regionItem) bool {
		region := item.region()
		storeID := item.storeID
		if !bytes.Equal(region.StartKey, lastEnd) {
			if u.cluster.GetStore(storeID).IsTiFlash() {
				storeID = getRandomStoreID()
				// can't create new region on tiflash store, choose a random one
				if storeID == 0 {
					err = errors.New("can't find available store(exclude tiflash) to create new region")
					return false
				}
			}
			newRegion, createRegionErr := createRegion(lastEnd, region.StartKey, storeID)
			if createRegionErr != nil {
				err = createRegionErr
				return false
			}
			// paranoid check: shouldn't overlap with any of the peers
			for _, peers := range peersMap {
				for _, peer := range peers {
					if !peer.isInitialized() {
						continue
					}
					if (bytes.Compare(newRegion.StartKey, peer.region().StartKey) <= 0 &&
						(len(newRegion.EndKey) == 0 || bytes.Compare(peer.region().StartKey, newRegion.EndKey) < 0)) ||
						((len(peer.region().EndKey) == 0 || bytes.Compare(newRegion.StartKey, peer.region().EndKey) < 0) &&
							(len(newRegion.EndKey) == 0 || (len(peer.region().EndKey) != 0 && bytes.Compare(peer.region().EndKey, newRegion.EndKey) <= 0))) {
						err = errors.Errorf(
							"Find overlap peer %v with newly created empty region %v",
							logutil.RedactStringer(core.RegionToHexMeta(peer.region())),
							logutil.RedactStringer(core.RegionToHexMeta(newRegion)),
						)
						return false
					}
				}
			}
			storeRecoveryPlan := u.getRecoveryPlan(storeID)
			storeRecoveryPlan.Creates = append(storeRecoveryPlan.Creates, newRegion)
			u.recordAffectedRegion(newRegion)
			u.newlyCreatedRegions[newRegion.GetId()] = struct{}{}
			hasPlan = true
		}
		lastEnd = region.EndKey
		lastStoreID = storeID
		return true
	})
	if err != nil {
		return false, err
	}

	if !bytes.Equal(lastEnd, []byte("")) || newestRegionTree.size() == 0 {
		if lastStoreID == 0 {
			// the last store id is invalid, so choose a random one
			lastStoreID = getRandomStoreID()
			if lastStoreID == 0 {
				return false, errors.New("can't find available store(exclude tiflash) to create new region")
			}
		}
		newRegion, err := createRegion(lastEnd, []byte(""), lastStoreID)
		if err != nil {
			return false, err
		}
		storeRecoveryPlan := u.getRecoveryPlan(lastStoreID)
		storeRecoveryPlan.Creates = append(storeRecoveryPlan.Creates, newRegion)
		u.recordAffectedRegion(newRegion)
		hasPlan = true
	}
	return hasPlan, nil
}

func (u *Controller) generateExitForceLeaderPlan() bool {
	hasPlan := false
	for storeID, storeReport := range u.storeReports {
		for _, peerReport := range storeReport.PeerReports {
			if peerReport.IsForceLeader {
				// empty recovery plan triggers exit force leader on TiKV side
				_ = u.getRecoveryPlan(storeID)
				hasPlan = true
				break
			}
		}
	}
	return hasPlan
}
