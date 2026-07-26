// Copyright 2021 TiKV Project Authors.
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

package unsaferecovery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

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
)

// stage is the stage of unsafe recovery.
type stage int

const (
	defaultPlanExecutionTimeout = time.Second * 60
	resetRegionCacheTimeout     = 3 * time.Second
)

var globalRecoveryStep = uint64(time.Now().UnixNano())

func nextRecoveryStep() uint64 {
	return atomic.AddUint64(&globalRecoveryStep, 1)
}

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
//	                    +-----------+       +-----------+       +-----------+     |
//	+-----------+       |           |       |           |       |           |     |
//	|           |       | Resetting |       | ExitForce |       |  Create   |     |
//	| Finished  |<------|   Cache   |<------|  Leader   |<------|  Region   |-----+
//	|           |       |           |       |           |       |           |
//	+-----------+       +-----------+       +-----------+       +-----------+
const (
	Idle stage = iota
	CollectReport
	TombstoneTiFlashLearner
	ForceLeaderForCommitMerge
	ForceLeader
	DemoteFailedVoter
	CreateEmptyRegion
	ExitForceLeader
	Resetting
	Finished
	Failed
)

type cluster interface {
	core.StoreSetInformer

	Context() context.Context
	ResetPreparedAndResetRegionCache(context.Context) error
	AllocID(uint32) (uint64, uint32, error)
	BuryStore(storeID uint64, forceBury bool) error
	GetSchedulerConfig() sc.SchedulerConfigProvider
}

// Controller is used to control the unsafe recovery process.
type Controller struct {
	syncutil.RWMutex

	cluster cluster
	stage   stage
	// the round of recovery, which is an increasing number to identify the reports of each round
	step              uint64
	recoveryStartStep uint64
	failedStores      map[uint64]struct{}
	timeout           time.Time
	autoDetect        bool
	// planExecutionTimeout is the duration PD waits for a store to execute the recovery plan
	// before dispatching the same plan again.
	planExecutionTimeout time.Duration
	// disableParanoidCheck skips the sanity check that newly created empty regions
	// don't overlap with any collected peer report.
	disableParanoidCheck bool

	// collected reports from store, if not reported yet, it would be nil
	storeReports      map[uint64]*pdpb.StoreReport
	numStoresReported int

	storePlanExpires   map[uint64]time.Time
	storeRecoveryPlans map[uint64]*pdpb.RecoveryPlan

	// Orphaned peers are the peers that exist in the forced leader but not in the target stores' reports
	// they are expected to exist when some of the peers were destroyed by TombstoneTiFlashLearner phase.
	// we need to explicitly remove them in DemoteFailedVoter phase, in case there is only one candidate store
	// for this peer, and PD is not able to remove it through the down peer removal mechanism.
	orphanedPeers map[uint64][]*metapb.Peer

	// accumulated output for the whole recovery process
	output []StageOutput
	// exposed to the outside for testing
	AffectedTableIDs     map[int64]struct{}
	affectedMetaRegions  map[uint64]struct{}
	newlyCreatedRegions  map[uint64]struct{}
	resettingRegionCache bool
	resetRegionCacheErr  error
	err                  error
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
	u.recoveryStartStep = 0
	u.failedStores = make(map[uint64]struct{})
	u.storeReports = make(map[uint64]*pdpb.StoreReport)
	u.numStoresReported = 0
	u.storePlanExpires = make(map[uint64]time.Time)
	u.storeRecoveryPlans = make(map[uint64]*pdpb.RecoveryPlan)
	u.output = make([]StageOutput, 0)
	u.AffectedTableIDs = make(map[int64]struct{}, 0)
	u.affectedMetaRegions = make(map[uint64]struct{}, 0)
	u.newlyCreatedRegions = make(map[uint64]struct{}, 0)
	u.orphanedPeers = make(map[uint64][]*metapb.Peer)
	u.resettingRegionCache = false
	u.resetRegionCacheErr = nil
	u.err = nil
	u.planExecutionTimeout = defaultPlanExecutionTimeout
	u.disableParanoidCheck = false
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

// RemoveFailedStoresOptions controls optional unsafe recovery behavior.
type RemoveFailedStoresOptions struct {
	PlanExecutionTimeout time.Duration
	DisableParanoidCheck bool
}

// RemoveFailedStores removes Failed stores from the cluster.
func (u *Controller) RemoveFailedStores(failedStores map[uint64]struct{}, timeout uint64, autoDetect bool) error {
	return u.RemoveFailedStoresWithOptions(failedStores, timeout, autoDetect, RemoveFailedStoresOptions{})
}

// RemoveFailedStoresWithOptions removes Failed stores from the cluster with options.
func (u *Controller) RemoveFailedStoresWithOptions(
	failedStores map[uint64]struct{},
	timeout uint64,
	autoDetect bool,
	options RemoveFailedStoresOptions,
) error {
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
	u.recoveryStartStep = nextRecoveryStep()
	u.step = u.recoveryStartStep
	u.failedStores = failedStores
	u.autoDetect = autoDetect
	if options.PlanExecutionTimeout > 0 {
		u.planExecutionTimeout = options.PlanExecutionTimeout
	}
	u.disableParanoidCheck = options.DisableParanoidCheck
	u.changeStage(CollectReport)
	return nil
}

// AbortFailedStoresRemoval aborts the current unsafe recovery process in a best-effort way.
// It asks TiKV to exit force leader by dispatching empty recovery plans, but any plan that
// has already been delivered to TiKV may keep running until TiKV finishes or times it out.
func (u *Controller) AbortFailedStoresRemoval() error {
	u.Lock()
	defer u.Unlock()

	if !isRunning(u.stage) {
		return errs.ErrUnsafeRecoveryInvalidInput.FastGenByArgs("no ongoing unsafe recovery")
	}
	if u.stage == Resetting {
		return errs.ErrUnsafeRecoveryInvalidInput.FastGenByArgs("unsafe recovery is resetting region cache")
	}
	if u.stage == ExitForceLeader {
		return nil
	}

	u.handleErr(errors.New("aborted by operator"))
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
	if u.stage == Resetting {
		status.Info = "Resetting region cache before finishing unsafe recovery"
		if u.resetRegionCacheErr != nil {
			status.Details = append(status.Details, fmt.Sprintf("failed to reset region cache: %v", u.resetRegionCacheErr))
		}
		return status
	}
	if u.numStoresReported != len(u.storeReports) {
		status.Info = fmt.Sprintf("Collecting reports from alive stores(%d/%d)", u.numStoresReported, len(u.storeReports))
		var (
			reportedIDs     []string
			unreportedIDs   []string
			undispatchedIDs []string
		)
		for storeID, report := range u.storeReports {
			s := strconv.FormatUint(storeID, 10)
			if report == nil {
				if _, requested := u.storePlanExpires[storeID]; !requested {
					undispatchedIDs = append(undispatchedIDs, s)
				} else {
					unreportedIDs = append(unreportedIDs, s)
				}
			} else {
				reportedIDs = append(reportedIDs, s)
			}
		}
		status.Details = append(status.Details, "Stores that have not dispatched plan: "+strings.Join(undispatchedIDs, ", "))
		status.Details = append(status.Details, "Stores that have reported to PD: "+strings.Join(reportedIDs, ", "))
		status.Details = append(status.Details, "Stores that have not reported to PD: "+strings.Join(unreportedIDs, ", "))
	} else {
		status.Info = fmt.Sprintf("Collected reports from all %d alive stores", len(u.storeReports))
	}
	return status
}

func (u *Controller) checkTimeout() error {
	if u.stage == Resetting || u.stage == Finished || u.stage == Failed {
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
	u.timeout = time.Now().Add(u.planExecutionTimeout * 2)
	// empty recovery plan would trigger exit force leader
	u.changeStage(ExitForceLeader)
	return false
}

// HandleStoreHeartbeat handles the store heartbeat requests and checks whether the stores need to
// send detailed report back.
func (u *Controller) HandleStoreHeartbeat(heartbeat *pdpb.StoreHeartbeatRequest, resp *pdpb.StoreHeartbeatResponse) {
	u.Lock()

	if !isRunning(u.stage) {
		// no recovery in progress, do nothing
		u.Unlock()
		return
	}
	if u.stage == Resetting {
		shouldReset := u.startRegionCacheReset()
		u.Unlock()
		if shouldReset {
			u.resetRegionCacheAndFinish()
		}
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
		shouldReset := u.startRegionCacheReset()
		u.Unlock()
		if shouldReset {
			u.resetRegionCacheAndFinish()
		}
		return
	}
	u.dispatchPlan(heartbeat, resp)
	u.Unlock()
}

// startRegionCacheReset marks the cache reset as in progress. The caller must
// hold the controller lock.
func (u *Controller) startRegionCacheReset() bool {
	if u.stage != Resetting || u.resettingRegionCache {
		return false
	}
	u.resettingRegionCache = true
	return true
}

func (u *Controller) resetRegionCacheAndFinish() {
	parentCtx := u.cluster.Context()
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	ctx, cancel := context.WithTimeout(parentCtx, resetRegionCacheTimeout)
	err := u.cluster.ResetPreparedAndResetRegionCache(ctx)
	cancel()

	u.Lock()
	defer u.Unlock()
	u.resettingRegionCache = false
	if u.stage != Resetting {
		return
	}
	u.resetRegionCacheErr = err
	if err != nil {
		log.Warn("failed to reset region cache before finishing unsafe recovery", zap.Error(err))
		return
	}
	u.changeStage(Finished)
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
		} else if u.step > u.recoveryStartStep+1 {
			// Only CollectReport has finished when step == recoveryStartStep+1,
			// which means no operation has done and no cache invalidation is needed.
			u.changeStage(Resetting)
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
		u.storePlanExpires[storeID] = now.Add(u.planExecutionTimeout)
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
			ids := make([]string, 0, len(u.failedStores))
			for store := range u.failedStores {
				ids = append(ids, strconv.FormatUint(store, 10))
			}
			output.Details = append(output.Details, fmt.Sprintf("Failed stores %s", strings.Join(ids, ", ")))
		}
		if u.disableParanoidCheck {
			output.Details = append(output.Details, "paranoid check disabled")
		}
		if u.planExecutionTimeout != defaultPlanExecutionTimeout {
			output.Details = append(output.Details, fmt.Sprintf("plan execution timeout %s", u.planExecutionTimeout))
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
	case Resetting:
		output.Info = "Unsafe recovery is resetting region cache"
	case Finished:
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
	u.orphanedPeers = map[uint64][]*metapb.Peer{}
	u.numStoresReported = 0
	u.step = nextRecoveryStep()
}

func (u *Controller) getForceLeaderPlanDigest() map[string][]string {
	outputs := make(map[string][]string)
	for storeID, plan := range u.storeRecoveryPlans {
		forceLeaders := plan.GetForceLeader()
		if forceLeaders != nil {
			ids := make([]string, 0, len(forceLeaders.GetEnterForceLeaders()))
			for _, regionID := range forceLeaders.GetEnterForceLeaders() {
				ids = append(ids, strconv.FormatUint(regionID, 10))
			}
			outputs[fmt.Sprintf("store %d", storeID)] = []string{fmt.Sprintf("force leader on regions: %s", strings.Join(ids, ", "))}
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
			var peerParts []string
			for _, peer := range demote.GetFailedVoters() {
				peerParts = append(peerParts, fmt.Sprintf("{ %v}", peer)) // the extra space is intentional
			}
			output = append(output, fmt.Sprintf("region %d demotes peers %s", demote.GetRegionId(), strings.Join(peerParts, ", ")))
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
		regionParts := make([]string, 0, len(u.affectedMetaRegions))
		for r := range u.affectedMetaRegions {
			regionParts = append(regionParts, strconv.FormatUint(r, 10))
		}
		details = append(details, "affected meta regions: "+strings.Join(regionParts, ", "))
	}
	if len(u.AffectedTableIDs) != 0 {
		tableParts := make([]string, 0, len(u.AffectedTableIDs))
		for t := range u.AffectedTableIDs {
			tableParts = append(tableParts, strconv.FormatInt(t, 10))
		}
		details = append(details, "affected table ids: "+strings.Join(tableParts, ", "))
	}
	if len(u.newlyCreatedRegions) != 0 {
		newRegionParts := make([]string, 0, len(u.newlyCreatedRegions))
		for r := range u.newlyCreatedRegions {
			newRegionParts = append(newRegionParts, strconv.FormatUint(r, 10))
		}
		details = append(details, "newly created empty regions: "+strings.Join(newRegionParts, ", "))
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

	exists := func(peers []*metapb.Peer, peer *metapb.Peer) bool {
		for _, p := range peers {
			if p.GetId() == peer.GetId() || p.GetStoreId() == peer.GetStoreId() {
				return true
			}
		}
		return false
	}

	var failedPeers []*metapb.Peer
	for _, peer := range region.Peers {
		if u.isFailed(peer) || exists(u.orphanedPeers[region.GetId()], peer) {
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
			if !u.cluster.GetStore(a.storeID).IsTiKV() {
				return -1
			}
			if !u.cluster.GetStore(b.storeID).IsTiKV() {
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

type emptyRegionIndex struct {
	regions []*metapb.Region
}

// findOverlap returns an empty region that overlaps with the half-open range of region.
// An empty end key means positive infinity.
// The empty regions are generated in ascending key order and don't overlap with each other.
func (idx *emptyRegionIndex) findOverlap(region *metapb.Region) *metapb.Region {
	start, end := region.GetStartKey(), region.GetEndKey()
	i := sort.Search(len(idx.regions), func(i int) bool {
		return endGreaterThanKey(idx.regions[i].GetEndKey(), start)
	})
	if i == len(idx.regions) {
		return nil
	}
	emptyRegion := idx.regions[i]
	if startBeforeEnd(emptyRegion.GetStartKey(), end) {
		return emptyRegion
	}
	return nil
}

func endGreaterThanKey(end, key []byte) bool {
	return len(end) == 0 || bytes.Compare(end, key) > 0
}

func startBeforeEnd(start, end []byte) bool {
	return len(end) == 0 || bytes.Compare(start, end) < 0
}

func sameRangeVersion(left, right *metapb.Region) bool {
	return left.GetRegionEpoch().GetVersion() == right.GetRegionEpoch().GetVersion() &&
		bytes.Equal(left.GetStartKey(), right.GetStartKey()) &&
		bytes.Equal(left.GetEndKey(), right.GetEndKey())
}

func findOverlapPeerWithEmptyRegions(peersMap map[uint64][]*regionItem, emptyRegions []*metapb.Region) (*regionItem, *metapb.Region) {
	if len(emptyRegions) == 0 {
		return nil, nil
	}
	index := &emptyRegionIndex{regions: emptyRegions}
	for _, peers := range peersMap {
		var checkedNoOverlapRegion *metapb.Region
		for _, peer := range peers {
			if !peer.isInitialized() {
				continue
			}
			region := peer.region()
			// Peers of the same region with the same version and range share the same overlap result,
			// so no need to check again, just skip.
			if checkedNoOverlapRegion != nil && sameRangeVersion(region, checkedNoOverlapRegion) {
				continue
			}
			if emptyRegion := index.findOverlap(region); emptyRegion != nil {
				return peer, emptyRegion
			}
			checkedNoOverlapRegion = region
		}
	}
	return nil, nil
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

		// find the orphaned peers, i.e. the peers exist in the forced leader but not in the target stores' reports
		// this is expected when some of the peers were destroyed by TombstoneTiFlashLearner phase.
		orphaned := func(peers []*regionItem, peer *metapb.Peer) bool {
			// If the peer is in the failed stores, it is considered failed instead of orphaned.
			if u.isFailed(peer) {
				return false
			}
			for _, p := range peers {
				if p.storeID == peer.StoreId {
					return false
				}
			}
			return true
		}

		for _, peer := range latest.report.RegionState.GetRegion().Peers {
			if orphaned(peers, peer) {
				u.orphanedPeers[latest.region().GetId()] = append(u.orphanedPeers[latest.region().GetId()], peer)
			}
		}
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
			if u.cluster.GetStore(storeID).IsTiFlashWrite() {
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
		regionID, _, err := u.cluster.AllocID(1)
		if err != nil {
			return nil, err
		}
		peerID, _, err := u.cluster.AllocID(1)
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
			if u.cluster.GetStore(storeID).IsTiKV() {
				return storeID
			}
		}
		return 0
	}

	var err error
	// There may be ranges that are covered by no one. Find these empty ranges, create new
	// regions that cover them and evenly distribute newly created regions among all stores.
	type createPlan struct {
		storeID uint64
		region  *metapb.Region
	}

	lastEnd := []byte("")
	var lastStoreID uint64
	var createPlans []createPlan
	var emptyRegions []*metapb.Region
	appendCreatePlan := func(startKey, endKey []byte, storeID uint64) error {
		store := u.cluster.GetStore(storeID)
		if storeID == 0 || store == nil || store.IsTiFlashWrite() {
			storeID = getRandomStoreID()
			if storeID == 0 {
				return errors.New("can't find available store(exclude tiflash) to create new region")
			}
		}
		newRegion, createRegionErr := createRegion(startKey, endKey, storeID)
		if createRegionErr != nil {
			return createRegionErr
		}
		createPlans = append(createPlans, createPlan{storeID: storeID, region: newRegion})
		emptyRegions = append(emptyRegions, newRegion)
		return nil
	}
	newestRegionTree.tree.Ascend(func(item *regionItem) bool {
		region := item.region()
		if !bytes.Equal(region.StartKey, lastEnd) {
			if createRegionErr := appendCreatePlan(lastEnd, region.StartKey, item.storeID); createRegionErr != nil {
				err = createRegionErr
				return false
			}
		}
		lastEnd = region.EndKey
		lastStoreID = item.storeID
		return true
	})
	if err != nil {
		return false, err
	}
	if !bytes.Equal(lastEnd, []byte("")) || newestRegionTree.size() == 0 {
		if err := appendCreatePlan(lastEnd, []byte(""), lastStoreID); err != nil {
			return false, err
		}
	}

	if !u.disableParanoidCheck {
		// paranoid check: newly created empty regions shouldn't overlap with any of the peers.
		if peer, emptyRegion := findOverlapPeerWithEmptyRegions(peersMap, emptyRegions); peer != nil {
			return false, errors.Errorf(
				"Find overlap peer %v with newly created empty region %v",
				logutil.RedactStringer(core.RegionToHexMeta(peer.region())),
				logutil.RedactStringer(core.RegionToHexMeta(emptyRegion)),
			)
		}
	}
	for _, createPlan := range createPlans {
		storeRecoveryPlan := u.getRecoveryPlan(createPlan.storeID)
		storeRecoveryPlan.Creates = append(storeRecoveryPlan.Creates, createPlan.region)
		u.recordAffectedRegion(createPlan.region)
		u.newlyCreatedRegions[createPlan.region.GetId()] = struct{}{}
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
