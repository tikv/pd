// Copyright 2017 TiKV Project Authors.
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

package schedulers

import (
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	// EvictLeaderBatchSize is the number of operators to transfer
	// leaders by one scheduling
	EvictLeaderBatchSize    = 3
	maxEvictLeaderBatchSize = 100
	lastStoreDeleteInfo     = "The last store has been deleted"
)

var invalidEvictLeaderBatchSizeMsg = "batch must be an integer in [1, " + strconv.Itoa(maxEvictLeaderBatchSize) + "]"

func isValidEvictLeaderBatchSize(batchFloat float64) bool {
	return batchFloat >= 1 &&
		batchFloat <= maxEvictLeaderBatchSize &&
		batchFloat == float64(int(batchFloat))
}

type evictLeaderSchedulerConfig struct {
	syncutil.RWMutex
	schedulerConfig

	StoreIDWithRanges map[uint64][]keyutil.KeyRange `json:"store-id-ranges"`
	// Batch is used to generate multiple operators by one scheduling
	Batch             int `json:"batch"`
	cluster           *core.BasicCluster
	removeSchedulerCb func(string) error
}

func (conf *evictLeaderSchedulerConfig) getStores() []uint64 {
	conf.RLock()
	defer conf.RUnlock()
	stores := make([]uint64, 0, len(conf.StoreIDWithRanges))
	for storeID := range conf.StoreIDWithRanges {
		stores = append(stores, storeID)
	}
	return stores
}

func (conf *evictLeaderSchedulerConfig) getBatch() int {
	conf.RLock()
	defer conf.RUnlock()
	return conf.Batch
}

func (conf *evictLeaderSchedulerConfig) clone() *evictLeaderSchedulerConfig {
	conf.RLock()
	defer conf.RUnlock()
	storeIDWithRanges := make(map[uint64][]keyutil.KeyRange)
	for id, ranges := range conf.StoreIDWithRanges {
		storeIDWithRanges[id] = append(storeIDWithRanges[id], ranges...)
	}
	return &evictLeaderSchedulerConfig{
		StoreIDWithRanges: storeIDWithRanges,
		Batch:             conf.Batch,
	}
}

func (conf *evictLeaderSchedulerConfig) getRanges(id uint64) []string {
	conf.RLock()
	defer conf.RUnlock()
	ranges := conf.StoreIDWithRanges[id]
	res := make([]string, 0, len(ranges)*2)
	for index := range ranges {
		res = append(res, (string)(ranges[index].StartKey), (string)(ranges[index].EndKey))
	}
	return res
}

func (conf *evictLeaderSchedulerConfig) removeStoreLocked(id uint64) (bool, error) {
	_, exists := conf.StoreIDWithRanges[id]
	if exists {
		delete(conf.StoreIDWithRanges, id)
		conf.cluster.ResumeLeaderTransfer(id, constant.In)
		return len(conf.StoreIDWithRanges) == 0, nil
	}
	return false, errs.ErrScheduleConfigNotExist.FastGenByArgs()
}

func (conf *evictLeaderSchedulerConfig) resetStoreLocked(id uint64, keyRange []keyutil.KeyRange) {
	if err := conf.cluster.PauseLeaderTransfer(id, constant.In); err != nil {
		log.Error("pause leader transfer failed", zap.Uint64("store-id", id), errs.ZapError(err))
	}
	conf.StoreIDWithRanges[id] = keyRange
}

func (conf *evictLeaderSchedulerConfig) resetStore(id uint64, keyRange []keyutil.KeyRange) {
	conf.Lock()
	defer conf.Unlock()
	conf.resetStoreLocked(id, keyRange)
}

func (conf *evictLeaderSchedulerConfig) getKeyRangesByID(id uint64) []keyutil.KeyRange {
	conf.RLock()
	defer conf.RUnlock()
	if ranges, exist := conf.StoreIDWithRanges[id]; exist {
		return ranges
	}
	return nil
}

func (conf *evictLeaderSchedulerConfig) encodeConfig() ([]byte, error) {
	conf.RLock()
	defer conf.RUnlock()
	return EncodeConfig(conf)
}

func (conf *evictLeaderSchedulerConfig) reloadConfig() error {
	conf.Lock()
	defer conf.Unlock()
	newCfg := &evictLeaderSchedulerConfig{}
	if err := conf.load(newCfg); err != nil {
		return err
	}
	if newCfg.Batch == 0 {
		newCfg.Batch = EvictLeaderBatchSize
	}
	pauseAndResumeLeaderTransfer(conf.cluster, constant.In, conf.StoreIDWithRanges, newCfg.StoreIDWithRanges)
	conf.StoreIDWithRanges = newCfg.StoreIDWithRanges
	conf.Batch = newCfg.Batch
	return nil
}

func (conf *evictLeaderSchedulerConfig) pauseLeaderTransfer(cluster sche.SchedulerCluster) error {
	conf.RLock()
	defer conf.RUnlock()
	var res error
	for id := range conf.StoreIDWithRanges {
		if err := cluster.PauseLeaderTransfer(id, constant.In); err != nil {
			res = err
		}
	}
	return res
}

func (conf *evictLeaderSchedulerConfig) resumeLeaderTransfer(cluster sche.SchedulerCluster) {
	conf.RLock()
	defer conf.RUnlock()
	for id := range conf.StoreIDWithRanges {
		cluster.ResumeLeaderTransfer(id, constant.In)
	}
}

// applyStoreIDs pauses leader transfer for any newly added store, resolves
// each store's target ranges, and persists everything with a single save —
// all under one lock, so the whole call is atomic with respect to concurrent
// config updates on this scheduler. ids may be a single store (a store_id
// request), several (a store_ids batch), or empty (a batch-size-only
// update). When hasExplicitRanges is false, a store that already existed
// keeps its current ranges untouched; a brand-new store defaults to the
// whole key space. On failure, previously existing stores are restored to
// their prior ranges and newly added stores (and their leader-transfer
// pause) are rolled back entirely; Batch is restored too.
func (conf *evictLeaderSchedulerConfig) applyStoreIDs(ids []uint64, explicitRanges []keyutil.KeyRange, hasExplicitRanges bool, batch int) error {
	conf.Lock()
	defer conf.Unlock()

	prevBatch := conf.Batch
	prevRanges := make(map[uint64][]keyutil.KeyRange)
	var pausedIDs []uint64

	// rollback undoes everything applied to StoreIDWithRanges so far in this
	// call: newly added stores are removed (and un-paused), and previously
	// existing stores get their old ranges back. It's shared by both failure
	// points below so a mid-batch pause failure rolls back exactly as
	// completely as a later save failure does.
	rollback := func() {
		for _, id := range pausedIDs {
			delete(conf.StoreIDWithRanges, id)
			conf.cluster.ResumeLeaderTransfer(id, constant.In)
		}
		maps.Copy(conf.StoreIDWithRanges, prevRanges)
	}

	for _, id := range ids {
		old, existed := conf.StoreIDWithRanges[id]
		if !existed {
			if err := conf.cluster.PauseLeaderTransfer(id, constant.In); err != nil {
				rollback()
				return err
			}
			pausedIDs = append(pausedIDs, id)
			if hasExplicitRanges {
				conf.StoreIDWithRanges[id] = slices.Clone(explicitRanges)
			} else {
				conf.StoreIDWithRanges[id] = []keyutil.KeyRange{keyutil.NewKeyRange("", "")}
			}
			continue
		}
		if !hasExplicitRanges {
			// Keep the existing store's current ranges untouched.
			continue
		}
		prevRanges[id] = old
		conf.StoreIDWithRanges[id] = slices.Clone(explicitRanges)
	}
	conf.Batch = batch

	if err := conf.save(); err != nil {
		conf.Batch = prevBatch
		rollback()
		return err
	}
	return nil
}

func (conf *evictLeaderSchedulerConfig) delete(id uint64) (any, error) {
	conf.Lock()
	var resp any
	keyRanges := conf.StoreIDWithRanges[id]
	last, err := conf.removeStoreLocked(id)
	if err != nil {
		conf.Unlock()
		return resp, err
	}

	err = conf.save()
	if err != nil {
		conf.resetStoreLocked(id, keyRanges)
		conf.Unlock()
		return resp, err
	}
	if !last {
		conf.Unlock()
		return resp, nil
	}
	conf.Unlock()
	if err := conf.removeSchedulerCb(types.EvictLeaderScheduler.String()); err != nil {
		if !errors.ErrorEqual(err, errs.ErrSchedulerNotFound.FastGenByArgs()) {
			conf.resetStore(id, keyRanges)
		}
		return resp, err
	}
	resp = lastStoreDeleteInfo
	return resp, nil
}

type evictLeaderScheduler struct {
	*BaseScheduler
	conf    *evictLeaderSchedulerConfig
	handler http.Handler
}

// newEvictLeaderScheduler creates an admin scheduler that transfers all leaders
// out of a store.
func newEvictLeaderScheduler(opController *operator.Controller, conf *evictLeaderSchedulerConfig) Scheduler {
	handler := newEvictLeaderHandler(conf)
	return &evictLeaderScheduler{
		BaseScheduler: NewBaseScheduler(opController, types.EvictLeaderScheduler, conf),
		conf:          conf,
		handler:       handler,
	}
}

// EvictStoreIDs returns the IDs of the evict-stores.
func (s *evictLeaderScheduler) EvictStoreIDs() []uint64 {
	return s.conf.getStores()
}

// ServeHTTP implements the http.Handler interface.
func (s *evictLeaderScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// EncodeConfig encodes the config to a byte array.
func (s *evictLeaderScheduler) EncodeConfig() ([]byte, error) {
	return s.conf.encodeConfig()
}

// ReloadConfig reloads the config from the storage.
func (s *evictLeaderScheduler) ReloadConfig() error {
	return s.conf.reloadConfig()
}

// PrepareConfig implements the Scheduler interface.
func (s *evictLeaderScheduler) PrepareConfig(cluster sche.SchedulerCluster) error {
	return s.conf.pauseLeaderTransfer(cluster)
}

// CleanConfig implements the Scheduler interface.
func (s *evictLeaderScheduler) CleanConfig(cluster sche.SchedulerCluster) {
	s.conf.resumeLeaderTransfer(cluster)
}

// IsScheduleAllowed implements the Scheduler interface.
func (s *evictLeaderScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetSchedulerConfig().GetLeaderScheduleLimit()
	if !allowed {
		operator.IncOperatorLimitCounter(s.GetType(), operator.OpLeader)
	}
	return allowed
}

// Schedule implements the Scheduler interface.
func (s *evictLeaderScheduler) Schedule(cluster sche.SchedulerCluster, _ bool) ([]*operator.Operator, []plan.Plan) {
	evictLeaderCounter.Inc()
	return scheduleEvictLeaderBatch(s.GetName(), cluster, s.conf, s.OpController), nil
}

func uniqueAppendOperator(dst []*operator.Operator, src ...*operator.Operator) []*operator.Operator {
	regionIDs := make(map[uint64]struct{})
	for i := range dst {
		regionIDs[dst[i].RegionID()] = struct{}{}
	}
	for i := range src {
		if _, ok := regionIDs[src[i].RegionID()]; ok {
			continue
		}
		regionIDs[src[i].RegionID()] = struct{}{}
		dst = append(dst, src[i])
	}
	return dst
}

type evictLeaderStoresConf interface {
	getStores() []uint64
	getKeyRangesByID(id uint64) []keyutil.KeyRange
	getBatch() int
}

func scheduleEvictLeaderBatch(name string, cluster sche.SchedulerCluster, conf evictLeaderStoresConf, explicit *operator.Controller) []*operator.Operator {
	var ops []*operator.Operator
	batchSize := conf.getBatch()
	for range batchSize {
		once := scheduleEvictLeaderOnce(name, cluster, conf, explicit)
		// no more regions
		if len(once) == 0 {
			break
		}
		ops = uniqueAppendOperator(ops, once...)
		// the batch has been fulfilled
		if len(ops) > batchSize {
			break
		}
	}
	return ops
}

func scheduleEvictLeaderOnce(name string, cluster sche.SchedulerCluster, conf evictLeaderStoresConf, explicit *operator.Controller) []*operator.Operator {
	stores := conf.getStores()
	ops := make([]*operator.Operator, 0, len(stores))
	for _, storeID := range stores {
		ranges := conf.getKeyRangesByID(storeID)
		if len(ranges) == 0 {
			continue
		}
		var filters []filter.Filter
		pendingFilter := filter.NewRegionPendingFilter()
		downFilter := filter.NewRegionDownFilter()
		sample := func() []*core.RegionInfo {
			regions := cluster.RandLeaderRegions(storeID, ranges)
			if explicit != nil {
				// An active eviction owns its retries. Other operators can still
				// be preempted by an urgent drain.
				regions = slices.DeleteFunc(regions, func(r *core.RegionInfo) bool {
					active := explicit.GetOperator(r.GetID())
					return active != nil && active.Desc() == name
				})
			}
			return regions
		}
		region := filter.SelectOneRegion(sample(), nil, pendingFilter, downFilter)
		if region == nil {
			// try to pick unhealthy region
			region = filter.SelectOneRegion(sample(), nil)
			if region == nil {
				evictLeaderNoLeaderCounter.Inc()
				continue
			}
			evictLeaderPickUnhealthyCounter.Inc()
			unhealthyPeerStores := make(map[uint64]struct{})
			for _, peer := range region.GetDownPeers() {
				unhealthyPeerStores[peer.GetPeer().GetStoreId()] = struct{}{}
			}
			for _, peer := range region.GetPendingPeers() {
				unhealthyPeerStores[peer.GetStoreId()] = struct{}{}
			}
			filters = append(filters, filter.NewExcludedFilter(name, nil, unhealthyPeerStores))
		}

		state := &filter.StoreStateFilter{ActionScope: name, TransferLeader: true}
		if explicit == nil {
			state.OperatorLevel = constant.Urgent
		}
		filters = append(filters, state)
		candidates := filter.NewCandidates(cluster.GetFollowerStores(region)).
			FilterTarget(cluster.GetSchedulerConfig(), nil, nil, filters...)
		targetIDs := make([]uint64, 0, len(candidates.Stores))
		for _, store := range candidates.Stores {
			targetIDs = append(targetIDs, store.GetID())
		}
		allowed := func(r *core.RegionInfo, ids []uint64) []uint64 {
			return eligibleEvictTargets(cluster, r, ids)
		}
		if explicit != nil {
			targetIDs = allowed(region, targetIDs)
		}
		if len(targetIDs) == 0 {
			evictLeaderNoTargetStoreCounter.Inc()
			continue
		}
		target := targetIDs[rand.IntN(len(targetIDs))]
		prefer := false
		if explicit != nil && len(targetIDs) > 1 {
			target, prefer = pickEvictLeaderTarget(cluster, explicit, name, region, targetIDs, target, targetIDs[rand.IntN(len(targetIDs))])
		}
		op, err := operator.CreateTransferLeaderOperator(name, cluster, region, target, targetIDs, operator.OpLeader)
		if err != nil {
			log.Debug("fail to create evict leader operator", errs.ZapError(err))
			continue
		}
		if explicit != nil {
			op.SetEvictLeaderTargets(allowed, prefer)
		}
		op.SetPriorityLevel(constant.Urgent)
		op.Counters = append(op.Counters, evictLeaderNewOperatorCounter)
		ops = append(ops, op)
	}
	return ops
}

// A target may be a voter while its placement rule still requires it to remain
// a follower. Check the resulting role assignment before advertising any peer.
func eligibleEvictTargets(cluster sche.SchedulerCluster, region *core.RegionInfo, ids []uint64) []uint64 {
	conf := cluster.GetSharedConfig()
	var fit *placement.RegionFit
	var rules []*placement.Rule
	if conf.IsPlacementRulesEnabled() {
		rm := cluster.GetRuleManager()
		rules = rm.GetRulesForApplyRegion(region)
		// Voter-only rules are invariant under a leadership change.
		if slices.ContainsFunc(rules, func(r *placement.Rule) bool { return r.Role == placement.Leader || r.Role == placement.Follower }) {
			fit = rm.FitRegion(cluster.GetBasicCluster(), region)
			rules = fit.GetRules()
		}
		if len(rules) == 0 {
			return nil
		}
	}
	// Admission already consumed the transfer budget for this operator.
	// Check persistent store exclusions without charging that budget again.
	state := &filter.StoreStateFilter{TransferLeader: true, AllowTemporaryStates: true}
	eligible := make([]uint64, 0, len(ids))
	for _, id := range ids {
		p := region.GetStorePeer(id)
		if p == nil || p.GetIsWitness() || !core.IsVoterOrIncomingVoter(p) ||
			region.GetDownPeer(p.GetId()) != nil || region.GetPendingPeer(p.GetId()) != nil {
			continue
		}
		store := cluster.GetStore(id)
		if store == nil || store.IsDisconnected() || !state.Target(conf, store).IsOK() {
			continue
		}
		if len(rules) > 0 && !placement.MatchesLeaderRules(rules, store) {
			continue
		}
		if fit == nil || fit.AllowsLeader(region, p, conf.IsWitnessAllowed()) {
			eligible = append(eligible, id)
		}
	}
	return eligible
}

func pickEvictLeaderTarget(cluster sche.SchedulerCluster, oc *operator.Controller, name string, region *core.RegionInfo, ids []uint64, ordinary, other uint64) (uint64, bool) {
	// A late ACK belongs to the current attempt. Only a finished timeout
	// advances its primary, using the controller's existing operator history.
	last := oc.GetOperatorStatus(region.GetID())
	if last != nil && last.Operator.Status() == operator.TIMEOUT && last.Desc() == name &&
		last.RegionEpoch().GetConfVer() == region.GetRegionEpoch().GetConfVer() &&
		last.RegionEpoch().GetVersion() == region.GetRegionEpoch().GetVersion() {
		if step, ok := last.Step(0).(operator.TransferLeader); ok && step.FromStore == region.GetLeader().GetStoreId() {
			slices.Sort(ids)
			i, found := slices.BinarySearch(ids, step.ToStore)
			if found {
				i++
			}
			return ids[i%len(ids)], true
		}
	}
	if other == ordinary {
		return ordinary, false
	}
	a, b := storeReadRate(cluster.GetStore(ordinary)), storeReadRate(cluster.GetStore(other))
	if a <= b || b < 0 {
		return ordinary, false
	}
	degree := utils.Read.HotDegreeThreshold(cluster.GetSchedulerConfig().GetHotRegionCacheHitsThreshold())
	rate := cluster.TryHotReadLeaderRate(region.GetID(), region.GetLeader().GetStoreId(), degree)
	// Compare two independent draws. Even if old telemetry keeps favouring one
	// store, it can only win if sampled. A gap smaller than the Region's own
	// rate is too small to justify redirecting the ordinary choice.
	if rate > 0 && a-b >= rate {
		return other, true
	}
	return ordinary, false
}

func storeReadRate(store *core.StoreInfo) float64 {
	if store == nil || store.IsDisconnected() {
		return -1
	}
	stats := store.GetStoreStats()
	interval := stats.GetInterval()
	if interval.GetEndTimestamp() <= interval.GetStartTimestamp() {
		return -1
	}
	// A fresh heartbeat can still carry a replayed measurement.
	measuredAt := time.Unix(int64(interval.EndTimestamp), 0)
	if store.GetLastHeartbeatTS().Sub(measuredAt).Abs() > utils.StoreHeartBeatReportInterval*time.Second {
		return -1
	}
	return float64(stats.GetBytesRead()) / float64(interval.EndTimestamp-interval.StartTimestamp)
}

type evictLeaderHandler struct {
	rd     *render.Render
	config *evictLeaderSchedulerConfig
}

// updateConfig handles both a single "store_id" (backward compatible) and a
// "store_ids" array (batch) the same way: it resolves the target store list
// first, then calls applyStoreIDs once. No leader-transfer pausing happens
// until that single call, so an early validation failure (bad batch, bad
// ranges) never needs to roll anything back — nothing has been touched yet.
func (handler *evictLeaderHandler) updateConfig(w http.ResponseWriter, r *http.Request) {
	var input map[string]any
	if err := apiutil.ReadJSONRespondError(handler.rd, w, r.Body, &input); err != nil {
		return
	}

	ids, err := parseUpdateStoreIDs(input)
	if err != nil {
		handler.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	batch := handler.config.getBatch()
	batchFloat, inputBatch := input["batch"].(float64)
	if input["batch"] != nil && !inputBatch {
		handler.rd.JSON(w, http.StatusBadRequest, fmt.Sprintf("invalid argument for 'batch': expected a number, got %T", input["batch"]))
		return
	}
	if inputBatch {
		if !isValidEvictLeaderBatchSize(batchFloat) {
			handler.rd.JSON(w, http.StatusBadRequest, invalidEvictLeaderBatchSizeMsg)
			return
		}
		batch = (int)(batchFloat)
	}

	rawRanges, hasRanges := input["ranges"]
	var explicitRanges []keyutil.KeyRange
	if hasRanges {
		if len(ids) == 0 {
			handler.rd.JSON(w, http.StatusBadRequest, errs.ErrSchedulerConfig.FastGenByArgs("id"))
			return
		}
		strs, ok := decodeStringSlice(rawRanges)
		if !ok {
			handler.rd.JSON(w, http.StatusBadRequest, fmt.Sprintf("invalid argument for 'ranges': expected an array of strings, got %T", rawRanges))
			return
		}
		// An empty "ranges" array carries no range pairs to apply; treat it
		// the same as an omitted field instead of letting getKeyRanges
		// default it to the whole key space and overwrite existing stores'
		// custom ranges.
		if len(strs) > 0 {
			explicitRanges, err = getKeyRanges(strs)
			if err != nil {
				handler.rd.JSON(w, http.StatusBadRequest, err.Error())
				return
			}
		} else {
			hasRanges = false
		}
	}

	if err := handler.config.applyStoreIDs(ids, explicitRanges, hasRanges, batch); err != nil {
		handler.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	handler.rd.JSON(w, http.StatusOK, "The scheduler has been applied to the store.")
}

// parseUpdateStoreIDs reads the target store IDs for a config update from the
// request body. It accepts a single "store_id" (backward compatible with
// existing clients), a "store_ids" array to update several stores in one
// call, or neither (a batch-size-only update).
func parseUpdateStoreIDs(input map[string]any) ([]uint64, error) {
	_, hasStoreID := input["store_id"]
	rawStoreIDs, hasStoreIDs := input["store_ids"]
	switch {
	case hasStoreID && hasStoreIDs:
		return nil, errors.New("only one of store_id and store_ids can be set")
	case hasStoreIDs:
		arr, ok := rawStoreIDs.([]any)
		if !ok || len(arr) == 0 {
			return nil, errors.New("please input a right store id")
		}
		ids := make([]uint64, 0, len(arr))
		seen := make(map[uint64]struct{}, len(arr))
		for _, v := range arr {
			f, ok := v.(float64)
			if !ok {
				return nil, errors.New("please input a right store id")
			}
			id, ok := storeIDFromFloat(f)
			if !ok {
				return nil, errors.New("please input a right store id")
			}
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
		return ids, nil
	case hasStoreID:
		f, ok := input["store_id"].(float64)
		if !ok {
			return nil, errors.New("please input a right store id")
		}
		id, ok := storeIDFromFloat(f)
		if !ok {
			return nil, errors.New("please input a right store id")
		}
		return []uint64{id}, nil
	default:
		return nil, nil
	}
}

// storeIDFromFloat converts a JSON-decoded number into a store ID, rejecting
// fractional, negative, or out-of-range values.
func storeIDFromFloat(f float64) (uint64, bool) {
	if f < 0 || f != math.Trunc(f) || f > float64(math.MaxUint64) {
		return 0, false
	}
	return uint64(f), true
}

// multiStoreIDsArgSeparator joins several store IDs into the single
// persisted creation arg produced by EvictLeaderMultiStoreArgs, so creating
// evict-leader-scheduler with several stores writes one args entry instead
// of creating with one store and adding the rest via a follow-up
// scheduler-config update (which, in microservice mode, is watched
// independently of scheduler creation and can race with it).
const multiStoreIDsArgSeparator = ","

// parseMultiStoreIDsArg parses a comma-joined list of store IDs, as produced
// by EvictLeaderMultiStoreArgs. Callers only reach this after arg has
// already failed to parse as a single store ID, so a comma-free arg (a
// single element after Split) fails here too: it's the same string that
// already failed once.
func parseMultiStoreIDsArg(arg string) ([]uint64, bool) {
	parts := strings.Split(arg, multiStoreIDsArgSeparator)
	ids := make([]uint64, 0, len(parts))
	for _, part := range parts {
		id, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return nil, false
		}
		ids = append(ids, id)
	}
	return ids, true
}

// EvictLeaderMultiStoreArgs encodes several store IDs into the single
// creation arg evict-leader-scheduler expects for a one-call, multi-store
// creation request (see parseMultiStoreIDsArg). Callers must pass at least
// two IDs: a single ID should instead be passed as a plain store-id arg.
func EvictLeaderMultiStoreArgs(ids []uint64) string {
	strs := make([]string, 0, len(ids))
	for _, id := range ids {
		strs = append(strs, strconv.FormatUint(id, 10))
	}
	return strings.Join(strs, multiStoreIDsArgSeparator)
}

// decodeStringSlice converts a JSON-decoded value into []string. A JSON
// array always decodes as []any (never []string), so this checks the
// element types explicitly instead of relying on a direct type assertion.
func decodeStringSlice(v any) ([]string, bool) {
	arr, ok := v.([]any)
	if !ok {
		return nil, false
	}
	strs := make([]string, 0, len(arr))
	for _, item := range arr {
		s, ok := item.(string)
		if !ok {
			return nil, false
		}
		strs = append(strs, s)
	}
	return strs, true
}

func (handler *evictLeaderHandler) listConfig(w http.ResponseWriter, _ *http.Request) {
	conf := handler.config.clone()
	handler.rd.JSON(w, http.StatusOK, conf)
}

func (handler *evictLeaderHandler) deleteConfig(w http.ResponseWriter, r *http.Request) {
	idStr := mux.Vars(r)["store_id"]
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		handler.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	resp, err := handler.config.delete(id)
	if err != nil {
		if errors.ErrorEqual(err, errs.ErrSchedulerNotFound.FastGenByArgs()) || errors.ErrorEqual(err, errs.ErrScheduleConfigNotExist.FastGenByArgs()) {
			handler.rd.JSON(w, http.StatusNotFound, err.Error())
		} else {
			handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	handler.rd.JSON(w, http.StatusOK, resp)
}

func newEvictLeaderHandler(config *evictLeaderSchedulerConfig) http.Handler {
	h := &evictLeaderHandler{
		config: config,
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/config", h.updateConfig).Methods(http.MethodPost)
	router.HandleFunc("/list", h.listConfig).Methods(http.MethodGet)
	router.HandleFunc("/delete/{store_id}", h.deleteConfig).Methods(http.MethodDelete)
	return router
}
