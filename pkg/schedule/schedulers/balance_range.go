// Copyright 2025 TiKV Project Authors.
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
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

var (
	defaultJobTimeout = 30 * time.Minute
	reserveDuration   = 7 * 24 * time.Hour
	reportInterval    = 10 * time.Second
)

type balanceRangeSchedulerHandler struct {
	rd     *render.Render
	config *balanceRangeSchedulerConfig
}

func newBalanceRangeHandler(conf *balanceRangeSchedulerConfig) http.Handler {
	handler := &balanceRangeSchedulerHandler{
		config: conf,
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/config", handler.updateConfig).Methods(http.MethodPost)
	router.HandleFunc("/list", handler.listConfig).Methods(http.MethodGet)
	router.HandleFunc("/job", handler.addJob).Methods(http.MethodPut)
	router.HandleFunc("/job", handler.deleteJob).Methods(http.MethodDelete)
	return router
}

func (handler *balanceRangeSchedulerHandler) updateConfig(w http.ResponseWriter, _ *http.Request) {
	handler.rd.JSON(w, http.StatusBadRequest, "update config is not supported")
}

func (handler *balanceRangeSchedulerHandler) listConfig(w http.ResponseWriter, _ *http.Request) {
	handler.config.Lock()
	defer handler.config.Unlock()
	conf := handler.config.cloneLocked()
	if err := handler.rd.JSON(w, http.StatusOK, conf); err != nil {
		log.Error("failed to marshal balance key range scheduler config", errs.ZapError(err))
	}
}

func (handler *balanceRangeSchedulerHandler) addJob(w http.ResponseWriter, r *http.Request) {
	var input map[string]any
	if err := apiutil.ReadJSONRespondError(handler.rd, w, r.Body, &input); err != nil {
		return
	}
	job := &balanceRangeSchedulerJob{
		Create:  time.Now(),
		Status:  pending,
		Timeout: defaultJobTimeout,
	}
	job.Engine = input["engine"].(string)
	if job.Engine != core.EngineTiFlash && job.Engine != core.EngineTiKV {
		handler.rd.JSON(w, http.StatusBadRequest, fmt.Sprintf("engine:%s must be tikv or tiflash", input["engine"].(string)))
		return
	}
	job.Rule = core.NewRule(input["rule"].(string))
	if job.Rule != core.LeaderScatter && job.Rule != core.PeerScatter && job.Rule != core.LearnerScatter {
		handler.rd.JSON(w, http.StatusBadRequest, fmt.Sprintf("rule:%s must be leader-scatter, learner-scatter or peer-scatter",
			input["engine"].(string)))
		return
	}

	job.Alias = input["alias"].(string)
	timeoutStr, ok := input["timeout"].(string)
	if ok && len(timeoutStr) > 0 {
		timeout, err := time.ParseDuration(timeoutStr)
		if err != nil {
			handler.rd.JSON(w, http.StatusBadRequest, fmt.Sprintf("timeout:%s is invalid", input["timeout"].(string)))
			return
		}
		job.Timeout = timeout
	}

	keys, err := keyutil.DecodeHTTPKeyRanges(input)
	if err != nil {
		handler.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	krs, err := getKeyRanges(keys)
	if err != nil {
		handler.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	log.Info("add balance key range job", zap.String("alias", job.Alias))
	job.Ranges = krs
	if err := handler.config.addJob(job); err != nil {
		handler.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	handler.rd.JSON(w, http.StatusOK, nil)
}

func (handler *balanceRangeSchedulerHandler) deleteJob(w http.ResponseWriter, r *http.Request) {
	jobStr := r.URL.Query().Get("job-id")
	if jobStr == "" {
		handler.rd.JSON(w, http.StatusBadRequest, "job-id is required")
		return
	}
	jobID, err := strconv.ParseUint(jobStr, 10, 64)
	if err != nil {
		handler.rd.JSON(w, http.StatusBadRequest, "invalid job-id")
		return
	}
	if err := handler.config.deleteJob(jobID); err != nil {
		handler.rd.JSON(w, http.StatusInternalServerError, err)
		return
	}
	handler.rd.JSON(w, http.StatusOK, nil)
}

type balanceRangeSchedulerConfig struct {
	schedulerConfig
	syncutil.Mutex
	jobs []*balanceRangeSchedulerJob
}

func (conf *balanceRangeSchedulerConfig) addJob(job *balanceRangeSchedulerJob) error {
	conf.Lock()
	defer conf.Unlock()
	for _, c := range conf.jobs {
		if c.isComplete() {
			continue
		}
		if job.Alias == c.Alias {
			return errors.New("job already exists")
		}
	}
	job.Status = pending
	if len(conf.jobs) == 0 {
		job.JobID = 1
	} else {
		job.JobID = conf.jobs[len(conf.jobs)-1].JobID + 1
	}
	return conf.persistLocked(func() {
		conf.jobs = append(conf.jobs, job)
	})
}

func (conf *balanceRangeSchedulerConfig) deleteJob(jobID uint64) error {
	conf.Lock()
	defer conf.Unlock()
	for _, job := range conf.jobs {
		if job.JobID == jobID {
			if job.isComplete() {
				return errs.ErrInvalidArgument.FastGenByArgs(fmt.Sprintf(
					"The job:%d has been completed and cannot be cancelled.", jobID))
			}
			return conf.persistLocked(func() {
				job.Status = cancelled
				now := time.Now()
				if job.Start == nil {
					job.Start = &now
				}
				job.Finish = &now
			})
		}
	}
	return errs.ErrScheduleConfigNotExist.FastGenByArgs(jobID)
}

func (conf *balanceRangeSchedulerConfig) gcLocked() error {
	needGC := false
	gcIdx := 0
	for idx, job := range conf.jobs {
		if job.isComplete() && job.expired(reserveDuration) {
			needGC = true
			gcIdx = idx
		} else {
			// The jobs are sorted by the started time and executed by it.
			// So it can end util the first element doesn't satisfy the condition.
			break
		}
	}
	if !needGC {
		return nil
	}
	return conf.persistLocked(func() {
		conf.jobs = conf.jobs[gcIdx+1:]
	})
}

func (conf *balanceRangeSchedulerConfig) persistLocked(updateFn func()) error {
	originJobs := conf.cloneLocked()
	updateFn()
	if err := conf.save(); err != nil {
		conf.jobs = originJobs
		return err
	}
	return nil
}

// MarshalJSON marshals to json.
func (conf *balanceRangeSchedulerConfig) MarshalJSON() ([]byte, error) {
	return json.Marshal(conf.jobs)
}

// UnmarshalJSON unmarshals from json.
func (conf *balanceRangeSchedulerConfig) UnmarshalJSON(data []byte) error {
	jobs := make([]*balanceRangeSchedulerJob, 0)
	if err := json.Unmarshal(data, &jobs); err != nil {
		return err
	}
	conf.jobs = jobs
	return nil
}

func (conf *balanceRangeSchedulerConfig) beginLocked(index int) error {
	job := conf.jobs[index]
	if job.Status != pending {
		return errors.New("the job is not pending")
	}
	return conf.persistLocked(func() {
		now := time.Now()
		job.Start = &now
		job.Status = running
	})
}

func (conf *balanceRangeSchedulerConfig) finishLocked(index int) error {
	job := conf.jobs[index]
	if job.Status != running {
		return errors.New("the job is not running")
	}
	return conf.persistLocked(func() {
		now := time.Now()
		job.Finish = &now
		job.Status = finished
	})
}

func (conf *balanceRangeSchedulerConfig) peekLocked() (int, *balanceRangeSchedulerJob) {
	for index, job := range conf.jobs {
		if job.isComplete() {
			continue
		}
		return index, job
	}
	return 0, nil
}

func (conf *balanceRangeSchedulerConfig) cloneLocked() []*balanceRangeSchedulerJob {
	jobs := make([]*balanceRangeSchedulerJob, 0, len(conf.jobs))
	for _, job := range conf.jobs {
		ranges := make([]keyutil.KeyRange, len(job.Ranges))
		copy(ranges, job.Ranges)
		jobs = append(jobs, &balanceRangeSchedulerJob{
			Ranges:  ranges,
			Rule:    job.Rule,
			Engine:  job.Engine,
			Timeout: job.Timeout,
			Alias:   job.Alias,
			JobID:   job.JobID,
			Start:   job.Start,
			Status:  job.Status,
			Create:  job.Create,
			Finish:  job.Finish,
		})
	}

	return jobs
}

type balanceRangeSchedulerJob struct {
	JobID   uint64             `json:"job-id"`
	Rule    core.Rule          `json:"rule"`
	Engine  string             `json:"engine"`
	Timeout time.Duration      `json:"timeout"`
	Ranges  []keyutil.KeyRange `json:"ranges"`
	Alias   string             `json:"alias"`
	Start   *time.Time         `json:"start,omitempty"`
	Finish  *time.Time         `json:"finish,omitempty"`
	Create  time.Time          `json:"create"`
	Status  JobStatus          `json:"status"`
}

func (job *balanceRangeSchedulerJob) expired(dur time.Duration) bool {
	if job == nil {
		return true
	}
	if job.Finish == nil {
		return false
	}
	now := time.Now()
	return now.Sub(*job.Finish) > dur
}

func (job *balanceRangeSchedulerJob) shouldFinished() bool {
	return time.Since(*job.Start) > job.Timeout
}

func (job *balanceRangeSchedulerJob) isComplete() bool {
	return job.Status == finished || job.Status == cancelled
}

// EncodeConfig serializes the config.
func (s *balanceRangeScheduler) EncodeConfig() ([]byte, error) {
	s.conf.Lock()
	defer s.conf.Unlock()
	return EncodeConfig(s.conf.jobs)
}

// ReloadConfig reloads the config.
func (s *balanceRangeScheduler) ReloadConfig() error {
	s.conf.Lock()
	defer s.conf.Unlock()
	jobs := make([]*balanceRangeSchedulerJob, 0, len(s.conf.jobs))
	if err := s.conf.load(&jobs); err != nil {
		return err
	}
	s.conf.jobs = jobs
	return nil
}

type balanceRangeScheduler struct {
	*BaseScheduler
	conf          *balanceRangeSchedulerConfig
	handler       http.Handler
	filters       []filter.Filter
	filterCounter *filter.Counter
	// stores is sorted by score desc
	stores []*core.StoreInfo
	// scoreMap records the storeID -> score
	scoreMap map[uint64]float64
	// expectScoreMap records the storeID -> expect score
	expectScoreMap map[uint64]float64
	job            *balanceRangeSchedulerJob
	solver         *solver
	lastReportTime time.Time
}

func (s *balanceRangeScheduler) report() {
	now := time.Now()
	if now.Sub(s.lastReportTime) < reportInterval {
		return
	}
	for storeID, score := range s.scoreMap {
		storeStr := strconv.FormatUint(storeID, 10)
		balanceRangeGauge.WithLabelValues(storeStr, "score").Set(score)
		if expect, ok := s.expectScoreMap[storeID]; ok {
			balanceRangeGauge.WithLabelValues(storeStr, "expect").Set(expect)
		} else {
			balanceRangeGauge.WithLabelValues(storeStr, "expect").Set(0)
		}
	}
	balanceRangeJobGauge.WithLabelValues().Set(float64(s.job.JobID))
	s.lastReportTime = now
}

// ServeHTTP implements the http.Handler interface.
func (s *balanceRangeScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

func (s *balanceRangeScheduler) isBalanced() bool {
	diff := 0
	for storeID, score := range s.scoreMap {
		if expect, ok := s.expectScoreMap[storeID]; ok {
			if expect > score {
				diff += int(expect - score)
			} else {
				diff += int(score - expect)
			}
		}
	}
	return diff <= 1
}

// IsScheduleAllowed checks if the scheduler is allowed to schedule new operators.
func (s *balanceRangeScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpRange) < cluster.GetSchedulerConfig().GetRegionScheduleLimit()
	if !allowed {
		operator.IncOperatorLimitCounter(s.GetType(), operator.OpRange)
		return false
	}
	return s.checkJob(cluster)
}

func (s *balanceRangeScheduler) checkJob(cluster sche.SchedulerCluster) bool {
	s.conf.Lock()
	defer s.conf.Unlock()
	if err := s.conf.gcLocked(); err != nil {
		log.Error("balance range jobs gc failed", errs.ZapError(err))
		return false
	}
	// If the running job has been cancelled, it needs to stop the job.
	if s.job != nil && s.job.Status == cancelled {
		s.cleanJobStatus(cluster, s.job)
	}

	index, job := s.conf.peekLocked()
	// all jobs are completed
	if job == nil {
		balanceRangeNoJobCounter.Inc()
		return false
	}

	if job.Status == pending {
		if err := s.conf.beginLocked(index); err != nil {
			return false
		}
		km := cluster.GetKeyRangeManager()
		km.Append(job.Ranges)
	}
	if job.shouldFinished() {
		if err := s.conf.finishLocked(index); err != nil {
			balancePersistFailedCounter.Inc()
			return false
		}
		s.cleanJobStatus(cluster, job)
		balanceRangeExpiredCounter.Inc()
		log.Info("balance key range job finished due to timeout", zap.String("alias", job.Alias), zap.Uint64("job-id", job.JobID))
	}

	opInfluence := s.OpController.GetOpInfluence(cluster.GetBasicCluster(), operator.WithRangeOption(job.Ranges))
	err := s.prepare(cluster, opInfluence, job)
	if err != nil {
		log.Warn("failed to prepare balance key range scheduler", errs.ZapError(err))
		return false
	}
	if s.isBalanced() {
		if err = s.conf.finishLocked(index); err != nil {
			balancePersistFailedCounter.Inc()
			return false
		}
		s.cleanJobStatus(cluster, job)
		balanceRangeBalancedCounter.Inc()
		log.Info("balance key range job finished due to balanced", zap.String("alias", job.Alias), zap.Uint64("job-id", job.JobID))
		return false
	}
	return true
}

func (s *balanceRangeScheduler) cleanJobStatus(cluster sche.SchedulerCluster, job *balanceRangeSchedulerJob) {
	km := cluster.GetKeyRangeManager()
	km.Delete(job.Ranges)
	for storeID := range s.scoreMap {
		storeStr := strconv.FormatUint(storeID, 10)
		balanceRangeGauge.WithLabelValues(storeStr, "score").Set(0)
		if _, ok := s.expectScoreMap[storeID]; ok {
			balanceRangeGauge.WithLabelValues(storeStr, "expect").Set(0)
		}
	}
}

// BalanceRangeCreateOption is used to create a scheduler with an option.
type BalanceRangeCreateOption func(s *balanceRangeScheduler)

// newBalanceRangeScheduler creates a scheduler that tends to keep given peer rule on
// special store balanced.
func newBalanceRangeScheduler(opController *operator.Controller, conf *balanceRangeSchedulerConfig, options ...BalanceRangeCreateOption) Scheduler {
	s := &balanceRangeScheduler{
		BaseScheduler: NewBaseScheduler(opController, types.BalanceRangeScheduler, conf),
		conf:          conf,
		handler:       newBalanceRangeHandler(conf),
	}
	for _, option := range options {
		option(s)
	}

	s.filters = []filter.Filter{
		&filter.StoreStateFilter{ActionScope: s.GetName(), TransferLeader: true, MoveRegion: true, OperatorLevel: constant.Medium},
		filter.NewSpecialUseFilter(s.GetName()),
	}
	s.filterCounter = filter.NewCounter(s.GetName())
	return s
}

// Schedule schedules the balance key range operator.
func (s *balanceRangeScheduler) Schedule(cluster sche.SchedulerCluster, _ bool) ([]*operator.Operator, []plan.Plan) {
	balanceRangeCounter.Inc()
	job := s.job
	defer s.filterCounter.Flush()

	faultStores := filter.SelectUnavailableTargetStores(s.stores, s.filters, cluster.GetSchedulerConfig(), nil, s.filterCounter)
	sources := filter.SelectSourceStores(s.stores, s.filters, cluster.GetSchedulerConfig(), nil, s.filterCounter)
	solver := s.solver
	replicaFilter := filter.NewRegionReplicatedFilter(cluster)
	baseRegionFilters := []filter.RegionFilter{
		filter.NewRegionDownFilter(),
		filter.NewSnapshotSendFilter(cluster.GetStores(), constant.Medium),
		filter.NewRegionPendingFilter(),
		replicaFilter,
	}

	for sourceIndex, sourceStore := range sources {
		solver.Source = sourceStore
		solver.sourceScore = s.score(solver.sourceStoreID())
		if solver.sourceScore <= s.expectScoreMap[solver.sourceStoreID()] {
			continue
		}
		switch job.Rule {
		case core.LeaderScatter:
			solver.Region = filter.SelectOneRegion(cluster.RandLeaderRegions(solver.sourceStoreID(), job.Ranges), nil, baseRegionFilters...)
		case core.LearnerScatter:
			solver.Region = filter.SelectOneRegion(cluster.RandLearnerRegions(solver.sourceStoreID(), job.Ranges), nil, baseRegionFilters...)
		case core.PeerScatter:
			solver.Region = filter.SelectOneRegion(cluster.RandFollowerRegions(solver.sourceStoreID(), job.Ranges), nil, baseRegionFilters...)
			if solver.Region == nil {
				solver.Region = filter.SelectOneRegion(cluster.RandLeaderRegions(solver.sourceStoreID(), job.Ranges), nil, baseRegionFilters...)
			}
			if solver.Region == nil {
				solver.Region = filter.SelectOneRegion(cluster.RandLearnerRegions(solver.sourceStoreID(), job.Ranges), nil, baseRegionFilters...)
			}
		}
		if solver.Region == nil {
			balanceRangeNoRegionCounter.Inc()
			continue
		}
		log.Debug("select region", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", solver.Region.GetID()))
		// Skip hot regions.
		if cluster.IsRegionHot(solver.Region) {
			log.Debug("region is hot", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", solver.Region.GetID()))
			balanceRangeHotCounter.Inc()
			continue
		}
		// Check region leader
		if solver.Region.GetLeader() == nil {
			log.Warn("region has no leader", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", solver.Region.GetID()))
			balanceRangeNoLeaderCounter.Inc()
			continue
		}
		solver.fit = replicaFilter.(*filter.RegionReplicatedFilter).GetFit()
		if op := s.transferPeer(sources[sourceIndex+1:], faultStores); op != nil {
			op.Counters = append(op.Counters, balanceRangeNewOperatorCounter)
			return []*operator.Operator{op}, nil
		}
	}
	return nil, nil
}

// transferPeer selects the best store to create a new peer to replace the old peer.
func (s *balanceRangeScheduler) transferPeer(dstStores []*core.StoreInfo, faultStores []*core.StoreInfo) *operator.Operator {
	solver := s.solver
	excludeTargets := solver.Region.GetStoreIDs()
	if s.job.Rule == core.LeaderScatter {
		excludeTargets = make(map[uint64]struct{})
		excludeTargets[solver.Region.GetLeader().GetStoreId()] = struct{}{}
	}
	for _, store := range faultStores {
		excludeTargets[store.GetID()] = struct{}{}
	}
	conf := solver.GetSchedulerConfig()
	filters := s.filters
	filters = append(filters,
		filter.NewExcludedFilter(s.GetName(), nil, excludeTargets),
		filter.NewPlacementSafeguard(s.GetName(), conf, solver.GetBasicCluster(), solver.GetRuleManager(), solver.Region, solver.Source, solver.fit),
	)

	candidates := filter.NewCandidates(s.R, dstStores).FilterTarget(conf, nil, s.filterCounter, filters...)
	for i := range candidates.Stores {
		solver.Target = candidates.Stores[len(candidates.Stores)-i-1]
		solver.targetScore = s.score(solver.targetStoreID())
		if solver.targetScore >= s.expectScoreMap[solver.targetStoreID()] {
			continue
		}
		regionID := solver.Region.GetID()
		sourceID := solver.sourceStoreID()
		targetID := solver.targetStoreID()
		if !s.shouldBalance(s.GetName()) {
			continue
		}
		log.Debug("candidate store", zap.Uint64("region-id", regionID), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID))

		oldPeer := solver.Region.GetStorePeer(sourceID)
		exist := false
		if s.job.Rule == core.LeaderScatter {
			peers := solver.Region.GetPeers()
			for _, peer := range peers {
				if peer.GetStoreId() == targetID {
					exist = true
					break
				}
			}
		}
		var op *operator.Operator
		var err error
		if exist {
			op, err = operator.CreateTransferLeaderOperator(s.GetName(), solver, solver.Region, targetID, []uint64{}, operator.OpRange)
		} else {
			newPeer := &metapb.Peer{StoreId: targetID, Role: oldPeer.Role}
			if solver.Region.GetLeader().GetStoreId() == sourceID {
				op, err = operator.CreateReplaceLeaderPeerOperator(s.GetName(), solver, solver.Region, operator.OpRange, oldPeer.GetStoreId(), newPeer, newPeer)
			} else {
				op, err = operator.CreateMovePeerOperator(s.GetName(), solver, solver.Region, operator.OpRange, oldPeer.GetStoreId(), newPeer)
			}
		}

		if err != nil {
			balanceRangeCreateOpFailCounter.Inc()
			return nil
		}
		op.FinishedCounters = append(op.FinishedCounters,
			balanceDirectionCounter.WithLabelValues(s.GetName(), solver.sourceMetricLabel(), "out"),
			balanceDirectionCounter.WithLabelValues(s.GetName(), solver.targetMetricLabel(), "in"),
		)
		op.SetAdditionalInfo("sourceScore", strconv.FormatFloat(s.score(sourceID), 'f', 2, 64))
		op.SetAdditionalInfo("targetScore", strconv.FormatFloat(s.score(targetID), 'f', 2, 64))
		op.SetAdditionalInfo("sourceExpectScore", strconv.FormatFloat(s.expectScoreMap[sourceID], 'f', 2, 64))
		op.SetAdditionalInfo("targetExpectScore", strconv.FormatFloat(s.expectScoreMap[targetID], 'f', 2, 64))
		return op
	}
	balanceRangeNoReplacementCounter.Inc()
	return nil
}

func (s *balanceRangeScheduler) prepare(cluster sche.SchedulerCluster, opInfluence operator.OpInfluence, job *balanceRangeSchedulerJob) error {
	basePlan := plan.NewBalanceSchedulerPlan()
	// todo: if supports to balance region size, it needs to change here.
	var kind constant.ScheduleKind
	switch job.Rule {
	case core.LeaderScatter:
		kind = constant.NewScheduleKind(constant.LeaderKind, constant.ByCount)
	default:
		kind = constant.NewScheduleKind(constant.RegionKind, constant.ByCount)
	}
	solver := newSolver(basePlan, kind, cluster, opInfluence)
	// only select source stores that are healthy and match the engine type and ignore the store limit restriction,
	filters := []filter.Filter{
		&filter.StoreStateFilter{ActionScope: s.GetName(), TransferLeader: true, MoveRegion: true, AllowTemporaryStates: true, OperatorLevel: constant.Medium},
		filter.NewStorageThresholdFilter(s.GetName()),
	}

	switch job.Engine {
	case core.EngineTiKV:
		filters = append(filters, filter.NewEngineFilter(string(types.BalanceRangeScheduler), filter.NotSpecialEngines))
	case core.EngineTiFlash:
		filters = append(filters, filter.NewEngineFilter(string(types.BalanceRangeScheduler), filter.SpecialEngines))
	default:
		return errs.ErrGetSourceStore.FastGenByArgs(job.Engine)
	}
	availableSource := filter.SelectSourceStores(cluster.GetStores(), filters, cluster.GetSchedulerConfig(), nil, nil)

	// filter some store that not match the rules in the key ranges
	sources := make([]*core.StoreInfo, 0)
	expectScoreMap := make(map[uint64]float64)
	for _, store := range availableSource {
		count := float64(0)
		for _, r := range job.Ranges {
			count += getCountThreshold(cluster, availableSource, store, r, job.Rule)
		}
		if count > 0 {
			sources = append(sources, store)
			expectScoreMap[store.GetID()] = count
		}
	}
	if len(sources) <= 1 {
		return errs.ErrNoStoreToBeSelected.FastGenByArgs()
	}
	// storeID <--> score mapping
	scoreMap := make(map[uint64]float64, len(sources))
	for _, source := range sources {
		count := 0
		for _, kr := range job.Ranges {
			switch job.Rule {
			case core.LeaderScatter:
				count += cluster.GetStoreLeaderCountByRange(source.GetID(), kr.StartKey, kr.EndKey)
			case core.PeerScatter:
				count += cluster.GetStorePeerCountByRange(source.GetID(), kr.StartKey, kr.EndKey)
			case core.LearnerScatter:
				count += cluster.GetStoreLearnerCountByRange(source.GetID(), kr.StartKey, kr.EndKey)
			default:
				return errs.ErrInvalidArgument.FastGenByArgs("not supported rule: " + job.Rule.String())
			}
		}
		scoreMap[source.GetID()] = float64(count)
	}

	sort.Slice(sources, func(i, j int) bool {
		iop := float64(solver.getOpInfluence(sources[i].GetID()))
		jop := float64(solver.getOpInfluence(sources[j].GetID()))
		iScore := scoreMap[sources[i].GetID()]
		jScore := scoreMap[sources[j].GetID()]
		return iScore+iop > jScore+jop
	})
	s.stores = sources
	s.scoreMap = scoreMap
	s.expectScoreMap = expectScoreMap
	s.solver = solver
	s.job = job
	s.report()
	return nil
}

func (s *balanceRangeScheduler) score(storeID uint64) float64 {
	return s.scoreMap[storeID]
}
func (s *balanceRangeScheduler) shouldBalance(scheduler string) bool {
	solve := s.solver
	sourceInf := solve.getOpInfluence(solve.sourceStoreID())
	// Sometimes, there are many remove-peer operators in the source store, we don't want to pick this store as source.
	if sourceInf < 0 {
		sourceInf = -sourceInf
	}
	// to avoid schedule too much, if A's core greater than B and C a little
	// we want that A should be moved out one region not two
	sourceScore := solve.sourceScore - float64(sourceInf) - 1

	targetInf := solve.getOpInfluence(solve.targetStoreID())
	// Sometimes, there are many add-peer operators in the target store, we don't want to pick this store as target.
	if targetInf < 0 {
		targetInf = -targetInf
	}
	targetScore := solve.targetScore + float64(targetInf) + 1

	// the source score must be greater than the target score
	shouldBalance := sourceScore >= targetScore
	if !shouldBalance && log.GetLevel() <= zap.DebugLevel {
		log.Debug("skip balance",
			zap.String("scheduler", scheduler),
			zap.Uint64("region-id", solve.Region.GetID()),
			zap.Uint64("source-store", solve.sourceStoreID()),
			zap.Uint64("target-store", solve.targetStoreID()),
			zap.Float64("origin-source-score", solve.sourceScore),
			zap.Float64("origin-target-score", solve.targetScore),
			zap.Float64("influence-source-score", sourceScore),
			zap.Float64("influence-target-score", targetScore),
			zap.Float64("expect-source-score", s.expectScoreMap[solve.sourceStoreID()]),
			zap.Float64("expect-target-score", s.expectScoreMap[solve.targetStoreID()]),
		)
	}
	return shouldBalance
}

// JobStatus is the status of the job.
type JobStatus int

const (
	pending JobStatus = iota
	running
	finished
	cancelled
)

func (s *JobStatus) String() string {
	switch *s {
	case pending:
		return "pending"
	case running:
		return "running"
	case finished:
		return "finished"
	case cancelled:
		return "cancelled"
	}
	return "unknown"
}

// MarshalJSON marshals to json.
func (s *JobStatus) MarshalJSON() ([]byte, error) {
	return []byte(`"` + s.String() + `"`), nil
}

// UnmarshalJSON unmarshals from json.
func (s *JobStatus) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case `"running"`:
		*s = running
	case `"finished"`:
		*s = finished
	case `"cancelled"`:
		*s = cancelled
	case `"pending"`:
		*s = pending
	}
	return nil
}
