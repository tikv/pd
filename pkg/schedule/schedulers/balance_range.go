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
	"bytes"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"
	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/metapb"
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
	"github.com/tikv/pd/pkg/utils/syncutil"
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
	return router
}

func (handler *balanceRangeSchedulerHandler) updateConfig(w http.ResponseWriter, _ *http.Request) {
	handler.rd.JSON(w, http.StatusBadRequest, "update config is not supported")
}

func (handler *balanceRangeSchedulerHandler) listConfig(w http.ResponseWriter, _ *http.Request) {
	conf := handler.config.clone()
	if err := handler.rd.JSON(w, http.StatusOK, conf); err != nil {
		log.Error("failed to marshal balance key range scheduler config", errs.ZapError(err))
	}
}

type balanceRangeSchedulerConfig struct {
	syncutil.RWMutex
	schedulerConfig
	jobs []*balanceRangeSchedulerJob
}

type balanceRangeSchedulerJob struct {
	JobID   uint64          `json:"job-id"`
	Role    Role            `json:"role"`
	Engine  string          `json:"engine"`
	Timeout time.Duration   `json:"timeout"`
	Ranges  []core.KeyRange `json:"ranges"`
	Alias   string          `json:"alias"`
	Start   *time.Time      `json:"start,omitempty"`
	Finish  *time.Time      `json:"finish,omitempty"`
	Create  time.Time       `json:"create"`
	Status  JobStatus       `json:"status"`
}

func (conf *balanceRangeSchedulerConfig) begin(index int) *balanceRangeSchedulerJob {
	conf.Lock()
	defer conf.Unlock()
	job := conf.jobs[index]
	if job.Status != pending {
		return nil
	}
	now := time.Now()
	job.Start = &now
	job.Status = running
	if err := conf.save(); err != nil {
		log.Warn("failed to persist config", zap.Error(err), zap.Uint64("job-id", job.JobID))
		job.Status = pending
		job.Start = nil
	}
	return job
}

func (conf *balanceRangeSchedulerConfig) finish(index int) *balanceRangeSchedulerJob {
	conf.Lock()
	defer conf.Unlock()
	job := conf.jobs[index]
	if job.Status != running {
		return nil
	}
	now := time.Now()
	job.Finish = &now
	job.Status = finished
	if err := conf.save(); err != nil {
		log.Warn("failed to persist config", zap.Error(err), zap.Uint64("job-id", job.JobID))
		job.Status = running
		job.Finish = nil
	}
	return job
}

func (conf *balanceRangeSchedulerConfig) peek() (int, *balanceRangeSchedulerJob) {
	conf.RLock()
	defer conf.RUnlock()
	for index, job := range conf.jobs {
		if job.Status == finished {
			continue
		}
		return index, job
	}
	return 0, nil
}

func (conf *balanceRangeSchedulerConfig) clone() []*balanceRangeSchedulerJob {
	conf.RLock()
	defer conf.RUnlock()
	jobs := make([]*balanceRangeSchedulerJob, 0, len(conf.jobs))
	for _, job := range conf.jobs {
		ranges := make([]core.KeyRange, len(job.Ranges))
		copy(ranges, job.Ranges)
		jobs = append(jobs, &balanceRangeSchedulerJob{
			Ranges:  ranges,
			Role:    job.Role,
			Engine:  job.Engine,
			Timeout: job.Timeout,
			Alias:   job.Alias,
			JobID:   job.JobID,
			Start:   job.Start,
		})
	}

	return jobs
}

// EncodeConfig serializes the config.
func (s *balanceRangeScheduler) EncodeConfig() ([]byte, error) {
	s.conf.RLock()
	defer s.conf.RUnlock()
	return EncodeConfig(s.conf.jobs)
}

// ReloadConfig reloads the config.
func (s *balanceRangeScheduler) ReloadConfig() error {
	s.conf.Lock()
	defer s.conf.Unlock()

	jobs := make([]*balanceRangeSchedulerJob, 0, len(s.conf.jobs))
	if err := s.conf.load(jobs); err != nil {
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
}

// ServeHTTP implements the http.Handler interface.
func (s *balanceRangeScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// IsScheduleAllowed checks if the scheduler is allowed to schedule new operators.
func (s *balanceRangeScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpRange) < cluster.GetSchedulerConfig().GetRegionScheduleLimit()
	if !allowed {
		operator.IncOperatorLimitCounter(s.GetType(), operator.OpRange)
	}
	index, job := s.conf.peek()
	if job != nil {
		if job.Status == pending {
			job = s.conf.begin(index)
		}
		// todo: add other conditions such as the diff of the score between the source and target store.
		if time.Since(*job.Start) > job.Timeout {
			s.conf.finish(index)
			balanceRangeExpiredCounter.Inc()
		}
	}

	return allowed
}

// BalanceRangeCreateOption is used to create a scheduler with an option.
type BalanceRangeCreateOption func(s *balanceRangeScheduler)

// newBalanceRangeScheduler creates a scheduler that tends to keep given peer Role on
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
		&filter.StoreStateFilter{ActionScope: s.GetName(), TransferLeader: true, OperatorLevel: constant.Medium},
		filter.NewSpecialUseFilter(s.GetName()),
	}
	s.filterCounter = filter.NewCounter(s.GetName())
	return s
}

// Schedule schedules the balance key range operator.
func (s *balanceRangeScheduler) Schedule(cluster sche.SchedulerCluster, _ bool) ([]*operator.Operator, []plan.Plan) {
	balanceRangeCounter.Inc()
	_, job := s.conf.peek()
	if job == nil {
		balanceRangeNoJobCounter.Inc()
		return nil, nil
	}

	opInfluence := s.OpController.GetOpInfluence(cluster.GetBasicCluster(), operator.WithRangeOption(job.Ranges))
	// todo: don't prepare every times, the prepare information can be reused.
	plan, err := s.prepare(cluster, opInfluence, job)
	if err != nil {
		log.Error("failed to prepare balance key range scheduler", errs.ZapError(err))
		return nil, nil
	}

	downFilter := filter.NewRegionDownFilter()
	replicaFilter := filter.NewRegionReplicatedFilter(cluster)
	snapshotFilter := filter.NewSnapshotSendFilter(cluster.GetStores(), constant.Medium)
	pendingFilter := filter.NewRegionPendingFilter()
	baseRegionFilters := []filter.RegionFilter{downFilter, replicaFilter, snapshotFilter, pendingFilter}

	for sourceIndex, sourceStore := range plan.stores {
		plan.source = sourceStore
		plan.sourceScore = plan.score(plan.source.GetID())
		if plan.sourceScore < plan.averageScore {
			break
		}
		switch job.Role {
		case leader:
			plan.region = filter.SelectOneRegion(cluster.RandLeaderRegions(plan.sourceStoreID(), job.Ranges), nil, baseRegionFilters...)
		case learner:
			plan.region = filter.SelectOneRegion(cluster.RandLearnerRegions(plan.sourceStoreID(), job.Ranges), nil, baseRegionFilters...)
		case follower:
			plan.region = filter.SelectOneRegion(cluster.RandFollowerRegions(plan.sourceStoreID(), job.Ranges), nil, baseRegionFilters...)
		}
		if plan.region == nil {
			balanceRangeNoRegionCounter.Inc()
			continue
		}
		log.Debug("select region", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", plan.region.GetID()))
		// Skip hot regions.
		if cluster.IsRegionHot(plan.region) {
			log.Debug("region is hot", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", plan.region.GetID()))
			balanceRangeHotCounter.Inc()
			continue
		}
		// Check region leader
		if plan.region.GetLeader() == nil {
			log.Warn("region has no leader", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", plan.region.GetID()))
			balanceRangeNoLeaderCounter.Inc()
			continue
		}
		plan.fit = replicaFilter.(*filter.RegionReplicatedFilter).GetFit()
		if op := s.transferPeer(plan, plan.stores[sourceIndex+1:]); op != nil {
			op.Counters = append(op.Counters, balanceRangeNewOperatorCounter)
			return []*operator.Operator{op}, nil
		}
	}
	return nil, nil
}

// transferPeer selects the best store to create a new peer to replace the old peer.
func (s *balanceRangeScheduler) transferPeer(plan *balanceRangeSchedulerPlan, dstStores []*core.StoreInfo) *operator.Operator {
	excludeTargets := plan.region.GetStoreIDs()
	if plan.job.Role == leader {
		excludeTargets = make(map[uint64]struct{})
		excludeTargets[plan.region.GetLeader().GetStoreId()] = struct{}{}
	}
	conf := plan.GetSchedulerConfig()
	filters := []filter.Filter{
		filter.NewExcludedFilter(s.GetName(), nil, excludeTargets),
		filter.NewPlacementSafeguard(s.GetName(), conf, plan.GetBasicCluster(), plan.GetRuleManager(), plan.region, plan.source, plan.fit),
	}
	candidates := filter.NewCandidates(s.R, dstStores).FilterTarget(conf, nil, s.filterCounter, filters...)
	for i := range candidates.Stores {
		plan.target = candidates.Stores[len(candidates.Stores)-i-1]
		plan.targetScore = plan.score(plan.target.GetID())
		if plan.targetScore > plan.averageScore {
			break
		}
		regionID := plan.region.GetID()
		sourceID := plan.source.GetID()
		targetID := plan.target.GetID()
		if !plan.shouldBalance(s.GetName()) {
			continue
		}
		log.Debug("candidate store", zap.Uint64("region-id", regionID), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID))

		oldPeer := plan.region.GetStorePeer(sourceID)
		exist := false
		if plan.job.Role == leader {
			peers := plan.region.GetPeers()
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
			op, err = operator.CreateTransferLeaderOperator(s.GetName(), plan, plan.region, plan.targetStoreID(), []uint64{}, operator.OpRange)
		} else {
			newPeer := &metapb.Peer{StoreId: plan.target.GetID(), Role: oldPeer.Role}
			op, err = operator.CreateMovePeerOperator(s.GetName(), plan, plan.region, operator.OpRange, oldPeer.GetStoreId(), newPeer)
		}

		if err != nil {
			balanceRangeCreateOpFailCounter.Inc()
			return nil
		}
		sourceLabel := strconv.FormatUint(sourceID, 10)
		targetLabel := strconv.FormatUint(targetID, 10)
		op.FinishedCounters = append(op.FinishedCounters,
			balanceDirectionCounter.WithLabelValues(s.GetName(), sourceLabel, targetLabel),
		)
		op.SetAdditionalInfo("sourceScore", strconv.FormatInt(plan.sourceScore, 10))
		op.SetAdditionalInfo("targetScore", strconv.FormatInt(plan.targetScore, 10))
		op.SetAdditionalInfo("tolerate", strconv.FormatInt(plan.tolerate, 10))
		return op
	}
	balanceRangeNoReplacementCounter.Inc()
	return nil
}

// balanceRangeSchedulerPlan is used to record the plan of balance key range scheduler.
type balanceRangeSchedulerPlan struct {
	sche.SchedulerCluster
	// stores is sorted by score desc
	stores []*core.StoreInfo
	// scoreMap records the storeID -> score
	scoreMap     map[uint64]int64
	source       *core.StoreInfo
	sourceScore  int64
	target       *core.StoreInfo
	targetScore  int64
	region       *core.RegionInfo
	fit          *placement.RegionFit
	averageScore int64
	job          *balanceRangeSchedulerJob
	opInfluence  operator.OpInfluence
	tolerate     int64
}

func fetchAllRegions(cluster sche.SchedulerCluster, ranges *core.KeyRanges) []*core.RegionInfo {
	scanLimit := 32
	regions := make([]*core.RegionInfo, 0)
	krs := ranges.Ranges()

	for _, kr := range krs {
		for {
			region := cluster.ScanRegions(kr.StartKey, kr.EndKey, scanLimit)
			if len(region) == 0 {
				break
			}
			regions = append(regions, region...)
			if len(region) < scanLimit {
				break
			}
			kr.StartKey = region[len(region)-1].GetEndKey()
			if bytes.Equal(kr.StartKey, kr.EndKey) {
				break
			}
		}
	}
	return regions
}

func (s *balanceRangeScheduler) prepare(cluster sche.SchedulerCluster, opInfluence operator.OpInfluence, job *balanceRangeSchedulerJob) (*balanceRangeSchedulerPlan, error) {
	filters := s.filters
	switch job.Engine {
	case core.EngineTiKV:
		filters = append(filters, filter.NewEngineFilter(string(types.BalanceRangeScheduler), filter.NotSpecialEngines))
	case core.EngineTiFlash:
		filters = append(filters, filter.NewEngineFilter(string(types.BalanceRangeScheduler), filter.SpecialEngines))
	default:
		return nil, errs.ErrGetSourceStore.FastGenByArgs(job.Engine)
	}
	sources := filter.SelectSourceStores(cluster.GetStores(), filters, cluster.GetSchedulerConfig(), nil, nil)
	if len(sources) <= 1 {
		return nil, errs.ErrStoresNotEnough.FastGenByArgs("no store to select")
	}

	krs := core.NewKeyRanges(job.Ranges)
	scanRegions := fetchAllRegions(cluster, krs)
	if len(scanRegions) == 0 {
		return nil, errs.ErrRegionNotFound.FastGenByArgs("no region found")
	}

	// storeID <--> score mapping
	scoreMap := make(map[uint64]int64, len(sources))
	for _, source := range sources {
		scoreMap[source.GetID()] = 0
	}
	totalScore := int64(0)
	for _, region := range scanRegions {
		for _, peer := range job.Role.getPeers(region) {
			scoreMap[peer.GetStoreId()] += 1
			totalScore += 1
		}
	}

	sort.Slice(sources, func(i, j int) bool {
		role := job.Role
		iop := role.getStoreInfluence(opInfluence.GetStoreInfluence(sources[i].GetID()))
		jop := role.getStoreInfluence(opInfluence.GetStoreInfluence(sources[j].GetID()))
		iScore := scoreMap[sources[i].GetID()]
		jScore := scoreMap[sources[j].GetID()]
		return iScore+iop > jScore+jop
	})

	averageScore := int64(0)
	averageScore = totalScore / int64(len(sources))

	tolerate := int64(float64(len(scanRegions)) * adjustRatio)
	if tolerate < 1 {
		tolerate = 1
	}
	return &balanceRangeSchedulerPlan{
		SchedulerCluster: cluster,
		stores:           sources,
		scoreMap:         scoreMap,
		source:           nil,
		target:           nil,
		region:           nil,
		averageScore:     averageScore,
		job:              job,
		opInfluence:      opInfluence,
		tolerate:         tolerate,
	}, nil
}

func (p *balanceRangeSchedulerPlan) sourceStoreID() uint64 {
	return p.source.GetID()
}

func (p *balanceRangeSchedulerPlan) targetStoreID() uint64 {
	return p.target.GetID()
}

func (p *balanceRangeSchedulerPlan) score(storeID uint64) int64 {
	return p.scoreMap[storeID]
}

func (p *balanceRangeSchedulerPlan) shouldBalance(scheduler string) bool {
	sourceInfluence := p.opInfluence.GetStoreInfluence(p.sourceStoreID())
	sourceInf := p.job.Role.getStoreInfluence(sourceInfluence)
	// Sometimes, there are many remove-peer operators in the source store, we don't want to pick this store as source.
	if sourceInf < 0 {
		sourceInf = -sourceInf
	}
	// to avoid schedule too much, if A's core greater than B and C a little
	// we want that A should be moved out one region not two
	sourceScore := p.sourceScore - sourceInf - p.tolerate

	targetInfluence := p.opInfluence.GetStoreInfluence(p.targetStoreID())
	targetInf := p.job.Role.getStoreInfluence(targetInfluence)
	// Sometimes, there are many add-peer operators in the target store, we don't want to pick this store as target.
	if targetInf < 0 {
		targetInf = -targetInf
	}
	targetScore := p.targetScore + targetInf + p.tolerate

	// the source score must be greater than the target score
	shouldBalance := sourceScore >= targetScore
	if !shouldBalance && log.GetLevel() <= zap.DebugLevel {
		log.Debug("skip balance",
			zap.String("scheduler", scheduler),
			zap.Uint64("region-id", p.region.GetID()),
			zap.Uint64("source-store", p.sourceStoreID()),
			zap.Uint64("target-store", p.targetStoreID()),
			zap.Int64("origin-source-score", p.sourceScore),
			zap.Int64("origin-target-score", p.targetScore),
			zap.Int64("influence-source-score", sourceScore),
			zap.Int64("influence-target-score", targetScore),
			zap.Int64("average-region-size", p.averageScore),
			zap.Int64("tolerate", p.tolerate),
		)
	}
	return shouldBalance
}

// Role is the role of the region.
type Role int

const (
	leader Role = iota
	follower
	learner
	unknown
)

func (r Role) String() string {
	switch r {
	case leader:
		return "leader"
	case follower:
		return "voter"
	case learner:
		return "learner"
	default:
		return "unknown"
	}
}

// JobStatus is the status of the job.
type JobStatus int

const (
	pending JobStatus = iota
	running
	finished
)

func (s JobStatus) String() string {
	switch s {
	case pending:
		return "pending"
	case running:
		return "running"
	case finished:
		return "finished"
	}
	return "unknown"
}

// MarshalJSON marshals to json.
func (s JobStatus) MarshalJSON() ([]byte, error) {
	return []byte(`"` + s.String() + `"`), nil
}

// NewRole creates a new role.
func NewRole(role string) Role {
	switch role {
	case "leader":
		return leader
	case "follower":
		return follower
	case "learner":
		return learner
	default:
		return unknown
	}
}

func (r Role) getPeers(region *core.RegionInfo) []*metapb.Peer {
	switch r {
	case leader:
		return []*metapb.Peer{region.GetLeader()}
	case follower:
		followers := region.GetFollowers()
		ret := make([]*metapb.Peer, 0, len(followers))
		for _, peer := range followers {
			ret = append(ret, peer)
		}
		return ret
	case learner:
		learners := region.GetLearners()
		return learners
	default:
		return nil
	}
}

func (r Role) getStoreInfluence(influence *operator.StoreInfluence) int64 {
	switch r {
	case leader:
		return influence.LeaderCount
	case follower:
		return influence.RegionCount
	case learner:
		return influence.RegionCount
	default:
		return 0
	}
}

// MarshalJSON marshals to json.
func (r Role) MarshalJSON() ([]byte, error) {
	return []byte(`"` + r.String() + `"`), nil
}
