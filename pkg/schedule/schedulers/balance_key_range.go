package schedulers

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/schedule/placement"
	"go.uber.org/zap"
	"net/http"
	"sort"
	"time"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const balanceKeyRangeName = "balance-key-ranges"

type balanceKeyRangeSchedulerHandler struct {
	rd     *render.Render
	config *balanceRangeSchedulerConfig
}

func newBalanceKeyRangeHandler(conf *balanceRangeSchedulerConfig) http.Handler {
	handler := &balanceKeyRangeSchedulerHandler{
		config: conf,
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/config", handler.updateConfig).Methods(http.MethodPost)
	router.HandleFunc("/list", handler.listConfig).Methods(http.MethodGet)
	return router
}

func (handler *balanceKeyRangeSchedulerHandler) updateConfig(w http.ResponseWriter, _ *http.Request) {
	handler.rd.JSON(w, http.StatusBadRequest, "update config is not supported")
}

func (handler *balanceKeyRangeSchedulerHandler) listConfig(w http.ResponseWriter, _ *http.Request) {
	conf := handler.config.clone()
	if err := handler.rd.JSON(w, http.StatusOK, conf); err != nil {
		log.Error("failed to marshal balance key range scheduler config", errs.ZapError(err))
	}
}

type balanceRangeSchedulerConfig struct {
	syncutil.RWMutex
	schedulerConfig
	balanceRangeSchedulerParam
}

type balanceRangeSchedulerParam struct {
	Role    string          `json:"role"`
	Engine  string          `json:"engine"`
	Timeout time.Duration   `json:"timeout"`
	Ranges  []core.KeyRange `json:"ranges"`
}

func (conf *balanceRangeSchedulerConfig) encodeConfig() ([]byte, error) {
	conf.RLock()
	defer conf.RUnlock()
	return EncodeConfig(conf)
}

func (conf *balanceRangeSchedulerConfig) clone() *balanceRangeSchedulerParam {
	conf.RLock()
	defer conf.RUnlock()
	ranges := make([]core.KeyRange, len(conf.Ranges))
	copy(ranges, conf.Ranges)
	return &balanceRangeSchedulerParam{
		Ranges:  ranges,
		Role:    conf.Role,
		Engine:  conf.Engine,
		Timeout: conf.Timeout,
	}
}

// EncodeConfig serializes the config.
func (s *balanceRangeScheduler) EncodeConfig() ([]byte, error) {
	return s.conf.encodeConfig()
}

// ReloadConfig reloads the config.
func (s *balanceRangeScheduler) ReloadConfig() error {
	s.conf.Lock()
	defer s.conf.Unlock()

	newCfg := &balanceRangeSchedulerConfig{}
	if err := s.conf.load(newCfg); err != nil {
		return err
	}
	s.conf.Ranges = newCfg.Ranges
	s.conf.Timeout = newCfg.Timeout
	s.conf.Role = newCfg.Role
	s.conf.Engine = newCfg.Engine
	return nil
}

type balanceRangeScheduler struct {
	*BaseScheduler
	conf          *balanceRangeSchedulerConfig
	handler       http.Handler
	start         time.Time
	role          Role
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
	if time.Now().Sub(s.start) > s.conf.Timeout {
		allowed = false
		balanceRangeExpiredCounter.Inc()
	}
	return allowed
}

// BalanceKeyRangeCreateOption is used to create a scheduler with an option.
type BalanceKeyRangeCreateOption func(s *balanceRangeScheduler)

// newBalanceKeyRangeScheduler creates a scheduler that tends to keep given peer role on
// special store balanced.
func newBalanceKeyRangeScheduler(opController *operator.Controller, conf *balanceRangeSchedulerConfig, options ...BalanceKeyRangeCreateOption) Scheduler {
	s := &balanceRangeScheduler{
		BaseScheduler: NewBaseScheduler(opController, types.BalanceRangeScheduler, conf),
		conf:          conf,
		handler:       newBalanceKeyRangeHandler(conf),
		start:         time.Now(),
		role:          NewRole(conf.Role),
	}
	for _, option := range options {
		option(s)
	}
	f := filter.NotSpecialEngines
	if conf.Engine == core.EngineTiFlash {
		f = filter.TiFlashEngineConstraint
	}
	s.filters = []filter.Filter{
		filter.NewEngineFilter(balanceKeyRangeName, f),
	}

	s.filterCounter = filter.NewCounter(s.GetName())
	return s
}

// Schedule schedules the balance key range operator.
func (s *balanceRangeScheduler) Schedule(cluster sche.SchedulerCluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	balanceRangeCounter.Inc()
	plan,err:=s.prepare(cluster)
	downFilter := filter.NewRegionDownFilter()
	replicaFilter := filter.NewRegionReplicatedFilter(cluster)
	snapshotFilter := filter.NewSnapshotSendFilter(plan.stores, constant.Medium)
	baseRegionFilters := []filter.RegionFilter{downFilter, replicaFilter, snapshotFilter}

	for sourceIndex,sourceStore:=range plan.stores{
		plan.source=sourceStore
		switch s.role{
		case Leader:
			plan.region=filter.SelectOneRegion(cluster.RandLeaderRegions(plan.sourceStoreID(), s.conf.Ranges), nil,baseRegionFilters...)
		case Learner:
			plan.region=filter.SelectOneRegion(cluster.RandLearnerRegions(plan.sourceStoreID(), s.conf.Ranges), nil,baseRegionFilters...)
		case Follower:
			plan.region=filter.SelectOneRegion(cluster.RandFollowerRegions(plan.sourceStoreID(), s.conf.Ranges), nil,baseRegionFilters...)
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
			log.Warn("region have no leader", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", solver.Region.GetID()))
			balanceRangeNoLeaderCounter.Inc()
			continue
		}
		plan.fit = replicaFilter.(*filter.RegionReplicatedFilter).GetFit()
		if op := s.transferPeer(plan, plan.stores[sourceIndex+1:]); op != nil {
			op.Counters = append(op.Counters, balanceRegionNewOpCounter)
			return []*operator.Operator{op}, nil
		}
	}

	if err != nil {
		log.Error("failed to prepare balance key range scheduler", errs.ZapError(err))
		return nil,nil

	}
}

// transferPeer selects the best store to create a new peer to replace the old peer.
func (s *balanceRangeScheduler) transferPeer(plan *balanceRangeSchedulerPlan, dstStores []*storeInfo) *operator.Operator {
	excludeTargets := plan.region.GetStoreIDs()
	if s.role!=Leader{
		excludeTargets = append(excludeTargets, plan.sourceStoreID())
	}
	return nil
}

// balanceRangeSchedulerPlan is used to record the plan of balance key range scheduler.
type balanceRangeSchedulerPlan struct {
	// stores is sorted by score desc
	stores []*storeInfo
	source *storeInfo
	target *storeInfo
	region *core.RegionInfo
	fit *placement.RegionFit
}

type storeInfo struct {
	store *core.StoreInfo
	score uint64
}

func (s *balanceRangeScheduler) prepare(cluster sche.SchedulerCluster)(*balanceRangeSchedulerPlan,error) {
	krs := core.NewKeyRanges(s.conf.Ranges)
	scanRegions, err := cluster.BatchScanRegions(krs)
	if err != nil {
		return nil,err
	}
	sources := filter.SelectSourceStores(cluster.GetStores(), s.filters, cluster.GetSchedulerConfig(), nil, nil)
	storeInfos:=make(map[uint64]*storeInfo,len(sources))
	for _, source := range sources {
		storeInfos[source.GetID()] = &storeInfo{store: source}
	}
	for _, region := range scanRegions {
		for _, peer := range s.role.getPeers(region) {
			storeInfos[peer.GetStoreId()].score += 1
		}
	}

	stores:=make([]*storeInfo,0,len(storeInfos))
	for _, store := range storeInfos {
		stores = append(stores, store)
	}
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].score > stores[j].score
	})
	return &balanceRangeSchedulerPlan{
		stores:stores,
		source: nil,
		target: nil,
		region: nil,
	},nil
}

func (p *balanceRangeSchedulerPlan) sourceStoreID() uint64 {
	return p.source.store.GetID()
}

func (p *balanceRangeSchedulerPlan) targetStoreID() uint64 {
	return p.target.store.GetID()
}



type Role int

const (
	Leader Role = iota
	Follower
	Learner
	Unknown
	RoleLen
)

func (r Role) String() string {
	switch r {
	case Leader:
		return "leader"
	case Follower:
		return "voter"
	case Learner:
		return "learner"
	default:
		return "unknown"
	}
}

func NewRole(role string) Role {
	switch role {
	case "leader":
		return Leader
	case "follower":
		return Follower
	case "learner":
		return Learner
	default:
		return Unknown
	}
}

func (r Role) getPeers(region *core.RegionInfo) []*metapb.Peer {{
	switch r {
	case Leader:
		return []*metapb.Peer{region.GetLeader()}
	case Follower:
		 followers:=region.GetFollowers()
		 ret:=make([]*metapb.Peer,len(followers))
		 for _,peer:=range followers{
		 	ret=append(ret,peer)
		 }
		 return ret
	case Learner:
		return region.GetLearners()
	default:
		return nil
	}
}
