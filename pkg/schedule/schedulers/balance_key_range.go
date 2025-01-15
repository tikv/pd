package schedulers

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"net/http"
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
	config *balanceKeyRangeSchedulerConfig
}

func newBalanceKeyRangeHandler(conf *balanceKeyRangeSchedulerConfig) http.Handler {
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

type balanceKeyRangeSchedulerConfig struct {
	syncutil.RWMutex
	schedulerConfig
	balanceKeyRangeSchedulerParam
}

type balanceKeyRangeSchedulerParam struct {
	Role    string          `json:"role"`
	Engine  string          `json:"engine"`
	Timeout time.Duration   `json:"timeout"`
	Ranges  []core.KeyRange `json:"ranges"`
}

func (conf *balanceKeyRangeSchedulerConfig) encodeConfig() ([]byte, error) {
	conf.RLock()
	defer conf.RUnlock()
	return EncodeConfig(conf)
}

func (conf *balanceKeyRangeSchedulerConfig) clone() *balanceKeyRangeSchedulerParam {
	conf.RLock()
	defer conf.RUnlock()
	ranges := make([]core.KeyRange, len(conf.Ranges))
	copy(ranges, conf.Ranges)
	return &balanceKeyRangeSchedulerParam{
		Ranges:  ranges,
		Role:    conf.Role,
		Engine:  conf.Engine,
		Timeout: conf.Timeout,
	}
}

// EncodeConfig serializes the config.
func (s *balanceKeyRangeScheduler) EncodeConfig() ([]byte, error) {
	return s.conf.encodeConfig()
}

// ReloadConfig reloads the config.
func (s *balanceKeyRangeScheduler) ReloadConfig() error {
	s.conf.Lock()
	defer s.conf.Unlock()

	newCfg := &balanceKeyRangeSchedulerConfig{}
	if err := s.conf.load(newCfg); err != nil {
		return err
	}
	s.conf.Ranges = newCfg.Ranges
	s.conf.Timeout = newCfg.Timeout
	s.conf.Role = newCfg.Role
	s.conf.Engine = newCfg.Engine
	return nil
}

type balanceKeyRangeScheduler struct {
	*BaseScheduler
	conf          *balanceKeyRangeSchedulerConfig
	handler       http.Handler
	start         time.Time
	role          Role
	filters       []filter.Filter
	filterCounter *filter.Counter
}

// ServeHTTP implements the http.Handler interface.
func (s *balanceKeyRangeScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// IsScheduleAllowed checks if the scheduler is allowed to schedule new operators.
func (s *balanceKeyRangeScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpKeyRange) < cluster.GetSchedulerConfig().GetRegionScheduleLimit()
	if !allowed {
		operator.IncOperatorLimitCounter(s.GetType(), operator.OpKeyRange)
	}
	if time.Now().Sub(s.start) > s.conf.Timeout {
		allowed = false
		balanceExpiredCounter.Inc()
	}
	return allowed
}

// BalanceKeyRangeCreateOption is used to create a scheduler with an option.
type BalanceKeyRangeCreateOption func(s *balanceKeyRangeScheduler)

// newBalanceKeyRangeScheduler creates a scheduler that tends to keep given peer role on
// special store balanced.
func newBalanceKeyRangeScheduler(opController *operator.Controller, conf *balanceKeyRangeSchedulerConfig, options ...BalanceKeyRangeCreateOption) Scheduler {
	s := &balanceKeyRangeScheduler{
		BaseScheduler: NewBaseScheduler(opController, types.BalanceKeyRangeScheduler, conf),
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
func (s *balanceKeyRangeScheduler) Schedule(cluster sche.SchedulerCluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	balanceKeyRangeCounter.Inc()
	plan,err:=s.prepare(cluster)
	if err != nil {
		log.Error("failed to prepare balance key range scheduler", errs.ZapError(err))
		return nil,nil

	}
}

// BalanceKeyRangeSchedulerPlan is used to record the plan of balance key range scheduler.
type BalanceKeyRangeSchedulerPlan struct {
	source []*core.StoreInfo
	// store_id -> score
	scores map[uint64]uint64
	// store_id -> peer
	regions map[uint64]*metapb.Peer
}

func (s *balanceKeyRangeScheduler) prepare(cluster sche.SchedulerCluster)(*BalanceKeyRangeSchedulerPlan,error) {
	krs := core.NewKeyRanges(s.conf.Ranges)
	scanRegions, err := cluster.BatchScanRegions(krs)
	if err != nil {
		return nil,err
	}
	stores := cluster.GetStores()
	sources := filter.SelectSourceStores(stores, s.filters, cluster.GetSchedulerConfig(), nil, nil)
	scores := make(map[uint64]uint64, len(sources))
	regions:=make(map[uint64]*metapb.Peer,len(scanRegions))
	for _, region := range scanRegions {
		for _, peer := range s.role.getPeers(region) {
			scores[peer.GetStoreId()] += 1
			regions[peer.GetStoreId()] = peer
		}
	}
	return &BalanceKeyRangeSchedulerPlan{
		source: sources,
		scores: scores,
		regions: regions,
	},nil
}



type Role int

const (
	Leader Role = iota
	Voter
	Learner
	Unknown
	RoleLen
)

func (r Role) String() string {
	switch r {
	case Leader:
		return "leader"
	case Voter:
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
	case "voter":
		return Voter
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
	case Voter:
		return region.GetVoters()
	case Learner:
		return region.GetLearners()
	default:
		return nil
	}
}
