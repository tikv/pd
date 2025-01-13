package schedulers

import (
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
)

const (
	// DefaultTimeout is the default balance key range scheduler timeout.
	DefaultTimeout = 1 * time.Hour
)

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
	baseDefaultSchedulerConfig
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
	filters       []filter.Filter
	filterCounter *filter.Counter
}

// ServeHTTP implements the http.Handler interface.
func (s *balanceKeyRangeScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// Schedule schedules the balance key range operator.
func (*balanceKeyRangeScheduler) Schedule(_cluster sche.SchedulerCluster, _dryRun bool) ([]*operator.Operator, []plan.Plan) {
	log.Debug("balance key range scheduler is scheduling, need to implement")
	return nil, nil
}

// IsScheduleAllowed checks if the scheduler is allowed to schedule new operators.
func (s *balanceKeyRangeScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpKeyRange) < cluster.GetSchedulerConfig().GetRegionScheduleLimit()
	if !allowed {
		operator.IncOperatorLimitCounter(s.GetType(), operator.OpKeyRange)
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
