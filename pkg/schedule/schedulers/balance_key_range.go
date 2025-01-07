package schedulers

import (
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/log"
	_ "github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/unrolled/render"
)

const (
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

func (handler *balanceKeyRangeSchedulerHandler) updateConfig(w http.ResponseWriter, r *http.Request) {
	handler.rd.JSON(w, http.StatusBadRequest, "update config is not supported")
}

func (handler *balanceKeyRangeSchedulerHandler) listConfig(w http.ResponseWriter, _ *http.Request) {
	conf := handler.config.clone()
	handler.rd.JSON(w, http.StatusOK, conf)
}

type balanceKeyRangeSchedulerConfig struct {
	baseDefaultSchedulerConfig
	balanceKeyRangeSchedulerParam
}

type balanceKeyRangeSchedulerParam struct {
	Role     string        `json:"role"`
	Engine   string        `json:"engine"`
	StartKey string        `json:"start_key"`
	EndKey   string        `json:"end_key"`
	Timeout  time.Duration `json:"timeout"`
}

func (conf *balanceKeyRangeSchedulerConfig) encodeConfig() ([]byte, error) {
	conf.RLock()
	defer conf.RUnlock()
	return EncodeConfig(conf)
}

func (conf *balanceKeyRangeSchedulerConfig) clone() *balanceKeyRangeSchedulerParam {
	conf.RLock()
	defer conf.RUnlock()
	return &balanceKeyRangeSchedulerParam{
		Role:     conf.Role,
		Engine:   conf.Engine,
		StartKey: conf.StartKey,
		EndKey:   conf.EndKey,
	}
}

func (conf *balanceKeyRangeSchedulerConfig) parseFromArgs(args []string) error {
	if len(args) < 4 {
		return errs.ErrSchedulerConfig.FastGenByArgs("args length should be greater than 4")
	}
	newConf := &balanceKeyRangeSchedulerConfig{}
	var err error
	newConf.StartKey, err = url.QueryUnescape(args[0])
	if err != nil {
		return errs.ErrQueryUnescape.Wrap(err)
	}
	newConf.EndKey, err = url.QueryUnescape(args[1])
	if err != nil {
		return errs.ErrQueryUnescape.Wrap(err)
	}

	newConf.Role, err = url.QueryUnescape(args[2])
	if err != nil {
		return errs.ErrQueryUnescape.Wrap(err)
	}

	newConf.Engine, err = url.QueryUnescape(args[3])
	if err != nil {
		return errs.ErrQueryUnescape.Wrap(err)
	}
	if len(args) >= 5 {
		timeout, err := url.QueryUnescape(args[4])
		if err != nil {
			return errs.ErrQueryUnescape.Wrap(err)
		}
		conf.Timeout, err = time.ParseDuration(timeout)
		if err != nil {
			return errs.ErrQueryUnescape.Wrap(err)
		}
	} else {
		conf.Timeout = DefaultTimeout
	}
	*newConf = *newConf
	return nil
}

func (s *balanceKeyRangeScheduler) EncodeConfig() ([]byte, error) {
	return s.conf.encodeConfig()
}

func (s *balanceKeyRangeScheduler) ReloadConfig() error {
	return nil
}

type balanceKeyRangeScheduler struct {
	*BaseScheduler
	conf          *balanceKeyRangeSchedulerConfig
	handler       http.Handler
	filters       []filter.Filter
	filterCounter *filter.Counter
}

func (s *balanceKeyRangeScheduler) Schedule(cluster sche.SchedulerCluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	log.Info("balance key range scheduler is scheduling, need to implement")
	return nil, nil
}

func (s *balanceKeyRangeScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpKeyRange) < cluster.GetSchedulerConfig().GetRegionScheduleLimit()
	if !allowed {
		operator.IncOperatorLimitCounter(s.GetType(), operator.OpKeyRange)
	}
	return allowed
}

type BalanceKeyRangeCreateOption func(s *balanceKeyRangeScheduler)

// newBalanceKeyRangeScheduler creates a scheduler that tends to keep given peer role on
// special store balanced.
func newBalanceKeyRangeScheduler(opController *operator.Controller, conf *balanceKeyRangeSchedulerConfig, options ...BalanceKeyRangeCreateOption) Scheduler {
	s := &balanceKeyRangeScheduler{
		BaseScheduler: NewBaseScheduler(opController, types.BalanceLeaderScheduler, conf),
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
