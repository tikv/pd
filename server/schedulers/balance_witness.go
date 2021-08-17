package schedulers

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"go.uber.org/zap"
	"sort"
	"strconv"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(BalanceWitnessType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*balanceWitnessSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Name = BalanceWitnessName
			return nil
		}
	})
	schedule.RegisterScheduler(BalanceWitnessType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &balanceWitnessSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newBalanceWitnessScheduler(opController, conf), nil
	})
}

const (
	// balanceWitnessRetryLimit is the limit to retry schedule for selected store.
	balanceWitnessRetryLimit = 10
	// BalanceWitnessName is balance witness scheduler name.
	BalanceWitnessName = "balance-witness-scheduler"
	// BalanceWitnessType is balance witness scheduler type.
	BalanceWitnessType = "balance-witness"
)

type balanceWitnessSchedulerConfig struct {
	Name   string          `json:"name"`
	Ranges []core.KeyRange `json:"ranges"`
}

type balanceWitnessScheduler struct {
	*BaseScheduler
	conf         *balanceWitnessSchedulerConfig
	opController *schedule.OperatorController
	filters      []filter.Filter
	counter      *prometheus.CounterVec
}

// newBalanceWitnessScheduler creates a scheduler that tends to keep witnesses on
// each store balanced.
func newBalanceWitnessScheduler(opController *schedule.OperatorController, conf *balanceWitnessSchedulerConfig, options ...BalanceWitnessCreateOption) schedule.Scheduler {
	base := NewBaseScheduler(opController)

	s := &balanceWitnessScheduler{
		BaseScheduler: base,
		conf:          conf,
		opController:  opController,
		counter:       balanceWitnessCounter,
	}
	for _, option := range options {
		option(s)
	}
	s.filters = []filter.Filter{
		&filter.StoreStateFilter{ActionScope: s.GetName(), TransferLeader: true},
		filter.NewSpecialUseFilter(s.GetName()),
	}
	return s
}

// BalanceWitnessCreateOption is used to create a scheduler with an option.
type BalanceWitnessCreateOption func(s *balanceWitnessScheduler)

// WithBalanceWitnessCounter sets the counter for the scheduler.
func WithBalanceWitnessCounter(counter *prometheus.CounterVec) BalanceWitnessCreateOption {
	return func(s *balanceWitnessScheduler) {
		s.counter = counter
	}
}

// WithBalanceWitnessName sets the name for the scheduler.
func WithBalanceWitnessName(name string) BalanceWitnessCreateOption {
	return func(s *balanceWitnessScheduler) {
		s.conf.Name = name
	}
}

func (w *balanceWitnessScheduler) GetName() string {
	return w.conf.Name
}

func (w *balanceWitnessScheduler) GetType() string {
	return BalanceWitnessType
}

func (w *balanceWitnessScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(w.conf)
}

func (w *balanceWitnessScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	allowed := w.opController.OperatorCount(operator.OpWitness) < cluster.GetOpts().GetRegionScheduleLimit()/3
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(w.GetType(), operator.OpWitness.String()).Inc()
	}
	return allowed
}

func (w *balanceWitnessScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(w.GetName(), "schedule").Inc()

	stores := cluster.GetStores()
	opts := cluster.GetOpts()
	stores = filter.SelectSourceStores(stores, w.filters, opts)
	opInfluence := w.opController.GetOpInfluence(cluster)
	w.OpController.GetFastOpInfluence(cluster, opInfluence)
	kind := core.NewScheduleKind(core.WitnessKind, core.ByCount)
	plan := newBalancePlan(kind, cluster, opInfluence)

	sort.Slice(stores, func(i, j int) bool {
		iOp := plan.GetOpInfluence(stores[i].GetID())
		jOp := plan.GetOpInfluence(stores[j].GetID())
		return stores[i].WitnessScore(int(iOp)) > stores[j].WitnessScore(int(jOp))
	})

	for _, plan.source = range stores {
		for i := 0; i < balanceWitnessRetryLimit; i++ {
			schedulerCounter.WithLabelValues(w.GetName(), "total").Inc()
			plan.region = cluster.RandWitnessRegion(plan.SourceStoreID(), w.conf.Ranges, opt.HealthAllowPending(cluster), opt.ReplicatedRegion(cluster), opt.AllowBalanceEmptyRegion(cluster))
			if plan.region == nil {
				schedulerCounter.WithLabelValues(w.GetName(), "no-witness").Inc()
				continue
			}
			log.Debug("select witness region", zap.String("scheduler", w.GetName()), zap.Uint64("region-id", plan.region.GetID()))

			// Check whether the region has a leader.
			if plan.region.GetLeader() == nil {
				log.Warn("region have no leader", zap.String("scheduler", w.GetName()), zap.Uint64("region-id", plan.region.GetID()))
				schedulerCounter.WithLabelValues(w.GetName(), "no-leader").Inc()
				continue
			}
			if op := w.transferPeer(plan); op != nil {
				op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(w.GetName(), "new-operator"))
				return []*operator.Operator{op}
			}
		}
	}
	return nil
}

// transferPeer selects the best store to create a new witness peer to replace the old witness peer.
func (w *balanceWitnessScheduler) transferPeer(plan *balancePlan) *operator.Operator {
	filters := []filter.Filter{
		filter.NewExcludedFilter(w.GetName(), nil, plan.region.GetStoreIds()),
		filter.NewPlacementSafeguard(w.GetName(), plan.cluster, plan.region, plan.source),
		filter.NewSpecialUseFilter(w.GetName()),
		&filter.StoreStateFilter{ActionScope: w.GetName(), MoveRegion: true},
	}
	candidates := filter.NewCandidates(plan.cluster.GetStores()).
		FilterTarget(plan.cluster.GetOpts(), filters...).
		Sort(filter.WitnessScoreComparer())

	for _, plan.target = range candidates.Stores {
		regionID := plan.region.GetID()
		sourceID := plan.source.GetID()
		targetID := plan.target.GetID()
		log.Debug("", zap.Uint64("region-id", regionID), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID))

		if !plan.shouldBalance(w.GetName()) {
			schedulerCounter.WithLabelValues(w.GetName(), "skip").Inc()
			continue
		}

		oldPeer := plan.region.GetStorePeer(sourceID)
		newPeer := &metapb.Peer{StoreId: plan.target.GetID(), Role: oldPeer.Role, Witness: oldPeer.Witness}
		op, err := operator.CreateMovePeerOperator(BalanceWitnessType, plan.cluster, plan.region, operator.OpWitness, oldPeer.GetStoreId(), newPeer)
		if err != nil {
			schedulerCounter.WithLabelValues(w.GetName(), "create-operator-fail").Inc()
			return nil
		}
		sourceLabel := strconv.FormatUint(sourceID, 10)
		targetLabel := strconv.FormatUint(targetID, 10)
		op.Counters = append(op.Counters,
			balanceDirectionCounter.WithLabelValues(w.GetName(), sourceLabel, targetLabel),
		)
		op.FinishedCounters = append(op.FinishedCounters,
			w.counter.WithLabelValues("move-witness", sourceLabel+"-out"),
			w.counter.WithLabelValues("move-witness", targetLabel+"-in"),
		)
		op.AdditionalInfos["sourceScore"] = strconv.FormatFloat(plan.sourceScore, 'f', 2, 64)
		op.AdditionalInfos["targetScore"] = strconv.FormatFloat(plan.targetScore, 'f', 2, 64)
		return op
	}
	return nil
}
