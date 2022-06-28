package schedule

import (
	"fmt"

	"github.com/tikv/pd/server/schedule/diagnosis"
)

const MaxSampleNum = 10

type Plan interface {
	GetSourceStore() uint64
	GetRegion() uint64
	GetTargetStore() uint64
	GetStep() diagnosis.ScheduleStep
	GetReason() string
	IsSchedulable() bool
	GetFailObject() uint64
}

// type schedulePlan struct {
// 	source *core.StoreInfo
// 	target *core.StoreInfo
// 	region *core.RegionInfo

// 	score int64
// }

// func NewSchedulePlan() Plan {
// 	return &schedulePlan{}
// }

// func (p *schedulePlan) SourceMetricLabel() string {
// 	return strconv.FormatUint(p.source.GetID(), 10)
// }

// func (p *schedulePlan) TargetMetricLabel() string {
// 	return strconv.FormatUint(p.target.GetID(), 10)
// }

type MatrixPlanAnalyzer struct {
	scheduler string
	level1    *SchedulePlanTreeLevel1Node
	cluster   Cluster
}

func (a *MatrixPlanAnalyzer) PutPlan(plan Plan) {

}

type SchedulePlanTreeLevel1Node struct {
	stores     map[uint64]*SchedulePlanTreeLevel2Node
	usedStores map[uint64]bool
	cluster    Cluster
}

func NewSchedulePlanTreeLevel1Node(cluster Cluster) *SchedulePlanTreeLevel1Node {
	stores := cluster.GetStores()
	usedStores := make(map[uint64]bool)
	for _, store := range stores {
		usedStores[store.GetID()] = false
	}
	return &SchedulePlanTreeLevel1Node{
		stores:     make(map[uint64]*SchedulePlanTreeLevel2Node),
		usedStores: usedStores,
		cluster:    cluster,
	}
}

func (node *SchedulePlanTreeLevel1Node) putPlan(plan Plan) {
	source := plan.GetSourceStore()
	node.usedStores[source] = true

	if plan.GetStep() == 0 {

	}

}

type SchedulePlanTreeLevel2Node struct {
	stores map[uint64]*SchedulePlanTreeLevel3Node
}

type SchedulePlanTreeLevel3Node struct {
	reasons map[string]*SchedulePlanTreeLevel4Node
	count   int
}

func NewSchedulePlanTreeLevel3Node() *SchedulePlanTreeLevel3Node {
	return &SchedulePlanTreeLevel3Node{
		reasons: make(map[string]*SchedulePlanTreeLevel4Node),
	}
}

func (r *SchedulePlanTreeLevel3Node) Add(reason string, id uint64) {
	if _, ok := r.reasons[reason]; !ok {
		r.reasons[reason] = &SchedulePlanTreeLevel4Node{Reason: reason}
	}
	node := r.reasons[reason]
	node.Count++
	if node.Count < MaxSampleNum {
		node.Samples[id] = struct{}{}
	}
}

func (r *SchedulePlanTreeLevel3Node) Count() int {
	return r.count
}

func (r *SchedulePlanTreeLevel3Node) GetMostReason() (most *SchedulePlanTreeLevel4Node) {
	for _, recorder := range r.reasons {
		if most == nil || most.Count < recorder.Count {
			most = recorder
		}
	}
	return
}

func (r *SchedulePlanTreeLevel3Node) GetAllReasons() map[string]*SchedulePlanTreeLevel4Node {
	return r.reasons
}

type SchedulePlanTreeLevel4Node struct {
	Reason  string
	Samples map[uint64]struct{}
	Count   int
}

type PeriodicalPlanAnalyzer struct {
	storeID uint64
	// schedulable is true when scheduler can create operator for specific store
	schedulable   bool
	stepRecorders []SchedulePlanTreeLevel3Node
	maxStep       diagnosis.ScheduleStep
}

func (a *PeriodicalPlanAnalyzer) PutPlan(plan Plan) {
	if plan.IsSchedulable() {
		a.schedulable = true
		return
	}
	step := plan.GetStep()
	a.stepRecorders[plan.GetStep()].Add(plan.GetReason(), plan.GetFailObject())
	if step > a.maxStep {
		a.maxStep = step
	}
}

func (a *PeriodicalPlanAnalyzer) Schedulable() bool {
	return a.schedulable
}

func (a *PeriodicalPlanAnalyzer) GetFinalReason() string {
	recoder := a.stepRecorders[a.maxStep]
	reason := recoder.GetMostReason()
	return reason.Reason
}

func (a *PeriodicalPlanAnalyzer) GetFinalStep() diagnosis.ScheduleStep {
	return a.maxStep
}

func (a *PeriodicalPlanAnalyzer) AnalysisResult(scope string) *diagnosis.StepDiagnosisResult {
	var description, reason string
	if a.schedulable {
		description = fmt.Sprintf("%s can create scheduling operator at store-%d", scope, a.storeID)
	} else {
		description = fmt.Sprintf("%s can't create schedule operator from store-%d in %s.", scope, a.storeID, a.GetFinalStep().Name())
		reason = a.maxStep.Reason(a.GetFinalReason(), a.storeID)
	}
	detailed := make([]*diagnosis.ReasonMetrics, 0)
	for i, stepRecord := range a.stepRecorders {
		step := diagnosis.ScheduleStep(i)
		for _, node := range stepRecord.GetAllReasons() {
			keys := make([]uint64, 0, len(node.Samples))
			for k := range node.Samples {
				keys = append(keys, k)
			}

			detailed = append(detailed, &diagnosis.ReasonMetrics{
				Step:         step.Description(),
				Reason:       node.Reason,
				Ratio:        fmt.Sprintf("%.2f", float64(node.Count)/float64(stepRecord.Count())),
				SampleObject: diagnosis.SampleObjects(keys).Object(step),
			})
		}
	}

	result := &diagnosis.StepDiagnosisResult{
		SchedulerName: scope,
		StoreID:       a.storeID,
		Schedulable:   a.schedulable,
		Description:   description,
		Reason:        reason,
		Detailed:      detailed,
	}
	return result
}
