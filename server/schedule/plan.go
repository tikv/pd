package schedule

import (
	"fmt"

	"github.com/tikv/pd/server/schedule/diagnosis"
)

const MaxSampleNum = 5

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
	*SchedulePlanTreeLevel1Node
	usedStores map[uint64]bool
	cluster    Cluster
}

func NewMatrixPlanAnalyzer(scope string, cluster Cluster) *MatrixPlanAnalyzer {
	stores := cluster.GetStores()
	usedStores := make(map[uint64]bool)
	for _, store := range stores {
		usedStores[store.GetID()] = false
	}
	return &MatrixPlanAnalyzer{
		SchedulePlanTreeLevel1Node: NewSchedulePlanTreeLevel1Node(cluster),
		usedStores:                 usedStores,
		cluster:                    cluster,
	}
}

func (a *MatrixPlanAnalyzer) PutPlan(plan Plan) {
	source := plan.GetSourceStore()
	a.usedStores[source] = true
	a.SchedulePlanTreeLevel1Node.putPlan(plan)
}

func (a *MatrixPlanAnalyzer) AnalysisResult() *diagnosis.MatrixDiagnosisResult {
	result := diagnosis.NewMatrixDiagnosisResult(a.scheduler)
	for storeID, used := range a.usedStores {
		if !used {
			result.AppendResult(diagnosis.NewAllEmptyMatrixStoreDiagnosisResult(storeID))
		} else {
			result.AppendResult(a.SchedulePlanTreeLevel1Node.AnalysisResult(storeID))
		}
	}
	return result
}

type SchedulePlanTreeLevel1Node struct {
	stores  map[uint64]*SchedulePlanTreeLevel2Node
	cluster Cluster
}

func NewSchedulePlanTreeLevel1Node(cluster Cluster) *SchedulePlanTreeLevel1Node {
	return &SchedulePlanTreeLevel1Node{
		stores:  make(map[uint64]*SchedulePlanTreeLevel2Node),
		cluster: cluster,
	}
}

func (node *SchedulePlanTreeLevel1Node) AnalysisResult(storeID uint64) *diagnosis.MatrixStoreDiagnosisResult {
	return nil
}

func (node *SchedulePlanTreeLevel1Node) putPlan(plan Plan) {
	source := plan.GetSourceStore()
	if _, ok := node.stores[source]; !ok {
		node.stores[source] = NewSchedulePlanTreeLevel2Node(source, node.cluster)
	}
	node2 := node.stores[source]
	node2.putPlan(plan)
}

type SchedulePlanTreeLevel2Node struct {
	storeID    uint64
	stores     map[uint64]*SchedulePlanTreeLevel3Node
	usedStores map[uint64]bool
	self       *SchedulePlanTreeLevel3Node
	regions    *SchedulePlanTreeLevel3Node
}

func NewSchedulePlanTreeLevel2Node(storeID uint64, cluster Cluster) *SchedulePlanTreeLevel2Node {
	stores := cluster.GetStores()
	usedStores := make(map[uint64]bool)
	for _, store := range stores {
		usedStores[store.GetID()] = false
	}
	return &SchedulePlanTreeLevel2Node{
		stores:     make(map[uint64]*SchedulePlanTreeLevel3Node),
		usedStores: usedStores,
	}
}

func (node *SchedulePlanTreeLevel2Node) AnalysisResult() (result *diagnosis.MatrixStoreDiagnosisResult) {
	result = diagnosis.NewMatrixStoreDiagnosisResult(node.storeID)
	if node.self != nil {
		result.Append(node.regions.AnalysisAll())
	} else if len(node.stores) > 0 {
		// todo: merge reigons and store
		for store, used := range node.usedStores {
			if used {
				node3 := node.stores[store]
				result.Append(node3.AnalysisResult(store))
			} else {
				result.Append(diagnosis.NewEmptyMatrixTargetDiagnosisResult(store))
			}
		}
	} else {
		result.Append(node.regions.AnalysisAll())
	}
	return
}

func (node *SchedulePlanTreeLevel2Node) putPlan(plan Plan) {
	switch plan.GetStep() {
	case 0:
		node.self = NewSchedulePlanTreeLevel3Node(0)
		node.self.Add(plan.GetReason(), 0)
	case 1:
		node.regions = NewSchedulePlanTreeLevel3Node(1)
		node.regions.Add(plan.GetReason(), plan.GetRegion())
	case 2:
		targetID := plan.GetTargetStore()
		node.usedStores[targetID] = true
		if _, ok := node.stores[targetID]; !ok {
			node.stores[targetID] = NewSchedulePlanTreeLevel3Node(2)
		}
		node3 := node.stores[targetID]
		node3.Add(plan.GetReason(), plan.GetRegion())
	case 3:
		targetID := plan.GetTargetStore()
		node.usedStores[targetID] = true
		if _, ok := node.stores[targetID]; !ok {
			node.stores[targetID] = NewSchedulePlanTreeLevel3Node(3)
		}
		node3 := node.stores[targetID]
		node3.Add(plan.GetReason(), plan.GetRegion())
	}
}

type SchedulePlanTreeLevel3Node struct {
	step    diagnosis.ScheduleStep
	reasons map[string]*SchedulePlanTreeLevel4Node
	count   int
}

func NewSchedulePlanTreeLevel3Node(step diagnosis.ScheduleStep) *SchedulePlanTreeLevel3Node {
	return &SchedulePlanTreeLevel3Node{
		reasons: make(map[string]*SchedulePlanTreeLevel4Node),
		step:    step,
	}
}

func (r *SchedulePlanTreeLevel3Node) AnalysisResult(storeID uint64) *diagnosis.MatrixTargetDiagnosisResult {
	result := diagnosis.NewMatrixTargetDiagnosisResult(storeID)
	for _, node4 := range r.reasons {
		result.Append(node4.build(r.count, r.step))
	}
	return result
}

func (r *SchedulePlanTreeLevel3Node) AnalysisAll() *diagnosis.MatrixTargetDiagnosisResult {
	result := diagnosis.NewAllMatrixTargetDiagnosisResult()
	for _, node4 := range r.reasons {
		result.Append(node4.build(r.count, r.step))
	}
	return result
}

func (r *SchedulePlanTreeLevel3Node) Add(reason string, id uint64) {
	if _, ok := r.reasons[reason]; !ok {
		r.reasons[reason] = NewSchedulePlanTreeLevel4Node(reason)
	}
	node := r.reasons[reason]
	node.put(id)
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

func NewSchedulePlanTreeLevel4Node(reason string) *SchedulePlanTreeLevel4Node {
	return &SchedulePlanTreeLevel4Node{
		Reason:  reason,
		Samples: make(map[uint64]struct{}),
		Count:   0,
	}
}

func (node *SchedulePlanTreeLevel4Node) put(sample uint64) {
	if len(node.Samples) < MaxSampleNum {
		node.Samples[sample] = struct{}{}
	}
	node.Count++
}

func (node *SchedulePlanTreeLevel4Node) build(sum int, step diagnosis.ScheduleStep) *diagnosis.ReasonMetrics {
	reasonMetrics := &diagnosis.ReasonMetrics{}
	reasonMetrics.Reason = node.Reason
	samples := make(diagnosis.SampleObjects, 0, len(node.Samples))
	for sample, _ := range node.Samples {
		samples = append(samples, sample)
	}
	reasonMetrics.SampleObject = samples.Object(step)
	reasonMetrics.Ratio = fmt.Sprintf("%.1f", float64(node.Count)/float64(sum))
	return reasonMetrics
}

type PeriodicalPlanAnalyzer struct {
	name    string
	storeID uint64
	// schedulable is true when scheduler can create operator for specific store
	schedulable   bool
	stepRecorders []SchedulePlanTreeLevel3Node
	maxStep       diagnosis.ScheduleStep
}

func NewPeriodicalPlanAnalyzer(name string, storeID uint64) *PeriodicalPlanAnalyzer {
	return &PeriodicalPlanAnalyzer{
		name:          name,
		storeID:       storeID,
		stepRecorders: make([]SchedulePlanTreeLevel3Node, 0),
	}
}

func (a *PeriodicalPlanAnalyzer) PutPlan(plan Plan) {
	if plan.GetSourceStore() != a.storeID {
		return
	}
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

func (a *PeriodicalPlanAnalyzer) AnalysisResult() *diagnosis.StepDiagnosisResult {
	scope := a.name
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
