package schedule

type Plan interface {
	GetSourceStore() uint64
	GetRegion() uint64
	GetTargetStore() uint64
	GetStep() uint64
	GetReason() string
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
