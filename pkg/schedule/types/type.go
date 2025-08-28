// Copyright 2024 TiKV Project Authors.
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

package types

// CheckerSchedulerType is the type of checker/scheduler.
type CheckerSchedulerType string

// String implements fmt.Stringer.
func (t CheckerSchedulerType) String() string {
	return string(t)
}

const (
	// JointStateChecker is the name for joint state checker.
	JointStateChecker CheckerSchedulerType = "joint-state-checker"
	// LearnerChecker is the name for learner checker.
	LearnerChecker CheckerSchedulerType = "learner-checker"
	// MergeChecker is the name for split checker.
	MergeChecker CheckerSchedulerType = "merge-checker"
	// ReplicaChecker is the name for replica checker.
	ReplicaChecker CheckerSchedulerType = "replica-checker"
	// RuleChecker is the name for rule checker.
	RuleChecker CheckerSchedulerType = "rule-checker"
	// SplitChecker is the name for split checker.
	SplitChecker CheckerSchedulerType = "split-checker"

	// BalanceLeaderScheduler is balance leader scheduler name.
	BalanceLeaderScheduler CheckerSchedulerType = "balance-leader-scheduler"
	// BalanceRegionScheduler is balance region scheduler name.
	BalanceRegionScheduler CheckerSchedulerType = "balance-region-scheduler"
	// BalanceWitnessScheduler is balance witness scheduler name.
	BalanceWitnessScheduler CheckerSchedulerType = "balance-witness-scheduler"
	// EvictLeaderScheduler is evict leader scheduler name.
	EvictLeaderScheduler CheckerSchedulerType = "evict-leader-scheduler"
	// EvictSlowStoreScheduler is evict leader scheduler name.
	EvictSlowStoreScheduler CheckerSchedulerType = "evict-slow-store-scheduler"
	// EvictSlowTrendScheduler is evict leader by slow trend scheduler name.
	EvictSlowTrendScheduler CheckerSchedulerType = "evict-slow-trend-scheduler"
	// GrantLeaderScheduler is grant leader scheduler name.
	GrantLeaderScheduler CheckerSchedulerType = "grant-leader-scheduler"
	// GrantHotRegionScheduler is grant hot region scheduler name.
	GrantHotRegionScheduler CheckerSchedulerType = "grant-hot-region-scheduler"
	// BalanceHotRegionScheduler is balance hot region scheduler name.
	BalanceHotRegionScheduler CheckerSchedulerType = "balance-hot-region-scheduler"
	// RandomMergeScheduler is random merge scheduler name.
	RandomMergeScheduler CheckerSchedulerType = "random-merge-scheduler"
	// ScatterRangeScheduler is scatter range scheduler name.
	ScatterRangeScheduler CheckerSchedulerType = "scatter-range-scheduler"
	// ShuffleHotRegionScheduler is shuffle hot region scheduler name.
	ShuffleHotRegionScheduler CheckerSchedulerType = "shuffle-hot-region-scheduler"
	// ShuffleLeaderScheduler is shuffle leader scheduler name.
	ShuffleLeaderScheduler CheckerSchedulerType = "shuffle-leader-scheduler"
	// ShuffleRegionScheduler is shuffle region scheduler name.
	ShuffleRegionScheduler CheckerSchedulerType = "shuffle-region-scheduler"
	// SplitBucketScheduler is the split bucket name.
	SplitBucketScheduler CheckerSchedulerType = "split-bucket-scheduler"
	// TransferWitnessLeaderScheduler is transfer witness leader scheduler name.
	TransferWitnessLeaderScheduler CheckerSchedulerType = "transfer-witness-leader-scheduler"
	// LabelScheduler is label scheduler name.
	LabelScheduler CheckerSchedulerType = "label-scheduler"
	// BalanceRangeScheduler is balance key range scheduler name.
	BalanceRangeScheduler CheckerSchedulerType = "balance-range-scheduler"
)

// TODO: SchedulerTypeCompatibleMap and ConvertOldStrToType should be removed after
// fixing this issue(https://github.com/tikv/pd/issues/8474).
var (
	// SchedulerTypeCompatibleMap exists for compatibility.
	//
	//	It is used for `SchedulerConfig` in the `PersistOptions` and `PersistConfig`.
	//	These two structs are persisted in the storage, so we need to keep the compatibility.
	SchedulerTypeCompatibleMap = map[CheckerSchedulerType]string{
		BalanceLeaderScheduler:         "balance-leader",
		BalanceRegionScheduler:         "balance-region",
		BalanceWitnessScheduler:        "balance-witness",
		EvictLeaderScheduler:           "evict-leader",
		EvictSlowStoreScheduler:        "evict-slow-store",
		EvictSlowTrendScheduler:        "evict-slow-trend",
		GrantLeaderScheduler:           "grant-leader",
		GrantHotRegionScheduler:        "grant-hot-region",
		BalanceHotRegionScheduler:      "hot-region",
		RandomMergeScheduler:           "random-merge",
		ScatterRangeScheduler:          "scatter-range",
		ShuffleHotRegionScheduler:      "shuffle-hot-region",
		ShuffleLeaderScheduler:         "shuffle-leader",
		ShuffleRegionScheduler:         "shuffle-region",
		SplitBucketScheduler:           "split-bucket",
		TransferWitnessLeaderScheduler: "transfer-witness-leader",
		LabelScheduler:                 "label",
		BalanceRangeScheduler:          "balance-range",
	}

	// ConvertOldStrToType exists for compatibility.
	//
	//	It is used to convert the old scheduler type to `CheckerSchedulerType`.
	ConvertOldStrToType = map[string]CheckerSchedulerType{
		"balance-leader":          BalanceLeaderScheduler,
		"balance-region":          BalanceRegionScheduler,
		"balance-witness":         BalanceWitnessScheduler,
		"evict-leader":            EvictLeaderScheduler,
		"evict-slow-store":        EvictSlowStoreScheduler,
		"evict-slow-trend":        EvictSlowTrendScheduler,
		"grant-leader":            GrantLeaderScheduler,
		"grant-hot-region":        GrantHotRegionScheduler,
		"hot-region":              BalanceHotRegionScheduler,
		"random-merge":            RandomMergeScheduler,
		"scatter-range":           ScatterRangeScheduler,
		"shuffle-hot-region":      ShuffleHotRegionScheduler,
		"shuffle-leader":          ShuffleLeaderScheduler,
		"shuffle-region":          ShuffleRegionScheduler,
		"split-bucket":            SplitBucketScheduler,
		"transfer-witness-leader": TransferWitnessLeaderScheduler,
		"label":                   LabelScheduler,
		"balance-range":           BalanceRangeScheduler,
	}

	// StringToSchedulerType is a map to convert the scheduler string to the CheckerSchedulerType.
	StringToSchedulerType = map[string]CheckerSchedulerType{
		"balance-leader-scheduler":     BalanceLeaderScheduler,
		"balance-region-scheduler":     BalanceRegionScheduler,
		"balance-witness-scheduler":    BalanceWitnessScheduler,
		"evict-leader-scheduler":       EvictLeaderScheduler,
		"evict-slow-store-scheduler":   EvictSlowStoreScheduler,
		"evict-slow-trend-scheduler":   EvictSlowTrendScheduler,
		"grant-leader-scheduler":       GrantLeaderScheduler,
		"grant-hot-region-scheduler":   GrantHotRegionScheduler,
		"balance-hot-region-scheduler": BalanceHotRegionScheduler,
		"random-merge-scheduler":       RandomMergeScheduler,
		"scatter-range-scheduler":      ScatterRangeScheduler,
		// TODO: remove `scatter-range` after remove `NewScatterRangeSchedulerCommand` from pd-ctl
		"scatter-range":                     ScatterRangeScheduler,
		"shuffle-hot-region-scheduler":      ShuffleHotRegionScheduler,
		"shuffle-leader-scheduler":          ShuffleLeaderScheduler,
		"shuffle-region-scheduler":          ShuffleRegionScheduler,
		"split-bucket-scheduler":            SplitBucketScheduler,
		"transfer-witness-leader-scheduler": TransferWitnessLeaderScheduler,
		"label-scheduler":                   LabelScheduler,
		"balance-range-scheduler":           BalanceRangeScheduler,
	}

	// DefaultSchedulers is the default scheduler types.
	// If you want to add a new scheduler, please
	//   1. add it to the list
	//   2. change the `schedulerConfig` interface to the `baseDefaultSchedulerConfig`
	// 		structure in related `xxxxSchedulerConfig`
	//   3. remove `syncutil.RWMutex` from related `xxxxSchedulerConfig`
	DefaultSchedulers = []CheckerSchedulerType{
		BalanceLeaderScheduler,
		BalanceRegionScheduler,
		BalanceHotRegionScheduler,
		EvictSlowStoreScheduler,
	}
)
