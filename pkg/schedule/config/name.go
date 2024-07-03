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

package config

import (
	"strings"

	"github.com/pingcap/errors"
)

// TODO: rename to type
type CheckerSchedulerName string

func (n CheckerSchedulerName) String() string {
	return string(n)
}

const (
	// JointStateCheckerName is the name for joint state checker.
	JointStateCheckerName CheckerSchedulerName = "joint-state-checker"
	// LearnerCheckerName is the name for learner checker.
	LearnerCheckerName CheckerSchedulerName = "learner-checker"
	// SplitCheckerName is the name for split checker.
	MergeCheckerName CheckerSchedulerName = "merge-checker"
	// ReplicaCheckerName is the name for replica checker.
	ReplicaCheckerName CheckerSchedulerName = "replica-checker"
	// RuleCheckerName is the name for rule checker.
	RuleCheckerName CheckerSchedulerName = "rule-checker"
	// SplitCheckerName is the name for split checker.
	SplitCheckerName CheckerSchedulerName = "split-checker"

	// BalanceLeaderName is balance leader scheduler name.
	BalanceLeaderName CheckerSchedulerName = "balance-leader-scheduler"
	// BalanceRegionName is balance region scheduler name.
	BalanceRegionName CheckerSchedulerName = "balance-region-scheduler"
	// BalanceWitnessName is balance witness scheduler name.
	BalanceWitnessName CheckerSchedulerName = "balance-witness-scheduler"
	// EvictLeaderName is evict leader scheduler name.
	EvictLeaderName CheckerSchedulerName = "evict-leader-scheduler"
	// EvictSlowStoreName is evict leader scheduler name.
	EvictSlowStoreName CheckerSchedulerName = "evict-slow-store-scheduler"
	// EvictSlowTrendName is evict leader by slow trend scheduler name.
	EvictSlowTrendName CheckerSchedulerName = "evict-slow-trend-scheduler"
	// GrantLeaderName is grant leader scheduler name.
	GrantLeaderName CheckerSchedulerName = "grant-leader-scheduler"
	// GrantHotRegionName is grant hot region scheduler name.
	GrantHotRegionName CheckerSchedulerName = "grant-hot-region-scheduler"
	// HotRegionName is balance hot region scheduler name.
	HotRegionName CheckerSchedulerName = "balance-hot-region-scheduler"
	// RandomMergeName is random merge scheduler name.
	RandomMergeName CheckerSchedulerName = "random-merge-scheduler"
	// ScatterRangeName is scatter range scheduler name
	ScatterRangeName CheckerSchedulerName = "scatter-range-scheduler"
	// ShuffleHotRegionName is shuffle hot region scheduler name.
	ShuffleHotRegionName CheckerSchedulerName = "shuffle-hot-region-scheduler"
	// ShuffleLeaderName is shuffle leader scheduler name.
	ShuffleLeaderName CheckerSchedulerName = "shuffle-leader-scheduler"
	// ShuffleRegionName is shuffle region scheduler name.
	ShuffleRegionName CheckerSchedulerName = "shuffle-region-scheduler"
	// SplitBucketName is the split bucket name.
	SplitBucketName CheckerSchedulerName = "split-bucket-scheduler"
	// TransferWitnessLeaderName is transfer witness leader scheduler name.
	TransferWitnessLeaderName CheckerSchedulerName = "transfer-witness-leader-scheduler"
	// LabelName is label scheduler name.
	LabelName CheckerSchedulerName = "label-scheduler"
)

var string2SchedulerName = map[string]CheckerSchedulerName{
	"balance-leader-scheduler":          BalanceLeaderName,
	"balance-region-scheduler":          BalanceRegionName,
	"balance-witness-scheduler":         BalanceWitnessName,
	"evict-leader-scheduler":            EvictLeaderName,
	"evict-slow-store-scheduler":        EvictSlowStoreName,
	"evict-slow-trend-scheduler":        EvictSlowTrendName,
	"grant-leader-scheduler":            GrantLeaderName,
	"grant-hot-region-scheduler":        GrantHotRegionName,
	"balance-hot-region-scheduler":      HotRegionName,
	"random-merge-scheduler":            RandomMergeName,
	"scatter-range-scheduler":           ScatterRangeName,
	"shuffle-hot-region-scheduler":      ShuffleHotRegionName,
	"shuffle-leader-scheduler":          ShuffleLeaderName,
	"shuffle-region-scheduler":          ShuffleRegionName,
	"split-bucket-scheduler":            SplitBucketName,
	"transfer-witness-leader-scheduler": TransferWitnessLeaderName,
	"label-scheduler":                   LabelName,
}

func CheckSchedulerType(str string) error {
	_, ok := string2SchedulerName[str]
	if !ok && !strings.HasPrefix(str, ScatterRangeName.String()) {
		return errors.Errorf("unknown scheduler name: %s", str)
	}
	return nil
}

func ConvertSchedulerStr2Name(str string) (CheckerSchedulerName, error) {
	name, ok := string2SchedulerName[str]
	if ok {
		return name, nil
	}
	return "", errors.Errorf("unknown scheduler name: %s", str)
}
