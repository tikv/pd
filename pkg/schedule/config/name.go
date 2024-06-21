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

type CheckerSchedulerName string

func (c CheckerSchedulerName) String() string {
	return string(c)
}

const (
	// JointStateCheckerName is the name for joint state checker.
	JointStateCheckerName CheckerSchedulerName = "joint_state_checker"
	// LearnerCheckerName is the name for learner checker.
	LearnerCheckerName CheckerSchedulerName = "learner_checker"
	// SplitCheckerName is the name for split checker.
	MergeCheckerName CheckerSchedulerName = "merge_checker"
	// ReplicaCheckerName is the name for replica checker.
	ReplicaCheckerName CheckerSchedulerName = "replica_checker"
	// RuleCheckerName is the name for rule checker.
	RuleCheckerName CheckerSchedulerName = "rule_checker"
	// SplitCheckerName is the name for split checker.
	SplitCheckerName CheckerSchedulerName = "split_checker"
)
