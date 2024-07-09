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

type CheckerSchedulerType string

func (n CheckerSchedulerType) String() string {
	return string(n)
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
)
