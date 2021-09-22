// Copyright 2020 TiKV Project Authors.
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

package checker

import (
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
)

// JointStateChecker ensures region is in joint state will leave.
type JointStateChecker struct {
	PauseController
	cluster opt.Cluster
}

// NewJointStateChecker creates a joint state checker.
func NewJointStateChecker(cluster opt.Cluster) *JointStateChecker {
	return &JointStateChecker{
		cluster: cluster,
	}
}

// Check verifies a region's role, creating an Operator if need.
func (c *JointStateChecker) Check(region *core.RegionInfo) *operator.Operator {
	checkerCounter.WithLabelValues("joint_state_checker", "check").Inc()
	if c.IsPaused() {
		checkerCounter.WithLabelValues("joint_state_checker", "paused").Inc()
		return nil
	}
	if !core.IsInJointState(region.GetPeers()...) {
		return nil
	}
	op, err := operator.CreateLeaveJointStateOperator("leave-joint-state", c.cluster, region)
	if err != nil {
		checkerCounter.WithLabelValues("joint_state_checker", "create-operator-fail").Inc()
		log.Debug("fail to create leave joint state operator", errs.ZapError(err))
		return nil
	} else if op != nil {
		checkerCounter.WithLabelValues("joint_state_checker", "new-operator").Inc()
		if op.Len() > 1 {
			checkerCounter.WithLabelValues("joint_state_checker", "transfer-leader").Inc()
		}
		op.SetPriorityLevel(core.HighPriority)
	}
	return op
}
