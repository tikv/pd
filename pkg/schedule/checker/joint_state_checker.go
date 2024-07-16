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
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
)

// JointStateChecker ensures region is in joint state will leave.
type JointStateChecker struct {
	PauseController
	cluster sche.CheckerCluster
}

// NewJointStateChecker creates a joint state checker.
func NewJointStateChecker(cluster sche.CheckerCluster) *JointStateChecker {
	return &JointStateChecker{
		cluster: cluster,
	}
}

// Check verifies a region's role, creating an Operator if need.
func (c *JointStateChecker) Check(region *core.RegionInfo) *operator.Operator {
	jointCheckCounter.Inc()
	if c.IsPaused() {
		jointCheckerPausedCounter.Inc()
		return nil
	}
	if !core.IsInJointState(region.GetPeers()...) {
		return nil
	}
	op, err := operator.CreateLeaveJointStateOperator(operator.OpDescLeaveJointState, c.cluster, region)
	if err != nil {
		jointCheckerFailedCounter.Inc()
		log.Debug("fail to create leave joint state operator", errs.ZapError(err))
		return nil
	} else if op != nil {
		jointCheckerNewOpCounter.Inc()
		if op.Len() > 1 {
			jointCheckerTransferLeaderCounter.Inc()
		}
		op.SetPriorityLevel(constant.High)
	}
	return op
}
