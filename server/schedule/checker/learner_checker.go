// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package checker

import (
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule/operator"
	"github.com/pingcap/pd/v4/server/schedule/opt"
	"go.uber.org/zap"
)

// LearnerChecker ensures region has a learner will be promoted.
type LearnerChecker struct {
	cluster opt.Cluster
}

// NewLearnerChecker creates a learner checker.
func NewLearnerChecker(cluster opt.Cluster) *LearnerChecker {
	return &LearnerChecker{
		cluster: cluster,
	}
}

// Check verifies a region's role, creating an Operator if need.
func (l *LearnerChecker) Check(region *core.RegionInfo) *operator.Operator {
	for _, p := range region.GetLearners() {
		if region.GetPendingLearner(p.GetId()) != nil {
			continue
		}
		op, err := operator.CreatePromoteLearnerOperator("promote-learner", l.cluster, region, p)
		if err != nil {
			log.Debug("fail to create promote learner operator", zap.Error(err))
			return nil
		}
		return op
	}
	return nil
}
