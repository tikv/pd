// Copyright 2025 TiKV Project Authors.
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
	"time"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/config"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/types"
)

const (
	// nolint:unused
	affinityLabel = "affinity"
)

// AffinityChecker groups regions with affinity labels together by affinity group.
// nolint:unused
type AffinityChecker struct {
	PauseController
	cluster         sche.CheckerCluster
	affinityManager *affinity.Manager
	conf            config.CheckerConfigProvider
	startTime       time.Time // it's used to judge whether server recently start.
}

// NewAffinityChecker create an affinity checker.
func NewAffinityChecker(cluster sche.CheckerCluster, affinityManager *affinity.Manager, conf config.CheckerConfigProvider) *AffinityChecker {
	return &AffinityChecker{
		cluster:         cluster,
		affinityManager: affinityManager,
		conf:            conf,
		startTime:       time.Now(),
	}
}

// GetType return AffinityChecker's type.
// nolint:unused
func (*AffinityChecker) GetType() types.CheckerSchedulerType {
	return types.AffinityChecker
}

// Check verifies a region's replicas, creating an Operator if needed.
// nolint:unused
func (*AffinityChecker) Check(*core.RegionInfo) []*operator.Operator {
	// TODO
	return nil
}
