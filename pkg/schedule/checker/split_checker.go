// Copyright 2021 TiKV Project Authors.
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
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
)

// keyspaceCheckerWrapper wraps the cluster to provide keyspace existence checking.
type keyspaceCheckerWrapper struct {
	cluster sche.CheckerCluster
}

// KeyspaceExists checks if a keyspace exists by probing the cluster interface.
func (w *keyspaceCheckerWrapper) KeyspaceExists(id uint32) bool {
	// Try to get the keyspace manager from the cluster
	type keyspaceManagerGetter interface {
		GetKeyspaceManager() interface{ KeyspaceExists(uint32) bool }
	}
	if kg, ok := w.cluster.(keyspaceManagerGetter); ok {
		if km := kg.GetKeyspaceManager(); km != nil {
			return km.KeyspaceExists(id)
		}
	}
	// If we can't get the keyspace manager, assume the keyspace exists
	// to maintain backward compatibility
	return true
}

// GetKeyspaceIDInRange returns one existing keyspace ID in (start, end).
func (w *keyspaceCheckerWrapper) GetKeyspaceIDInRange(start, end uint32) (uint32, bool) {
	if start >= end {
		return 0, false
	}
	type keyspaceManagerGetter interface {
		GetKeyspaceManager() interface{ KeyspaceExists(uint32) bool }
	}
	if kg, ok := w.cluster.(keyspaceManagerGetter); ok {
		if km := kg.GetKeyspaceManager(); km != nil {
			if getter, ok := any(km).(interface {
				GetKeyspaceIDInRange(uint32, uint32) (uint32, bool)
			}); ok {
				return getter.GetKeyspaceIDInRange(start, end)
			}
			for id := start + 1; id < end; id++ {
				if km.KeyspaceExists(id) {
					return id, true
				}
			}
			return 0, false
		}
	}
	// If we can't get the keyspace manager, assume keyspaces exist to maintain
	// backward compatibility.
	if start+1 < end {
		return start + 1, true
	}
	return 0, false
}

// SplitChecker splits regions when the key range spans across rule/label boundary.
type SplitChecker struct {
	PauseController
	cluster     sche.CheckerCluster
	ruleManager *placement.RuleManager
	labeler     *labeler.RegionLabeler
}

// NewSplitChecker creates a new SplitChecker.
func NewSplitChecker(cluster sche.CheckerCluster, ruleManager *placement.RuleManager, labeler *labeler.RegionLabeler) *SplitChecker {
	return &SplitChecker{
		cluster:     cluster,
		ruleManager: ruleManager,
		labeler:     labeler,
	}
}

// GetType returns the checker type.
func (*SplitChecker) GetType() string {
	return "split-checker"
}

// Check checks whether the region need to split and returns Operator to fix.
func (c *SplitChecker) Check(region *core.RegionInfo) *operator.Operator {
	splitCheckerCounter.Inc()

	if c.IsPaused() {
		splitCheckerPausedCounter.Inc()
		return nil
	}

	start, end := region.GetStartKey(), region.GetEndKey()

	// We may consider to merge labeler split keys and rule split keys together
	// before creating operator. It can help to reduce operator count. However,
	// handle them separately helps to understand the reason for the split.
	desc := "labeler-split-region"
	keys := c.labeler.GetSplitKeys(start, end)

	if len(keys) == 0 && c.cluster.GetCheckerConfig().IsPlacementRulesEnabled() {
		desc = "rule-split-region"
		keys = c.ruleManager.GetSplitKeys(start, end)
	}

	if len(keys) == 0 {
		// First check if the region spans multiple keyspaces
		// This ensures one region corresponds to one keyspace
		checker := &keyspaceCheckerWrapper{cluster: c.cluster}
		if keyspace.RegionSpansMultipleKeyspaces(start, end, checker) {
			desc = "keyspace-split-region"
			keys = keyspace.GetKeyspaceSplitKeys(start, end, checker)
			if len(keys) > 0 {
				op, err := operator.CreateSplitRegionOperator(desc, region, operator.OpSplit, pdpb.CheckPolicy_USEKEY, keys)
				if err != nil {
					log.Debug("create keyspace split region operator failed", errs.ZapError(err))
					return nil
				}
				return op
			}
		}
	}

	if len(keys) == 0 {
		return nil
	}

	op, err := operator.CreateSplitRegionOperator(desc, region, operator.OpSplit, pdpb.CheckPolicy_USEKEY, keys)
	if err != nil {
		log.Debug("create split region operator failed", errs.ZapError(err))
		return nil
	}
	return op
}
