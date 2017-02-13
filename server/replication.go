// Copyright 2016 PingCAP, Inc.
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

package server

import "math"

const replicaBaseScore = 100

// Replication provides some help to do replication.
type Replication struct {
	cfg *ReplicationConfig
}

func newReplication(cfg *ReplicationConfig) *Replication {
	return &Replication{cfg: cfg}
}

// GetMaxReplicas returns the number of replicas for each region.
func (r *Replication) GetMaxReplicas() int {
	return int(r.cfg.MaxReplicas)
}

// GetDistinctScore returns the score that the other is distinct from the stores.
// A higher score means the other store is more different from the existed stores.
func (r *Replication) GetDistinctScore(stores []*storeInfo, other *storeInfo) float64 {
	score := float64(0)

	for i := range r.cfg.LocationLabels {
		keys := r.cfg.LocationLabels[0 : i+1]
		level := len(r.cfg.LocationLabels) - i - 1
		levelScore := math.Pow(replicaBaseScore, float64(level))

		for _, s := range stores {
			if s.GetId() == other.GetId() {
				continue
			}
			id1 := s.getLocationID(keys)
			if len(id1) == 0 {
				return 0
			}
			id2 := other.getLocationID(keys)
			if len(id2) == 0 {
				return 0
			}
			if id1 != id2 {
				score += levelScore
			}
		}
	}

	return score
}

// compareStoreScore compares which store is better for replication.
// Returns 0 if store A is as good as store B.
// Returns 1 if store A is better than store B.
// Returns -1 if store B is better than store A.
func compareStoreScore(storeA *storeInfo, scoreA float64, storeB *storeInfo, scoreB float64) int {
	// The store with higher score is better.
	if scoreA > scoreB {
		return 1
	}
	if scoreA < scoreB {
		return -1
	}
	// The store with lower region ratio is better.
	if storeA.regionRatio() < storeB.regionRatio() {
		return 1
	}
	if storeA.regionRatio() > storeB.regionRatio() {
		return -1
	}
	return 0
}
