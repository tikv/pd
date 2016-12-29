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

// GetMaxReplicaScore returns the max replica score of a store.
func (r *Replication) GetMaxReplicaScore() int {
	return len(r.cfg.LocationLabels)
}

// GetReplicaScore returns the replica score of the store relative to the candidates.
func (r *Replication) GetReplicaScore(candidates []*storeInfo, store *storeInfo) int {
	for i, key := range r.cfg.LocationLabels {
		value := store.labelValue(key)
		if len(value) == 0 {
			// If the store doesn't have this label, we assume
			// it has the same value with all candidates.
			continue
		}

		// Reset candidates.
		stores := candidates
		candidates = []*storeInfo{}

		// Push stores with the same label value to candidates.
		for _, s := range stores {
			if s.GetId() == store.GetId() {
				continue
			}
			if s.labelValue(key) == value {
				candidates = append(candidates, s)
			}
		}

		// If no candidates, it means the label value is different from others.
		if len(candidates) == 0 {
			return r.GetMaxReplicaScore() - i
		}
	}
	return 0
}

// compareStoreScore compares which store is better for replication.
// Returns 0 if store A is as good as store B.
// Returns 1 if store A is better than store B.
// Returns -1 if store B is better than store A.
func compareStoreScore(storeA *storeInfo, scoreA int, storeB *storeInfo, scoreB int) int {
	if scoreA > scoreB {
		return 1
	}
	if scoreA < scoreB {
		return -1
	}
	if storeA.storageRatio() < storeB.storageRatio() {
		return 1
	}
	if storeA.storageRatio() > storeB.storageRatio() {
		return -1
	}
	return 0
}
