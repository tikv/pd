// Copyright 2026 TiKV Project Authors.
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

package endpoint

import (
	"crypto/rand"
	"encoding/hex"
	"strings"

	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// MaxAffinityTxnBatchOps reserves one revision write per data operation and
// space for shared generations. It also applies to direct label-rule patches.
const MaxAffinityTxnBatchOps = (etcdutil.MaxEtcdTxnOps - 2) / 2

// AffinityTableRevisionKey identifies a table's durable target generation.
// It lives outside the group prefix so watchers never parse it as a Group.
func AffinityTableRevisionKey(groupID string) string {
	tableID := affinityTableID(groupID)
	return strings.TrimSuffix(keypath.AffinityGroupsPrefix(), "affinity_groups/") + "affinity_revisions/" + tableID
}

// AffinityTableMetadataRevisionKey identifies metadata changes for one
// affinity table. Region-label updates for another table must not invalidate
// this table's cached balance actions or an in-flight manual plan.
func AffinityTableMetadataRevisionKey(groupID string) string {
	tableID := affinityTableID(groupID)
	return strings.TrimSuffix(keypath.AffinityGroupsPrefix(), "affinity_groups/") + "affinity_metadata_revisions/" + tableID
}

func affinityTableID(groupID string) string {
	tableID := groupID
	if strings.HasPrefix(tableID, "_tidb_pt_") {
		if index := strings.LastIndex(tableID, "_p"); index >= len("_tidb_pt_") {
			tableID = tableID[:index]
		}
	}
	return tableID
}

// AffinityPlacementRevisionKey fences placement rules, rule groups, and
// non-affinity region labels that can carry shared scheduling constraints.
func AffinityPlacementRevisionKey() string {
	return strings.TrimSuffix(keypath.AffinityGroupsPrefix(), "affinity_groups/") + "affinity_placement_revision"
}

type balanceMutationTxn struct {
	kv.Txn
	// True means this token was already written explicitly by the caller.
	revisions map[string]bool
}

func (txn *balanceMutationTxn) mark(key string) {
	if id, ok := strings.CutPrefix(key, keypath.AffinityGroupsPrefix()); ok {
		key = AffinityTableRevisionKey(id)
	} else if strings.HasPrefix(key, keypath.RegionLabelPathPrefix()) {
		ruleID := strings.TrimPrefix(key, keypath.RegionLabelPathPrefix())
		if strings.HasPrefix(ruleID, "affinity_group/") {
			groupID := strings.TrimPrefix(ruleID, "affinity_group/")
			key = AffinityTableMetadataRevisionKey(groupID)
		} else {
			key = AffinityPlacementRevisionKey()
		}
	} else if strings.HasPrefix(key, keypath.RulesPathPrefix()) ||
		strings.HasPrefix(key, keypath.RuleGroupPathPrefix()) {
		key = AffinityPlacementRevisionKey()
	} else {
		return
	}
	txn.trackRevision(key, false)
}

func (txn *balanceMutationTxn) trackRevision(key string, explicit bool) {
	if txn.revisions == nil {
		txn.revisions = make(map[string]bool)
	}
	txn.revisions[key] = explicit || txn.revisions[key]
}

// Save records the mutation and queues the original write.
func (txn *balanceMutationTxn) Save(key, value string) error {
	txn.mark(key)
	return txn.Txn.Save(key, value)
}

// Remove records the mutation and queues the original deletion.
func (txn *balanceMutationTxn) Remove(key string) error {
	txn.mark(key)
	return txn.Txn.Remove(key)
}

func (txn *balanceMutationTxn) saveExplicitRevision(key, value string) error {
	if err := txn.Txn.Save(key, value); err != nil {
		return err
	}
	txn.trackRevision(key, true)
	return nil
}

// SaveAffinityTableRevision writes and returns the exact target token for a
// table. Balance submissions use the returned value after committing their
// target changes instead of rereading a token that a later write may replace.
func SaveAffinityTableRevision(txn kv.Txn, groupID string) (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	value := hex.EncodeToString(token[:])
	key := AffinityTableRevisionKey(groupID)
	if tracked, ok := txn.(*balanceMutationTxn); ok {
		if err := tracked.saveExplicitRevision(key, value); err != nil {
			return "", err
		}
	} else if err := txn.Save(key, value); err != nil {
		return "", err
	}
	return value, nil
}

// LoadWithRevision preserves revision comparisons supported by the backend.
func (txn *balanceMutationTxn) LoadWithRevision(key string) (string, error) {
	if revision, ok := txn.Txn.(kv.RevisionTxn); ok {
		return revision.LoadWithRevision(key)
	}
	return txn.Load(key)
}

func trackBalanceMutations(f func(kv.Txn) error) func(kv.Txn) error {
	return func(txn kv.Txn) error {
		tracked := &balanceMutationTxn{Txn: txn}
		if err := f(tracked); err != nil {
			return err
		}
		// One put per token even when a batch modifies many groups/rules.
		for key, explicit := range tracked.revisions {
			if explicit {
				continue
			}
			var token [16]byte
			if _, err := rand.Read(token[:]); err != nil {
				return err
			}
			if err := txn.Save(key, hex.EncodeToString(token[:])); err != nil {
				return err
			}
		}
		return nil
	}
}
