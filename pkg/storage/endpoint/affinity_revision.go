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
	"github.com/tikv/pd/pkg/utils/keypath"
)

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

// AffinityMetadataRevisionKey covers placement rules and Region labels.
func AffinityMetadataRevisionKey() string {
	return strings.TrimSuffix(keypath.AffinityGroupsPrefix(), "affinity_groups/") + "affinity_metadata_revision"
}

type balanceMutationTxn struct {
	kv.Txn
	changed map[string]struct{}
}

func (txn *balanceMutationTxn) mark(key string) {
	if id, ok := strings.CutPrefix(key, keypath.AffinityGroupsPrefix()); ok {
		txn.changed[AffinityTableRevisionKey(id)] = struct{}{}
	} else if strings.HasPrefix(key, keypath.RegionLabelPathPrefix()) {
		ruleID := strings.TrimPrefix(key, keypath.RegionLabelPathPrefix())
		if strings.HasPrefix(ruleID, "affinity_group/") {
			groupID := strings.TrimPrefix(ruleID, "affinity_group/")
			txn.changed[AffinityTableMetadataRevisionKey(groupID)] = struct{}{}
			// Preserve the cluster-wide metadata generation for consumers that
			// reconcile the complete label snapshot. Balance submission uses the
			// table-scoped token above, so this does not abort unrelated tables.
			txn.changed[AffinityMetadataRevisionKey()] = struct{}{}
		} else {
			txn.changed[AffinityMetadataRevisionKey()] = struct{}{}
		}
	} else if strings.HasPrefix(key, keypath.RulesPathPrefix()) ||
		strings.HasPrefix(key, keypath.RuleGroupPathPrefix()) {
		txn.changed[AffinityMetadataRevisionKey()] = struct{}{}
	}
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

// LoadWithRevision preserves revision comparisons supported by the backend.
func (txn *balanceMutationTxn) LoadWithRevision(key string) (string, error) {
	if revision, ok := txn.Txn.(kv.RevisionTxn); ok {
		return revision.LoadWithRevision(key)
	}
	return txn.Load(key)
}

func trackBalanceMutations(f func(kv.Txn) error) func(kv.Txn) error {
	return func(txn kv.Txn) error {
		tracked := &balanceMutationTxn{Txn: txn, changed: make(map[string]struct{})}
		if err := f(tracked); err != nil {
			return err
		}
		// One put per token even when a batch modifies many groups/rules.
		for key := range tracked.changed {
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
