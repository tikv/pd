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

package labeler

import (
	"github.com/tikv/pd/pkg/storage/kv"
)

// Plan is an execution plan for the RegionLabeler.
// It is used to perform transactional commits together with other modules.
type Plan struct {
	labeler        *RegionLabeler
	err            error
	commitOps      []func(txn kv.Txn) error
	applyLockedOps []func()
}

// NewPlan creates a new execution plan.
func (l *RegionLabeler) NewPlan() *Plan {
	return &Plan{
		labeler:        l,
		err:            nil,
		commitOps:      make([]func(txn kv.Txn) error, 0),
		applyLockedOps: make([]func(), 0),
	}
}

// SetLabelRule validates the rule and adds SetLabelRule to the transaction.
// Note that it only takes effect after CommitOps and Set are executed.
func (p *Plan) SetLabelRule(rule *LabelRule) error {
	if p.err != nil {
		return p.err
	}
	if err := rule.checkAndAdjust(); err != nil {
		p.err = err
		return err
	}
	p.commitOps = append(p.commitOps, func(txn kv.Txn) error {
		return p.labeler.storage.SaveRegionRule(txn, rule.ID, rule)
	})
	p.applyLockedOps = append(p.applyLockedOps, func() {
		p.labeler.labelRules[rule.ID] = rule
	})
	return nil
}

// DeleteLabelRule validates the rule and adds DeleteLabelRule to the transaction.
// Note that it only takes effect after CommitOps and Set are executed.
func (p *Plan) DeleteLabelRule(id string) error {
	if p.err != nil {
		return p.err
	}
	p.commitOps = append(p.commitOps, func(txn kv.Txn) error {
		return p.labeler.storage.DeleteRegionRule(txn, id)
	})
	p.applyLockedOps = append(p.applyLockedOps, func() {
		if _, ok := p.labeler.labelRules[id]; !ok {
			return
		}
		delete(p.labeler.labelRules, id)
	})
	return nil
}

// CommitOps retrieves all operations in the transaction.
func (p *Plan) CommitOps() []func(txn kv.Txn) error {
	if p.err != nil {
		return nil
	}
	return p.commitOps
}

// Apply applies the changes to memory.
func (p *Plan) Apply() {
	if p.err != nil {
		return
	}

	p.labeler.Lock()
	defer p.labeler.Unlock()
	for _, op := range p.applyLockedOps {
		op()
	}
	p.labeler.BuildRangeListLocked()
}
