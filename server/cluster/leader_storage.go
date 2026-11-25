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

package cluster

import (
	"context"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
)

// leaderKV binds the metadata managers created by Start to that campaign. The
// endpoint above it routes both direct saves and transactions through this fence.
// The cluster's shared storage and its region backend remain unchanged.
type leaderKV struct {
	kv.Base
	runner kv.ConditionalTxnRunner
	member *member.Member
	lease  *election.Lease
	term   election.LeadershipTerm
}

type leaderMetadataStorage interface {
	endpoint.RuleStorage
	endpoint.AffinityStorage
	endpoint.ReplicationStatusStorage
}

func newLeaderStorage(base storage.Storage, member *member.Member, lease *election.Lease, term election.LeadershipTerm) (leaderMetadataStorage, error) {
	runner, ok := base.(kv.ConditionalTxnRunner)
	if !ok {
		return nil, errors.New("cluster metadata storage does not support conditional transactions")
	}
	return endpoint.NewStorageEndpoint(&leaderKV{
		Base: base, runner: runner, member: member, lease: lease, term: term,
	}, nil), nil
}

// RunInTxn fences the metadata transaction to the captured leadership term.
func (s *leaderKV) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	if !s.member.IsLeaseValid(s.lease) {
		return errs.ErrNotLeader
	}
	return s.runner.RunInTxnWithConditions(ctx, s.term.Comparisons(), func(txn kv.Txn) error {
		if err := f(txn); err != nil {
			return err
		}
		if !s.member.IsLeaseValid(s.lease) {
			return errs.ErrNotLeader
		}
		return nil
	})
}

// Save writes metadata through the same transaction fence.
func (s *leaderKV) Save(key, value string) error {
	return s.RunInTxn(context.Background(), func(txn kv.Txn) error {
		return txn.Save(key, value)
	})
}

// Remove deletes metadata through the same transaction fence.
func (s *leaderKV) Remove(key string) error {
	return s.RunInTxn(context.Background(), func(txn kv.Txn) error {
		return txn.Remove(key)
	})
}
