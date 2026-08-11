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

package server

import (
	"context"
	"encoding/json"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

var (
	// ErrMicroserviceMetadataCleanupRejected indicates that the persisted
	// keyspace-group state cannot be safely cleaned up.
	ErrMicroserviceMetadataCleanupRejected = errors.New("microservice metadata cleanup rejected")
	// ErrMicroserviceMetadataCleanupUnavailable indicates that the request could
	// not be completed under the current normal-PD leadership term.
	ErrMicroserviceMetadataCleanupUnavailable = errors.New("microservice metadata cleanup unavailable")
)

type microserviceMetadataCleanupTerm struct {
	leaderKey   string
	leaderValue string
	leaseID     clientv3.LeaseID
}

func rejectMicroserviceMetadataCleanup(format string, args ...any) error {
	return errors.Wrapf(ErrMicroserviceMetadataCleanupRejected, format, args...)
}

func unavailableMicroserviceMetadataCleanup(format string, args ...any) error {
	return errors.Wrapf(ErrMicroserviceMetadataCleanupUnavailable, format, args...)
}

// CleanupMicroserviceMetadata clears the persisted Members field of the default
// TSO keyspace group in normal PD mode and reports whether it changed. A nil
// error is fenced by one exact normal-PD leadership term and represents a
// linearizable check that no non-default keyspace group existed at that point.
func (s *Server) CleanupMicroserviceMetadata(ctx context.Context) (bool, error) {
	term, err := s.captureMicroserviceMetadataCleanupTerm()
	if err != nil {
		return false, err
	}
	if s.IsKeyspaceGroupEnabled() {
		return false, rejectMicroserviceMetadataCleanup("pd is already running in microservice mode")
	}

	groupKey := keypath.KeyspaceGroupIDPath(constant.DefaultKeyspaceGroupID)
	groupPrefixEnd := clientv3.GetPrefixRangeEnd(keypath.KeyspaceGroupIDPrefix())
	operationCtx, cancel := context.WithTimeout(ctx, etcdutil.DefaultRequestTimeout)
	defer cancel()
	resp, err := s.client.Get(
		operationCtx,
		groupKey,
		clientv3.WithRange(groupPrefixEnd),
		clientv3.WithLimit(2),
	)
	if err != nil {
		return false, unavailableMicroserviceMetadataCleanup(
			"failed to read keyspace-group metadata: %v", err)
	}

	var (
		group         *endpoint.KeyspaceGroup
		groupRevision clientv3.Cmp
		changed       bool
	)
	switch {
	case len(resp.Kvs) == 0:
		groupRevision = clientv3.Compare(clientv3.CreateRevision(groupKey), "=", 0)
	case string(resp.Kvs[0].Key) != groupKey || len(resp.Kvs) > 1:
		return false, rejectMicroserviceMetadataCleanup("found a non-default TSO keyspace group")
	default:
		group = &endpoint.KeyspaceGroup{}
		if err := json.Unmarshal(resp.Kvs[0].Value, group); err != nil {
			return false, errs.ErrJSONUnmarshal.Wrap(err).GenWithStackByCause()
		}
		if group.ID != constant.DefaultKeyspaceGroupID {
			return false, rejectMicroserviceMetadataCleanup(
				"found TSO keyspace group %d at the default group path", group.ID)
		}
		if group.IsSplitting() {
			return false, rejectMicroserviceMetadataCleanup("default TSO keyspace group is splitting")
		}
		if group.IsMerging() {
			return false, rejectMicroserviceMetadataCleanup("default TSO keyspace group is merging")
		}
		groupRevision = clientv3.Compare(
			clientv3.ModRevision(groupKey),
			"=",
			resp.Kvs[0].ModRevision,
		)
		changed = len(group.Members) > 0
	}

	nonDefaultGroupStart := keypath.KeyspaceGroupIDPath(constant.DefaultKeyspaceGroupID + 1)
	comparisons := []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(term.leaderKey), "=", term.leaderValue),
		clientv3.Compare(clientv3.LeaseValue(term.leaderKey), "=", term.leaseID),
		groupRevision,
		clientv3.Compare(
			clientv3.CreateRevision(nonDefaultGroupStart).WithRange(groupPrefixEnd),
			"=",
			0,
		),
	}
	operation := clientv3.OpGet(groupKey)
	if changed {
		group.Members = nil
		value, err := json.Marshal(group)
		if err != nil {
			return false, errs.ErrJSONMarshal.Wrap(err).GenWithStackByCause()
		}
		operation = clientv3.OpPut(groupKey, string(value))
	}

	failpoint.InjectCall("beforeCleanupMicroserviceMetadataCommit")
	txnResp, err := kv.NewSlowLogTxnWithContext(operationCtx, s.client).
		If(comparisons...).
		Then(operation).
		Commit()
	if err != nil {
		return false, unavailableMicroserviceMetadataCleanup(
			"failed to commit microservice metadata cleanup: %v", err)
	}
	if txnResp.Succeeded {
		return changed, nil
	}
	return false, unavailableMicroserviceMetadataCleanup(
		"leadership or keyspace-group metadata changed during cleanup")
}

func (s *Server) captureMicroserviceMetadataCleanupTerm() (microserviceMetadataCleanupTerm, error) {
	if s.client == nil || s.member == nil || !s.member.IsServing() {
		return microserviceMetadataCleanupTerm{}, unavailableMicroserviceMetadataCleanup(
			"normal PD leader is not serving")
	}
	leadership := s.member.GetLeadership()
	if leadership == nil || leadership.GetLease() == nil {
		return microserviceMetadataCleanupTerm{}, unavailableMicroserviceMetadataCleanup(
			"normal PD leadership is not initialized")
	}
	term := microserviceMetadataCleanupTerm{
		leaderKey:   leadership.GetLeaderKey(),
		leaderValue: leadership.GetLeaderValue(),
		leaseID:     leadership.GetLease().GetID(),
	}
	if term.leaderKey == "" || term.leaderValue == "" || term.leaseID == 0 {
		return microserviceMetadataCleanupTerm{}, unavailableMicroserviceMetadataCleanup(
			"normal PD leadership term is incomplete")
	}
	return term, nil
}
