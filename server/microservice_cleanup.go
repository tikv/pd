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
	goerrors "errors"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
)

const microserviceMetadataCleanupRetryInterval = 5 * time.Second

var errMicroserviceMetadataCleanupRejected = errors.New("microservice metadata cleanup rejected")

type microserviceMetadataCleanupTerm struct {
	leaderKey   string
	leaderValue string
	leaseID     clientv3.LeaseID
}

func rejectMicroserviceMetadataCleanup(format string, args ...any) error {
	return errors.Wrapf(errMicroserviceMetadataCleanupRejected, format, args...)
}

func (s *Server) scheduleMicroserviceMetadataCleanup(ctx context.Context) {
	if s.IsKeyspaceGroupEnabled() {
		return
	}
	term, ok := s.captureMicroserviceMetadataCleanupTerm()
	if !ok {
		return
	}
	s.serverLoopWg.Add(1)
	go func() {
		defer logutil.LogPanic()
		defer s.serverLoopWg.Done()
		for {
			if s.member != nil && !s.member.IsServing() {
				return
			}
			err := s.cleanupMicroserviceMetadataInPDMode(ctx, term)
			if err == nil {
				return
			}
			if goerrors.Is(err, errMicroserviceMetadataCleanupRejected) {
				log.Warn("cannot safely clean up microservice metadata in PD mode",
					errs.ZapError(err))
				return
			}
			log.Warn("failed to clean up microservice metadata in PD mode, retry later",
				errs.ZapError(err))
			timer := time.NewTimer(microserviceMetadataCleanupRetryInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
}

func (s *Server) captureMicroserviceMetadataCleanupTerm() (microserviceMetadataCleanupTerm, bool) {
	if s.member == nil {
		return microserviceMetadataCleanupTerm{}, false
	}
	leadership := s.member.GetLeadership()
	if leadership == nil {
		return microserviceMetadataCleanupTerm{}, false
	}
	lease := leadership.GetLease()
	if lease == nil {
		return microserviceMetadataCleanupTerm{}, false
	}
	term := microserviceMetadataCleanupTerm{
		leaderKey:   leadership.GetLeaderKey(),
		leaderValue: leadership.GetLeaderValue(),
		leaseID:     lease.GetID(),
	}
	return term, term.leaderKey != "" && term.leaderValue != "" && term.leaseID != 0
}

func (s *Server) cleanupMicroserviceMetadataInPDMode(
	ctx context.Context,
	term microserviceMetadataCleanupTerm,
) error {
	if s.IsKeyspaceGroupEnabled() {
		return nil
	}

	// The persisted keyspace group contains the stale TSO member addresses that
	// block a later switch back to API service mode. Keyspace assignment markers
	// and the rest of the keyspace group metadata are intentionally left untouched.
	needsCleanup, err := s.validateMicroserviceMetadataCleanup()
	if err != nil || !needsCleanup {
		return err
	}
	cleared, err := s.clearDefaultTSOKeyspaceGroupMembers(ctx, term)
	if err != nil {
		return err
	}
	if cleared {
		log.Info("cleaned up microservice metadata in PD mode",
			zap.Bool("cleared-default-keyspace-group-members", true))
	}
	return nil
}

func (s *Server) validateMicroserviceMetadataCleanup() (bool, error) {
	// At most two records are needed: the default group and the first unsupported
	// non-default group, if one exists.
	groups, err := s.storage.LoadKeyspaceGroups(constant.DefaultKeyspaceGroupID, 2)
	if err != nil {
		return false, err
	}
	needsCleanup := false
	for _, group := range groups {
		if group == nil {
			continue
		}
		if group.ID != constant.DefaultKeyspaceGroupID {
			return false, rejectMicroserviceMetadataCleanup("found non-default TSO keyspace group %d when cleaning up PD mode microservice metadata", group.ID)
		}
		if group.IsSplitting() {
			return false, rejectMicroserviceMetadataCleanup("default TSO keyspace group is splitting when cleaning up PD mode microservice metadata")
		}
		if group.IsMerging() {
			return false, rejectMicroserviceMetadataCleanup("default TSO keyspace group is merging when cleaning up PD mode microservice metadata")
		}
		needsCleanup = len(group.Members) > 0
	}
	return needsCleanup, nil
}

func (s *Server) clearDefaultTSOKeyspaceGroupMembers(
	ctx context.Context,
	term microserviceMetadataCleanupTerm,
) (bool, error) {
	groupKey := keypath.KeyspaceGroupIDPath(constant.DefaultKeyspaceGroupID)
	resp, err := etcdutil.EtcdKVGet(s.client, groupKey)
	if err != nil {
		return false, err
	}
	if len(resp.Kvs) == 0 {
		return false, nil
	}
	if len(resp.Kvs) != 1 {
		return false, errs.ErrEtcdKVGetResponse.FastGenByArgs(resp.Kvs)
	}

	groupKV := resp.Kvs[0]
	group := &endpoint.KeyspaceGroup{}
	if err := json.Unmarshal(groupKV.Value, group); err != nil {
		return false, errs.ErrJSONUnmarshal.Wrap(err).GenWithStackByCause()
	}
	if group.ID != constant.DefaultKeyspaceGroupID {
		return false, rejectMicroserviceMetadataCleanup(
			"found TSO keyspace group %d at the default group path when clearing PD mode microservice metadata", group.ID)
	}
	if group.IsSplitting() {
		return false, rejectMicroserviceMetadataCleanup("default TSO keyspace group is splitting when clearing PD mode microservice metadata")
	}
	if group.IsMerging() {
		return false, rejectMicroserviceMetadataCleanup("default TSO keyspace group is merging when clearing PD mode microservice metadata")
	}
	if len(group.Members) == 0 {
		return false, nil
	}

	group.Members = nil
	value, err := json.Marshal(group)
	if err != nil {
		return false, errs.ErrJSONMarshal.Wrap(err).GenWithStackByCause()
	}

	failpoint.InjectCall("beforeClearDefaultTSOKeyspaceGroupMembersCommit")
	txnResp, err := kv.NewSlowLogTxnWithContext(ctx, s.client).
		If(
			clientv3.Compare(clientv3.Value(term.leaderKey), "=", term.leaderValue),
			clientv3.Compare(clientv3.LeaseValue(term.leaderKey), "=", term.leaseID),
			clientv3.Compare(clientv3.ModRevision(groupKey), "=", groupKV.ModRevision),
		).
		Then(clientv3.OpPut(groupKey, string(value))).
		Commit()
	if err != nil {
		return false, errs.ErrEtcdTxnInternal.Wrap(err).GenWithStackByCause()
	}
	if !txnResp.Succeeded {
		return false, errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	return true, nil
}
