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

const (
	microserviceMetadataCleanupInitialRetryInterval = time.Second
	microserviceMetadataCleanupMaxRetryInterval     = 30 * time.Second
)

var errMicroserviceMetadataCleanupRejected = errors.New("microservice metadata cleanup rejected")

type microserviceMetadataCleanupTerm struct {
	leaderKey   string
	leaderValue string
	leaseID     clientv3.LeaseID
}

func rejectMicroserviceMetadataCleanup(format string, args ...any) error {
	return errors.Wrapf(errMicroserviceMetadataCleanupRejected, format, args...)
}

// scheduleMicroserviceMetadataCleanup starts best-effort cleanup of supported
// API-service metadata after the PD leader starts serving. It does not gate
// leader readiness and is scoped to the current leadership term.
func (s *Server) scheduleMicroserviceMetadataCleanup(ctx context.Context) {
	if s.IsKeyspaceGroupEnabled() {
		return
	}
	term, ok := s.captureMicroserviceMetadataCleanupTerm()
	if !ok {
		log.Warn("cannot capture the PD leadership term for microservice metadata cleanup")
		return
	}
	s.serverLoopWg.Add(1)
	go func() {
		defer logutil.LogPanic()
		defer s.serverLoopWg.Done()
		if err := s.runMicroserviceMetadataCleanup(ctx, term); err != nil && ctx.Err() == nil {
			log.Warn("microservice metadata cleanup stopped before completion", errs.ZapError(err))
		}
	}()
}

func (s *Server) runMicroserviceMetadataCleanup(
	ctx context.Context,
	term microserviceMetadataCleanupTerm,
) error {
	retryInterval := microserviceMetadataCleanupInitialRetryInterval
	for {
		err := s.cleanupMicroserviceMetadataInPDMode(ctx, term)
		if err == nil {
			return nil
		}
		if goerrors.Is(err, errMicroserviceMetadataCleanupRejected) {
			log.Warn("cannot safely clean up microservice metadata in PD mode",
				errs.ZapError(err))
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		log.Warn("failed to clean up microservice metadata in PD mode, retry later",
			zap.Duration("retry-interval", retryInterval),
			errs.ZapError(err))
		retryTimer := time.NewTimer(retryInterval)
		select {
		case <-ctx.Done():
			retryTimer.Stop()
			return ctx.Err()
		case <-retryTimer.C:
		}
		retryInterval = min(retryInterval*2, microserviceMetadataCleanupMaxRetryInterval)
	}
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
	// Read the default group first so normal PD-mode leader campaigns do not scan
	// all keyspace groups. Non-default groups only matter when cleanup would mutate
	// the default group.
	groupKey := keypath.KeyspaceGroupIDPath(constant.DefaultKeyspaceGroupID)
	resp, err := etcdutil.EtcdKVGetWithContext(ctx, s.client, groupKey)
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return nil
	}

	groupKV := resp.Kvs[0]
	group := &endpoint.KeyspaceGroup{}
	if err := json.Unmarshal(groupKV.Value, group); err != nil {
		return rejectMicroserviceMetadataCleanup(
			"cannot decode TSO keyspace group metadata when cleaning up PD mode microservice metadata: %v", err)
	}
	if group.ID != constant.DefaultKeyspaceGroupID {
		return rejectMicroserviceMetadataCleanup("found TSO keyspace group %d at the default group path when cleaning up PD mode microservice metadata", group.ID)
	}
	if group.IsSplitting() {
		return rejectMicroserviceMetadataCleanup("default TSO keyspace group is splitting when cleaning up PD mode microservice metadata")
	}
	if group.IsMerging() {
		return rejectMicroserviceMetadataCleanup("default TSO keyspace group is merging when cleaning up PD mode microservice metadata")
	}
	if len(group.Members) == 0 {
		return nil
	}

	resp, err = etcdutil.EtcdKVGetWithContext(
		ctx,
		s.client,
		keypath.KeyspaceGroupIDPath(constant.DefaultKeyspaceGroupID+1),
		clientv3.WithRange(clientv3.GetPrefixRangeEnd(keypath.KeyspaceGroupIDPrefix())),
		clientv3.WithLimit(1),
	)
	if err != nil {
		return err
	}
	if len(resp.Kvs) > 0 {
		nonDefaultGroup := &endpoint.KeyspaceGroup{}
		if err := json.Unmarshal(resp.Kvs[0].Value, nonDefaultGroup); err != nil {
			return rejectMicroserviceMetadataCleanup(
				"cannot decode TSO keyspace group metadata when cleaning up PD mode microservice metadata: %v", err)
		}
		return rejectMicroserviceMetadataCleanup(
			"found non-default TSO keyspace group %d when cleaning up PD mode microservice metadata", nonDefaultGroup.ID)
	}

	group.Members = nil
	value, err := json.Marshal(group)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByCause()
	}

	failpoint.InjectCall("beforeMicroserviceMetadataCleanupCommit")
	txnResp, err := kv.NewSlowLogTxnWithContext(ctx, s.client).
		If(
			clientv3.Compare(clientv3.Value(term.leaderKey), "=", term.leaderValue),
			clientv3.Compare(clientv3.LeaseValue(term.leaderKey), "=", term.leaseID),
			clientv3.Compare(clientv3.ModRevision(groupKey), "=", groupKV.ModRevision),
		).
		Then(clientv3.OpPut(groupKey, string(value))).
		Commit()
	if err != nil {
		return errs.ErrEtcdTxnInternal.Wrap(err).GenWithStackByCause()
	}
	if !txnResp.Succeeded {
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	log.Info("cleaned up microservice metadata in PD mode",
		zap.Bool("cleared-default-keyspace-group-members", true))
	return nil
}
