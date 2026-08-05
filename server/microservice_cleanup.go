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
	goerrors "errors"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/logutil"
)

const microserviceMetadataCleanupRetryInterval = 5 * time.Second

var errMicroserviceMetadataCleanupRejected = errors.New("microservice metadata cleanup rejected")

func rejectMicroserviceMetadataCleanup(format string, args ...any) error {
	return errors.Wrapf(errMicroserviceMetadataCleanupRejected, format, args...)
}

func (s *Server) scheduleMicroserviceMetadataCleanup(ctx context.Context) {
	if s.IsKeyspaceGroupEnabled() {
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
			err := s.cleanupMicroserviceMetadataInPDMode(ctx)
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

func (s *Server) cleanupMicroserviceMetadataInPDMode(ctx context.Context) error {
	if s.IsKeyspaceGroupEnabled() {
		return nil
	}

	// The persisted keyspace group contains the stale TSO member addresses that
	// block a later switch back to API service mode. Keyspace assignment markers
	// and lease-owned microservice keys are intentionally left untouched.
	needsCleanup, err := s.validateMicroserviceMetadataCleanup()
	if err != nil || !needsCleanup {
		return err
	}
	deleted, err := s.deleteDefaultTSOKeyspaceGroup(ctx)
	if err != nil {
		return err
	}
	if deleted {
		log.Info("cleaned up microservice metadata in PD mode",
			zap.Bool("deleted-default-keyspace-group", true))
	}
	return nil
}

func (s *Server) validateMicroserviceMetadataCleanup() (bool, error) {
	groups, err := s.storage.LoadKeyspaceGroups(constant.DefaultKeyspaceGroupID, 0)
	if err != nil {
		return false, err
	}
	needsCleanup := false
	for _, group := range groups {
		if group == nil {
			continue
		}
		needsCleanup = true
		if group.ID != constant.DefaultKeyspaceGroupID {
			return false, rejectMicroserviceMetadataCleanup("found non-default TSO keyspace group %d when cleaning up PD mode microservice metadata", group.ID)
		}
		if group.IsSplitting() {
			return false, rejectMicroserviceMetadataCleanup("default TSO keyspace group is splitting when cleaning up PD mode microservice metadata")
		}
		if group.IsMerging() {
			return false, rejectMicroserviceMetadataCleanup("default TSO keyspace group is merging when cleaning up PD mode microservice metadata")
		}
	}
	return needsCleanup, nil
}

func (s *Server) deleteDefaultTSOKeyspaceGroup(ctx context.Context) (bool, error) {
	deleted := false
	err := s.storage.RunInTxn(ctx, func(txn kv.Txn) error {
		group, err := s.storage.LoadKeyspaceGroup(txn, constant.DefaultKeyspaceGroupID)
		if err != nil || group == nil {
			return err
		}
		if group.IsSplitting() {
			return rejectMicroserviceMetadataCleanup("default TSO keyspace group is splitting when deleting PD mode microservice metadata")
		}
		if group.IsMerging() {
			return rejectMicroserviceMetadataCleanup("default TSO keyspace group is merging when deleting PD mode microservice metadata")
		}
		if err := s.storage.DeleteKeyspaceGroup(txn, constant.DefaultKeyspaceGroupID); err != nil {
			return err
		}
		deleted = true
		return nil
	})
	return deleted, err
}
