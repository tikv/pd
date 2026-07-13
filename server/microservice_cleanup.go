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
	"fmt"
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	mcs "github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
)

const (
	microserviceMetadataCleanupRetryInterval = 5 * time.Second
	// Each loaded key adds one comparison and each updated key adds one write to
	// the etcd transaction. Use half of the operation limit so a full batch stays
	// within the transaction limit.
	microserviceMetadataCleanupBatchSize = etcdutil.MaxEtcdTxnOps / 2
)

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
	start := time.Now()

	if err := s.validateMicroserviceMetadataCleanup(ctx); err != nil {
		return err
	}
	cleanedKeyspaces, err := s.cleanupDefaultTSOKeyspaceGroupConfig(ctx)
	if err != nil {
		return err
	}
	deletedDefaultGroup, err := s.deleteDefaultTSOKeyspaceGroup(ctx)
	if err != nil {
		return err
	}
	deletedMicroserviceKeys, err := s.deleteMicroserviceEtcdKeys(ctx)
	if err != nil {
		return err
	}

	if deletedDefaultGroup || cleanedKeyspaces > 0 || deletedMicroserviceKeys > 0 {
		log.Info("cleaned up microservice metadata in PD mode",
			zap.Bool("deleted-default-keyspace-group", deletedDefaultGroup),
			zap.Int("cleaned-keyspace-configs", cleanedKeyspaces),
			zap.Int64("deleted-microservice-keys", deletedMicroserviceKeys),
			zap.Duration("cost", time.Since(start)))
	}
	return nil
}

func (s *Server) validateMicroserviceMetadataCleanup(ctx context.Context) error {
	groups, err := s.storage.LoadKeyspaceGroups(constant.DefaultKeyspaceGroupID, 0)
	if err != nil {
		return err
	}
	for _, group := range groups {
		if group == nil {
			continue
		}
		if group.ID != constant.DefaultKeyspaceGroupID {
			return errors.Errorf("found non-default TSO keyspace group %d when cleaning up PD mode microservice metadata", group.ID)
		}
		if group.IsSplitting() {
			return errors.Errorf("default TSO keyspace group is splitting when cleaning up PD mode microservice metadata")
		}
		if group.IsMerging() {
			return errors.Errorf("default TSO keyspace group is merging when cleaning up PD mode microservice metadata")
		}
	}
	return s.scanKeyspaceMetadata(ctx, etcdutil.MaxEtcdTxnOps, func(_ kv.Txn, metas []*keyspacepb.KeyspaceMeta) error {
		for _, meta := range metas {
			if meta == nil {
				continue
			}
			if err := validateDefaultTSOKeyspaceGroupAssignment(meta.GetId(), meta.GetConfig()); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Server) cleanupDefaultTSOKeyspaceGroupConfig(ctx context.Context) (int, error) {
	cleaned := 0
	err := s.scanKeyspaceMetadata(ctx, microserviceMetadataCleanupBatchSize, func(txn kv.Txn, metas []*keyspacepb.KeyspaceMeta) error {
		for _, meta := range metas {
			if meta == nil || meta.Config == nil {
				continue
			}
			if _, ok := meta.Config[keyspace.TSOKeyspaceGroupIDKey]; !ok {
				continue
			}
			if err := validateDefaultTSOKeyspaceGroupAssignment(meta.GetId(), meta.GetConfig()); err != nil {
				return err
			}
			delete(meta.Config, keyspace.TSOKeyspaceGroupIDKey)
			if err := s.storage.SaveKeyspaceMeta(txn, meta); err != nil {
				return err
			}
			cleaned++
		}
		return nil
	})
	return cleaned, err
}

func (s *Server) scanKeyspaceMetadata(
	ctx context.Context,
	batchSize int,
	process func(kv.Txn, []*keyspacepb.KeyspaceMeta) error,
) error {
	startID := constant.StartKeyspaceID
	for {
		var metas []*keyspacepb.KeyspaceMeta
		err := s.storage.RunInTxn(ctx, func(txn kv.Txn) error {
			var err error
			metas, err = s.storage.LoadRangeKeyspace(txn, startID, batchSize)
			if err != nil {
				return err
			}
			return process(txn, metas)
		})
		if err != nil {
			return err
		}
		if len(metas) < batchSize {
			return nil
		}
		lastMeta := metas[len(metas)-1]
		if lastMeta == nil {
			return nil
		}
		lastID := lastMeta.GetId()
		if lastID == ^uint32(0) {
			return nil
		}
		startID = lastID + 1
	}
}

func validateDefaultTSOKeyspaceGroupAssignment(keyspaceID uint32, config map[string]string) error {
	groupIDText, ok := config[keyspace.TSOKeyspaceGroupIDKey]
	if !ok {
		return nil
	}
	groupID, err := strconv.ParseUint(groupIDText, 10, 32)
	if err != nil {
		return errors.Errorf("keyspace %d has invalid TSO keyspace group ID %q", keyspaceID, groupIDText)
	}
	if groupID != uint64(constant.DefaultKeyspaceGroupID) {
		return errors.Errorf("keyspace %d is assigned to non-default TSO keyspace group %d", keyspaceID, groupID)
	}
	return nil
}

func (s *Server) deleteDefaultTSOKeyspaceGroup(ctx context.Context) (bool, error) {
	deleted := false
	err := s.storage.RunInTxn(ctx, func(txn kv.Txn) error {
		group, err := s.storage.LoadKeyspaceGroup(txn, constant.DefaultKeyspaceGroupID)
		if err != nil || group == nil {
			return err
		}
		if group.IsSplitting() {
			return errors.Errorf("default TSO keyspace group is splitting when deleting PD mode microservice metadata")
		}
		if group.IsMerging() {
			return errors.Errorf("default TSO keyspace group is merging when deleting PD mode microservice metadata")
		}
		if err := s.storage.DeleteKeyspaceGroup(txn, constant.DefaultKeyspaceGroupID); err != nil {
			return err
		}
		deleted = true
		return nil
	})
	return deleted, err
}

func (s *Server) deleteMicroserviceEtcdKeys(ctx context.Context) (int64, error) {
	if s.client == nil {
		return 0, nil
	}
	ctx, cancel := context.WithTimeout(ctx, etcdutil.DefaultRequestTimeout)
	defer cancel()
	resp, err := s.client.Delete(ctx, microserviceEtcdPrefix(), clientv3.WithPrefix())
	if err != nil {
		return 0, errs.ErrEtcdKVDelete.Wrap(err).GenWithStackByCause()
	}
	return resp.Deleted, nil
}

func microserviceEtcdPrefix() string {
	return fmt.Sprintf("%s/%d/", mcs.MicroserviceRootPath, keypath.ClusterID())
}
