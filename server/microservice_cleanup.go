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
	"github.com/pingcap/failpoint"
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

// microserviceMetadataCleanupDelay lets clients observe the service mode change
// before stale microservice metadata disappears from etcd.
const (
	microserviceMetadataCleanupDelay         = 10 * time.Second
	microserviceMetadataCleanupRetryInterval = 5 * time.Second
)

func (s *Server) scheduleMicroserviceMetadataCleanup(ctx context.Context) {
	if s.IsKeyspaceGroupEnabled() {
		return
	}
	s.serverLoopWg.Add(1)
	go func() {
		defer logutil.LogPanic()
		defer s.serverLoopWg.Done()
		if !waitMicroserviceMetadataCleanupDelay(ctx) {
			return
		}
		for {
			if s.member != nil && !s.member.IsServing() {
				return
			}
			if err := s.cleanupMicroserviceMetadataInPDMode(ctx); err == nil {
				return
			} else {
				log.Warn("failed to clean up microservice metadata in PD mode, retry later",
					errs.ZapError(err))
			}
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

func waitMicroserviceMetadataCleanupDelay(ctx context.Context) bool {
	delay := microserviceMetadataCleanupDelay
	failpoint.Inject("skipMicroserviceMetadataCleanupDelay", func() {
		delay = 0
	})
	if delay == 0 {
		return true
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *Server) cleanupMicroserviceMetadataInPDMode(ctx context.Context) error {
	if s.IsKeyspaceGroupEnabled() {
		return nil
	}
	start := time.Now()

	defaultGroupExists, err := s.checkDefaultOnlyTSOKeyspaceGroup()
	if err != nil {
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

	if defaultGroupExists || deletedDefaultGroup || cleanedKeyspaces > 0 || deletedMicroserviceKeys > 0 {
		log.Info("cleaned up microservice metadata in PD mode",
			zap.Bool("default-keyspace-group-exists", defaultGroupExists),
			zap.Bool("deleted-default-keyspace-group", deletedDefaultGroup),
			zap.Int("cleaned-keyspace-configs", cleanedKeyspaces),
			zap.Int64("deleted-microservice-keys", deletedMicroserviceKeys),
			zap.Duration("cost", time.Since(start)))
	}
	return nil
}

func (s *Server) checkDefaultOnlyTSOKeyspaceGroup() (bool, error) {
	groups, err := s.storage.LoadKeyspaceGroups(constant.DefaultKeyspaceGroupID, 0)
	if err != nil {
		return false, err
	}
	defaultGroupExists := false
	for _, group := range groups {
		if group == nil {
			continue
		}
		if group.ID != constant.DefaultKeyspaceGroupID {
			return false, errors.Errorf("found non-default TSO keyspace group %d when cleaning up PD mode microservice metadata", group.ID)
		}
		if group.IsSplitting() {
			return false, errors.Errorf("default TSO keyspace group is splitting when cleaning up PD mode microservice metadata")
		}
		if group.IsMerging() {
			return false, errors.Errorf("default TSO keyspace group is merging when cleaning up PD mode microservice metadata")
		}
		defaultGroupExists = true
	}
	return defaultGroupExists, nil
}

func (s *Server) cleanupDefaultTSOKeyspaceGroupConfig(ctx context.Context) (int, error) {
	var (
		cleaned int
		startID uint32 = constant.StartKeyspaceID
	)
	for {
		var (
			batchCleaned int
			keyspaceIDs  []uint32
			loaded       int
		)
		err := s.storage.RunInTxn(ctx, func(txn kv.Txn) error {
			metas, err := s.storage.LoadRangeKeyspace(txn, startID, etcdutil.MaxEtcdTxnOps)
			if err != nil {
				return err
			}
			loaded = len(metas)
			keyspaceIDs = make([]uint32, 0, len(metas))
			for _, meta := range metas {
				if meta == nil {
					continue
				}
				keyspaceIDs = append(keyspaceIDs, meta.GetId())
				if meta.Config == nil {
					continue
				}
				groupIDText, ok := meta.Config[keyspace.TSOKeyspaceGroupIDKey]
				if !ok {
					continue
				}
				groupID, err := strconv.ParseUint(groupIDText, 10, 32)
				if err != nil {
					return errors.Errorf("keyspace %d has invalid TSO keyspace group ID %q", meta.GetId(), groupIDText)
				}
				if uint32(groupID) != constant.DefaultKeyspaceGroupID {
					return errors.Errorf("keyspace %d is assigned to non-default TSO keyspace group %d", meta.GetId(), groupID)
				}
				delete(meta.Config, keyspace.TSOKeyspaceGroupIDKey)
				if err := s.storage.SaveKeyspaceMeta(txn, meta); err != nil {
					return err
				}
				batchCleaned++
			}
			return nil
		})
		if err != nil {
			return cleaned, err
		}
		cleaned += batchCleaned
		if loaded < etcdutil.MaxEtcdTxnOps {
			break
		}
		if len(keyspaceIDs) == 0 {
			break
		}
		lastID := keyspaceIDs[len(keyspaceIDs)-1]
		if lastID == ^uint32(0) {
			break
		}
		startID = lastID + 1
	}
	return cleaned, nil
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
