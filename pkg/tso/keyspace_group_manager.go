// Copyright 2023 TiKV Project Authors.
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

package tso

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/memberutil"
	"go.uber.org/zap"
)

const (
	// maxKeyspaceGroupCount is the max count of keyspace groups.
	maxKeyspaceGroupCount = uint32(4096)
)

// KeyspaceGroupManager manages the primary/secondaries of the keyspace groups
// assigned to this host. The primaries provide the tso service for the corresponding
// keyspace groups.
type KeyspaceGroupManager struct {
	// ksgAllocatorManagers[i] stores the AllocatorManager of the keyspace group i.
	// Use a fixed size array to maximize the efficiency of concurrent access to
	// different keyspace groups for tso service.
	// TODO: change item type to atomic.Value stored as *AllocatorManager after we
	// support online keyspace group assignment.
	ksgAllocatorManagers [maxKeyspaceGroupCount]*AllocatorManager

	ctx    context.Context
	cancel context.CancelFunc
	// TSO config
	ksgRootPath            string
	storage                endpoint.TSOStorage
	enableLocalTSO         bool
	saveInterval           time.Duration
	updatePhysicalInterval time.Duration
	// leaderLease defines the time within which a TSO primary/leader must update its TTL
	// in etcd, otherwise etcd will expire the leader key and other servers can campaign
	// the primary/leader again. Etcd only supports seconds TTL, so here is second too.
	leaderLease    int64
	maxResetTSGap  func() time.Duration
	securityConfig *grpcutil.TLSConfig
}

// NewAllocatorManager creates a new Keyspace Group Manager.
func NewKeyspaceGroupManager(
	parentCtx context.Context,
	electionNamePrefix  
	ksgRootPath string,
	storage endpoint.TSOStorage,
	enableLocalTSO bool,
	saveInterval time.Duration,
	updatePhysicalInterval time.Duration,
	leaderLease int64,
	tlsConfig *grpcutil.TLSConfig,
	maxResetTSGap func() time.Duration,
) *KeyspaceGroupManager {
	ctx, cancel := context.WithCancel(parentCtx)
	allocatorManager := &KeyspaceGroupManager{
		ctx:                    ctx,
		cancel:                 cancel,
		ksgRootPath:            ksgRootPath,
		storage:                storage,
		enableLocalTSO:         enableLocalTSO,
		saveInterval:           saveInterval,
		updatePhysicalInterval: updatePhysicalInterval,
		leaderLease:            leaderLease,
		maxResetTSGap:          maxResetTSGap,
		securityConfig:         tlsConfig,
	}

	// TODO: add watch and dynamically load keyspace group assignment from the persistent storage
	allocatorManager.ksgAllocatorManagers[utils.DefaultKeySpaceGroupID]

	uniqueName := s.listenURL.Host // in the host:port format
	uniqueID := memberutil.GenerateUniqueID(uniqueName)
	log.Info("joining primary election", zap.String("participant-name", uniqueName), zap.Uint64("participant-id", uniqueID))

	s.participant = member.NewParticipant(s.etcdClient)
	s.participant.InitInfo(uniqueName, uniqueID, fmt.Sprintf(tsoSvcDiscoveryPrefixFormat, s.clusterID, mcsutils.DefaultKeyspaceID),
		"primary", "keyspace group primary election", s.cfg.ListenAddr)

	s.defaultGroupStorage = endpoint.NewStorageEndpoint(kv.NewEtcdKVBase(s.GetClient(), s.defaultGroupRootPath), nil)
	s.tsoAllocatorManager = tso.NewAllocatorManager(
		s.ctx, true, mcsutils.DefaultKeySpaceGroupID, s.participant, s.defaultGroupRootPath, s.defaultGroupStorage,
		s.cfg.IsLocalTSOEnabled(), s.cfg.GetTSOSaveInterval(), s.cfg.GetTSOUpdatePhysicalInterval(), s.cfg.LeaderLease,
		s.cfg.GetTLSConfig(), func() time.Duration { return s.cfg.MaxResetTSGap.Duration })

	return allocatorManager
}

func (mgr *KeyspaceGroupManager) a() {

}
