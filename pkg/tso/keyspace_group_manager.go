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
	"path"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/mcs/utils"
	mcsutils "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/memberutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	// maxKeyspaceGroupCount is the max count of keyspace groups.
	// Keyspace group in tso is the sharding unit, i.e., by the definition here,
	// the max count of the shards we support is maxKeyspaceGroupCount. We use
	// five-digits number (%05d) to render the keyspace group id in the storage
	// path, so theoritically the max count is 99999 which is far beyond what's
	// actually needed.
	maxKeyspaceGroupCount = uint32(4096)
	// primaryElectionSuffix is the suffix of the key for keyspace group primary election
	primaryElectionSuffix = "primary"
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

	ctx        context.Context
	cancel     context.CancelFunc
	etcdClient *clientv3.Client
	// electionNamePrefix is the name prefix to generate the unique name of a participant,
	// which participate in the election of its keyspace group's primary, in the format of
	// "electionNamePrefix:keyspace-group-id"
	electionNamePrefix string
	// defaultKsgStorageTSRootPath is the root path of the default keyspace group in the
	// storage endpoiont which is used for LoadTimestamp/SaveTimestamp.
	// This is the legacy root path in the format of "/pd/{cluster_id}".
	// Below is the entire path of in the legacy format (used by the default keyspace group)
	// Key: /pd/{cluster_id}/timestamp
	// Value: ts(time.Time)
	// Key: /pd/{cluster_id}/lta/{dc-location}/timestamp
	// Value: ts(time.Time)
	defaultKsgStorageTSRootPath string
	// tsoSvcRootPath defines the root path for all etcd paths used for different purposes.
	// It is in the format of "/ms/<cluster-id>/tso".
	// The main paths for different usages in the tso microservice include:
	// 1. The path for keyspace group primary election. Format: "/ms/{cluster_id}/tso/{group}/primary"
	// 2. The path for LoadTimestamp/SaveTimestamp in the storage endpoint for all the non-default
	//    keyspace groups.
	//    Key: /ms/{cluster_id}/tso/{group}/gts/timestamp
	//    Value: ts(time.Time)
	//    Key: /ms/{cluster_id}/tso/{group}/lts/{dc-location}/timestamp
	//    Value: ts(time.Time)
	// Note: The {group} is 5 digits integer with leading zeros.
	tsoSvcRootPath string
	// cfg is the TSO config
	cfg           *Config
	maxResetTSGap func() time.Duration
}

// NewAllocatorManager creates a new Keyspace Group Manager.
func NewKeyspaceGroupManager(
	ctx context.Context,
	etcdClient *clientv3.Client,
	electionNamePrefix string,
	defaultKsgStorageTSRootPath string,
	tsoSvcRootPath string,
	cfg *Config,
) *KeyspaceGroupManager {
	ctx, cancel := context.WithCancel(ctx)
	ksgMgr := &KeyspaceGroupManager{
		ctx:                         ctx,
		cancel:                      cancel,
		etcdClient:                  etcdClient,
		electionNamePrefix:          electionNamePrefix,
		defaultKsgStorageTSRootPath: defaultKsgStorageTSRootPath,
		tsoSvcRootPath:              tsoSvcRootPath,
		cfg:                         cfg,
		maxResetTSGap:               func() time.Duration { return cfg.MaxResetTSGap.Duration },
	}

	return ksgMgr
}

func (s *KeyspaceGroupManager) Initialize() {
	// TODO: dynamically load keyspace group assignment from the persistent storage and add watch for the assignment change

	// Generate the default keyspace group
	uniqueName := fmt.Sprintf("%s:%5d", s.electionNamePrefix, utils.DefaultKeySpaceGroupID)
	uniqueID := memberutil.GenerateUniqueID(uniqueName)
	log.Info("joining primary election", zap.String("participant-name", uniqueName), zap.Uint64("participant-id", uniqueID))

	participant := member.NewParticipant(s.etcdClient)
	participant.InitInfo(uniqueName, uniqueID, path.Join(s.tsoSvcRootPath, fmt.Sprintf("%05d", mcsutils.DefaultKeySpaceGroupID)),
		primaryElectionSuffix, "keyspace group primary election", s.cfg.AdvertiseListenAddr)

	defaultKsgGroupStorage := endpoint.NewStorageEndpoint(kv.NewEtcdKVBase(s.etcdClient, s.defaultKsgStorageTSRootPath), nil)
	s.ksgAllocatorManagers[utils.DefaultKeySpaceGroupID] = NewAllocatorManager(
		s.ctx, true, mcsutils.DefaultKeySpaceGroupID, participant, s.defaultKsgStorageTSRootPath, defaultKsgGroupStorage,
		s.cfg.IsLocalTSOEnabled(), s.cfg.GetTSOSaveInterval(), s.cfg.GetTSOUpdatePhysicalInterval(), s.cfg.GetLeaderLease(),
		s.cfg.GetTLSConfig(), s.maxResetTSGap)
}
