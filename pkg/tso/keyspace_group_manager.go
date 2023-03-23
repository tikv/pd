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

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	mcsutils "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/memberutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	// maxKeyspaceGroupCount is the max count of keyspace groups. keyspace group in tso
	// is the sharding unit, i.e., by the definition here, the max count of the shards
	// that we support is maxKeyspaceGroupCount. The keyspace group id is in the range
	// [0, 99999], which explains we use five-digits number (%05d) to render the keyspace
	// group id in the storage endpoint path.
	maxKeyspaceGroupCount = uint32(100000)
	// maxKeyspaceGroupCountInUse is the max count of keyspace groups in use, which should
	// never exceed maxKeyspaceGroupCount defined above. Compared to maxKeyspaceGroupCount,
	// maxKeyspaceGroupCountInUse is a much more reasonable value of the max count in the
	// foreseen future, and the former is just for extensibility in theory.
	maxKeyspaceGroupCountInUse = uint32(4096)
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
	ksgAllocatorManagers [maxKeyspaceGroupCountInUse]*AllocatorManager

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
	cfg           ServiceConfig
	maxResetTSGap func() time.Duration
}

// NewKeyspaceGroupManager creates a new Keyspace Group Manager.
func NewKeyspaceGroupManager(
	ctx context.Context,
	etcdClient *clientv3.Client,
	electionNamePrefix string,
	defaultKsgStorageTSRootPath string,
	tsoSvcRootPath string,
	cfg ServiceConfig,
) *KeyspaceGroupManager {
	if maxKeyspaceGroupCountInUse > maxKeyspaceGroupCount {
		log.Fatal("maxKeyspaceGroupCountInUse is larger than maxKeyspaceGroupCount",
			zap.Uint32("maxKeyspaceGroupCountInUse", maxKeyspaceGroupCountInUse),
			zap.Uint32("maxKeyspaceGroupCount", maxKeyspaceGroupCount))
	}

	ctx, cancel := context.WithCancel(ctx)
	ksgMgr := &KeyspaceGroupManager{
		ctx:                         ctx,
		cancel:                      cancel,
		etcdClient:                  etcdClient,
		electionNamePrefix:          electionNamePrefix,
		defaultKsgStorageTSRootPath: defaultKsgStorageTSRootPath,
		tsoSvcRootPath:              tsoSvcRootPath,
		cfg:                         cfg,
		maxResetTSGap:               func() time.Duration { return cfg.GetMaxResetTSGap() },
	}

	return ksgMgr
}

// Initialize this KeyspaceGroupManager
func (kgm *KeyspaceGroupManager) Initialize() {
	// TODO: dynamically load keyspace group assignment from the persistent storage and add watch for the assignment change
	kgm.initDefaultKeyspaceGroup()
}

// Initialize this the default keyspace group
func (kgm *KeyspaceGroupManager) initDefaultKeyspaceGroup() {
	uniqueName := fmt.Sprintf("%s-%5d", kgm.electionNamePrefix, mcsutils.DefaultKeySpaceGroupID)
	uniqueID := memberutil.GenerateUniqueID(uniqueName)
	log.Info("joining primary election", zap.String("participant-name", uniqueName), zap.Uint64("participant-id", uniqueID))

	participant := member.NewParticipant(kgm.etcdClient)
	participant.InitInfo(uniqueName, uniqueID, path.Join(kgm.tsoSvcRootPath, fmt.Sprintf("%05d", mcsutils.DefaultKeySpaceGroupID)),
		primaryElectionSuffix, "keyspace group primary election", kgm.cfg.GetAdvertiseListenAddr())

	defaultKsgGroupStorage := endpoint.NewStorageEndpoint(kv.NewEtcdKVBase(kgm.etcdClient, kgm.defaultKsgStorageTSRootPath), nil)
	kgm.ksgAllocatorManagers[mcsutils.DefaultKeySpaceGroupID] = NewAllocatorManager(
		kgm.ctx, true, mcsutils.DefaultKeySpaceGroupID, participant,
		kgm.defaultKsgStorageTSRootPath, defaultKsgGroupStorage,
		kgm.cfg.IsLocalTSOEnabled(), kgm.cfg.GetTSOSaveInterval(),
		kgm.cfg.GetTSOUpdatePhysicalInterval(), kgm.cfg.GetLeaderLease(),
		kgm.cfg.GetTLSConfig(), kgm.maxResetTSGap)
}

// GetAllocatorManager returns the AllocatorManager of the given keyspace group
func (kgm *KeyspaceGroupManager) GetAllocatorManager(keyspaceGroupID uint32) *AllocatorManager {
	return kgm.ksgAllocatorManagers[keyspaceGroupID]
}

// GetElectionMember returns the election member of the given keyspace group
func (kgm *KeyspaceGroupManager) GetElectionMember(keyspaceGroupID uint32) ElectionMember {
	return *kgm.ksgAllocatorManagers[keyspaceGroupID].getMember()
}

// HandleTSORequest forwards TSO allocation requests to correct TSO Allocators of the given keyspace group.
func (kgm *KeyspaceGroupManager) HandleTSORequest(keyspaceGroupID uint32, dcLocation string, count uint32) (pdpb.Timestamp, error) {
	return kgm.ksgAllocatorManagers[keyspaceGroupID].HandleTSORequest(dcLocation, count)
}

// Close this KeyspaceGroupManager
func (kgm *KeyspaceGroupManager) Close() {
	kgm.cancel()
	kgm.ksgAllocatorManagers[mcsutils.DefaultKeySpaceGroupID].close()
}
