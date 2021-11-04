// Copyright 2021 TiKV Project Authors.
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

package core

import (
	"fmt"
	"path"

	"github.com/tikv/pd/server/kv"
	"go.etcd.io/etcd/clientv3"
)

// Because the etcd storage inside the PD server is initialized with a root path,
// so we only define some key suffixes here rather than the full key paths.
const (
	clusterPath                = "raft"
	configPath                 = "config"
	schedulePath               = "schedule"
	gcPath                     = "gc"
	rulesPath                  = "rules"
	ruleGroupPath              = "rule_group"
	regionLabelPath            = "region_label"
	replicationPath            = "replication_mode"
	componentPath              = "component"
	customScheduleConfigPath   = "scheduler_config"
	encryptionKeysPath         = "encryption_keys"
	gcWorkerServiceSafePointID = "gc_worker"
)

// ClusterStatePath returns the path to save an option.
func ClusterStatePath(option string) string {
	return path.Join(clusterPath, "status", option)
}

// EncryptionKeysPath returns the path to save encryption keys.
func EncryptionKeysPath() string {
	return path.Join(encryptionKeysPath, "keys")
}

func storePath(storeID uint64) string {
	return path.Join(clusterPath, "s", fmt.Sprintf("%020d", storeID))
}

func regionPath(regionID uint64) string {
	return path.Join(clusterPath, "r", fmt.Sprintf("%020d", regionID))
}

func storeLeaderWeightPath(storeID uint64) string {
	return path.Join(schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "leader")
}

func storeRegionWeightPath(storeID uint64) string {
	return path.Join(schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "region")
}

var _ Storage = (*EtcdStorage)(nil)

// EtcdStorage is a storage that stores data in etcd,
// which is used by the PD server.
type EtcdStorage struct {
	defaultStorage
}

// NewEtcdStorage is used to create a new etcd storage.
func NewEtcdStorage(client *clientv3.Client, rootPatch string) *EtcdStorage {
	return &EtcdStorage{defaultStorage{kv.NewEtcdKVBase(client, rootPatch), nil}}
}
