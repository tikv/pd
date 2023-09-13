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

package config

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/log"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
)

// Watcher is used to watch the PD API server for any configuration changes.
type Watcher struct {
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	// configPath is the path of the configuration in etcd:
	//  - Key: /pd/{cluster_id}/config
	//  - Value: configuration JSON.
	configPath string
	// schedulerConfigPathPrefix is the path prefix of the scheduler configuration in etcd:
	//  - Key: /pd/{cluster_id}/scheduler_config/{scheduler_name}
	//  - Value: configuration JSON.
	schedulerConfigPathPrefix string

	etcdClient             *clientv3.Client
	configWatcher          *etcdutil.LoopWatcher
	schedulerConfigWatcher *etcdutil.LoopWatcher

	*PersistConfig
	// Some data, like the scheduler configs, should be loaded into the storage
	// to make sure the coordinator could access them correctly.
	storage storage.Storage
}

type persistedConfig struct {
	ClusterVersion semver.Version       `json:"cluster-version"`
	Schedule       sc.ScheduleConfig    `json:"schedule"`
	Replication    sc.ReplicationConfig `json:"replication"`
	Store          sc.StoreConfig       `json:"store"`
}

// NewWatcher creates a new watcher to watch the config meta change from PD API server.
func NewWatcher(
	ctx context.Context,
	etcdClient *clientv3.Client,
	clusterID uint64,
	persistConfig *PersistConfig,
	storage storage.Storage,
) (*Watcher, error) {
	ctx, cancel := context.WithCancel(ctx)
	cw := &Watcher{
		ctx:                       ctx,
		cancel:                    cancel,
		configPath:                endpoint.ConfigPath(clusterID),
		schedulerConfigPathPrefix: endpoint.SchedulerConfigPathPrefix(clusterID),
		etcdClient:                etcdClient,
		PersistConfig:             persistConfig,
		storage:                   storage,
	}
	err := cw.initializeConfigWatcher()
	if err != nil {
		return nil, err
	}
	err = cw.initializeSchedulerConfigWatcher()
	if err != nil {
		return nil, err
	}
	return cw, nil
}

func (cw *Watcher) initializeConfigWatcher() error {
	putFn := func(kv *mvccpb.KeyValue) error {
		cfg := &persistedConfig{}
		if err := json.Unmarshal(kv.Value, cfg); err != nil {
			log.Warn("failed to unmarshal scheduling config entry",
				zap.String("event-kv-key", string(kv.Key)), zap.Error(err))
			return err
		}
		cw.AdjustScheduleCfg(&cfg.Schedule)
		cw.SetClusterVersion(&cfg.ClusterVersion)
		cw.SetScheduleConfig(&cfg.Schedule)
		cw.SetReplicationConfig(&cfg.Replication)
		cw.SetStoreConfig(&cfg.Store)
		return nil
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		return nil
	}
	postEventFn := func() error {
		return nil
	}
	cw.configWatcher = etcdutil.NewLoopWatcher(
		cw.ctx, &cw.wg,
		cw.etcdClient,
		"scheduling-config-watcher", cw.configPath,
		putFn, deleteFn, postEventFn,
	)
	cw.configWatcher.StartWatchLoop()
	return cw.configWatcher.WaitLoad()
}

func (cw *Watcher) initializeSchedulerConfigWatcher() error {
	prefixToTrim := cw.schedulerConfigPathPrefix + "/"
	putFn := func(kv *mvccpb.KeyValue) error {
		return cw.storage.SaveScheduleConfig(
			strings.TrimPrefix(string(kv.Key), prefixToTrim),
			kv.Value,
		)
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		return cw.storage.RemoveScheduleConfig(
			strings.TrimPrefix(string(kv.Key), prefixToTrim),
		)
	}
	postEventFn := func() error {
		return nil
	}
	cw.schedulerConfigWatcher = etcdutil.NewLoopWatcher(
		cw.ctx, &cw.wg,
		cw.etcdClient,
		"scheduling-scheduler-config-watcher", cw.schedulerConfigPathPrefix,
		putFn, deleteFn, postEventFn,
		clientv3.WithPrefix(),
	)
	cw.schedulerConfigWatcher.StartWatchLoop()
	return cw.schedulerConfigWatcher.WaitLoad()
}

// Close closes the watcher.
func (cw *Watcher) Close() {
	cw.cancel()
	cw.wg.Wait()
}
