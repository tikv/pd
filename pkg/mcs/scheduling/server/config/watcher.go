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
	"sync/atomic"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/log"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/schedulers"
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

	// Some data, like the global schedule config, should be loaded into `PersistConfig`.
	*PersistConfig
	// Some data, like the scheduler configs, should be loaded into the storage
	// to make sure the coordinator could access them correctly.
	storage storage.Storage
	// schedulersController is used to trigger the scheduler's config reloading.
	// Store as `*schedulers.Controller`.
	schedulersController atomic.Value
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

// SetSchedulersController sets the schedulers controller.
func (cw *Watcher) SetSchedulersController(sc *schedulers.Controller) {
	cw.schedulersController.Store(sc)
}

func (cw *Watcher) getSchedulersController() *schedulers.Controller {
	sc := cw.schedulersController.Load()
	if sc == nil {
		return nil
	}
	return sc.(*schedulers.Controller)
}

func (cw *Watcher) initializeConfigWatcher() error {
	putFn := func(kv *mvccpb.KeyValue) error {
		cfg := &persistedConfig{}
		if err := json.Unmarshal(kv.Value, cfg); err != nil {
			log.Warn("failed to unmarshal scheduling config entry",
				zap.String("event-kv-key", string(kv.Key)), zap.Error(err))
			return err
		}
		log.Info("update scheduling config", zap.Reflect("new", cfg))
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
		name := strings.TrimPrefix(string(kv.Key), prefixToTrim)
		log.Info("update scheduler config", zap.String("name", name),
			zap.String("value", string(kv.Value)))
		err := cw.storage.SaveSchedulerConfig(name, kv.Value)
		if err != nil {
			log.Warn("failed to save scheduler config",
				zap.String("event-kv-key", string(kv.Key)),
				zap.String("trimmed-key", name),
				zap.Error(err))
			return err
		}
		// Ensure the scheduler config could be updated as soon as possible.
		if sc := cw.getSchedulersController(); sc != nil {
			return sc.ReloadSchedulerConfig(name)
		}
		return nil
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		log.Info("remove scheduler config", zap.String("key", string(kv.Key)))
		return cw.storage.RemoveSchedulerConfig(
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
