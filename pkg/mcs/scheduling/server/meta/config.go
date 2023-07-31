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

package meta

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/pingcap/log"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
)

// persistedConfig is the configuration struct that is persisted in etcd.
// We only use it internally to do the unmarshal work.
type persistedConfig struct {
	sync.RWMutex
	Schedule    sc.ScheduleConfig    `toml:"schedule" json:"schedule"`
	Replication sc.ReplicationConfig `toml:"replication" json:"replication"`
}

// ConfigWatcher is used to watch the PD API server for any configuration changes.
type ConfigWatcher struct {
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	etcdClient *clientv3.Client
	watcher    *etcdutil.LoopWatcher

	config *persistedConfig
	// TODO: watch the scheduler config change.
}

// NewConfigWatcher creates a new watcher to watch the config meta change from PD API server.
func NewConfigWatcher(
	ctx context.Context,
	etcdClient *clientv3.Client,
	// configPath is the path of the configuration in etcd:
	//  - Key: /pd/{cluster_id}/config
	//  - Value: configuration JSON.
	configPath string,
) (*ConfigWatcher, error) {
	ctx, cancel := context.WithCancel(ctx)
	cw := &ConfigWatcher{
		ctx:        ctx,
		cancel:     cancel,
		etcdClient: etcdClient,
		config:     &persistedConfig{},
	}
	putFn := func(kv *mvccpb.KeyValue) error {
		cfg := &persistedConfig{}
		if err := json.Unmarshal(kv.Value, cfg); err != nil {
			log.Warn("failed to unmarshal scheduling config entry",
				zap.String("event-kv-key", string(kv.Key)), zap.Error(err))
			return err
		}
		cw.config.Lock()
		cw.config.Schedule = cfg.Schedule
		cw.config.Replication = cfg.Replication
		cw.config.Unlock()
		return nil
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		return nil
	}
	postEventFn := func() error {
		return nil
	}
	cw.watcher = etcdutil.NewLoopWatcher(
		ctx,
		&cw.wg,
		etcdClient,
		"scheduling-config-watcher",
		configPath,
		putFn,
		deleteFn,
		postEventFn,
	)
	cw.watcher.StartWatchLoop()
	if err := cw.watcher.WaitLoad(); err != nil {
		return nil, err
	}
	return cw, nil
}

// Close closes the watcher.
func (cw *ConfigWatcher) Close() {
	cw.cancel()
	cw.wg.Wait()
}

// GetScheduleConfig returns the schedule configuration.
func (cw *ConfigWatcher) GetScheduleConfig() sc.ScheduleConfig {
	cw.config.RLock()
	defer cw.config.RUnlock()
	return cw.config.Schedule
}

// GetReplicationConfig returns the replication configuration.
func (cw *ConfigWatcher) GetReplicationConfig() sc.ReplicationConfig {
	cw.config.RLock()
	defer cw.config.RUnlock()
	return cw.config.Replication
}
