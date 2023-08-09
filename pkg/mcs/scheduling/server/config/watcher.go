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
	"sync"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/log"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/server/config"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
)

// Watcher is used to watch the PD API server for any configuration changes.
type Watcher struct {
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	etcdClient *clientv3.Client
	watcher    *etcdutil.LoopWatcher

	*PersistConfig
	// TODO: watch the scheduler config change.
}

type persistedConfig struct {
	ClusterVersion semver.Version       `json:"cluster-version"`
	Schedule       sc.ScheduleConfig    `json:"schedule"`
	Replication    sc.ReplicationConfig `json:"replication"`
	Store          config.StoreConfig   `json:"store"`
}

// NewWatcher creates a new watcher to watch the config meta change from PD API server.
func NewWatcher(
	ctx context.Context,
	etcdClient *clientv3.Client,
	// configPath is the path of the configuration in etcd:
	//  - Key: /pd/{cluster_id}/config
	//  - Value: configuration JSON.
	configPath string,
	persistConfig *PersistConfig,
) (*Watcher, error) {
	ctx, cancel := context.WithCancel(ctx)
	cw := &Watcher{
		ctx:           ctx,
		cancel:        cancel,
		etcdClient:    etcdClient,
		PersistConfig: persistConfig,
	}
	putFn := func(kv *mvccpb.KeyValue) error {
		cfg := &persistedConfig{}
		if err := json.Unmarshal(kv.Value, cfg); err != nil {
			log.Warn("failed to unmarshal scheduling config entry",
				zap.String("event-kv-key", string(kv.Key)), zap.Error(err))
			return err
		}
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
func (cw *Watcher) Close() {
	cw.cancel()
	cw.wg.Wait()
}
