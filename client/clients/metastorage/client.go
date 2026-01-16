// Copyright 2024 TiKV Project Authors.
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

package metastorage

import (
	"context"
	"encoding/json"
	"sort"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
)

// Client is the interface for meta storage client.
type Client interface {
	// Watch watches on a key or prefix.
	Watch(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (chan []*meta_storagepb.Event, error)
	// Get gets the value for a key.
	Get(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.GetResponse, error)
	// Put puts a key-value pair into meta storage.
	Put(ctx context.Context, key []byte, value []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.PutResponse, error)
}

// DiscoveryMSAddrs returns all the sorted members address of the specified service name.
func DiscoveryMSAddrs(ctx context.Context, serviceKey string, client Client) ([]string, error) {
	resp, err := client.Get(ctx, []byte(serviceKey), opt.WithPrefix())
	if err != nil {
		return nil, errs.ErrClientGetMetaStorageClient.Wrap(err).GenWithStackByCause()
	}
	if err := resp.GetHeader().GetError(); err != nil {
		return nil, errs.ErrClientGetProtoClient.Wrap(errors.New(err.GetMessage())).GenWithStackByCause()
	}
	sortedAddrs := make([]string, 0, len(resp.GetKvs()))
	for _, kv := range resp.GetKvs() {
		var entry ServiceRegistryEntry
		if err = entry.Deserialize(kv.Value); err != nil {
			log.Warn("try to deserialize service registry entry failed",
				zap.String("service-key", serviceKey),
				zap.String("key", string(kv.Key)), zap.Error(err))
			continue
		}
		sortedAddrs = append(sortedAddrs, entry.ServiceAddr)
	}
	sort.Strings(sortedAddrs)
	return sortedAddrs, nil
}

// ServiceRegistryEntry is the registry entry of a service
type ServiceRegistryEntry struct {
	// The specific value will be assigned only if the startup parameter is added.
	// If not assigned, the default value(service-hostname) will be used.
	Name           string `json:"name"`
	ServiceAddr    string `json:"service-addr"`
	Version        string `json:"version"`
	GitHash        string `json:"git-hash"`
	DeployPath     string `json:"deploy-path"`
	StartTimestamp int64  `json:"start-timestamp"`
}

// Deserialize the data to this service registry entry
func (e *ServiceRegistryEntry) Deserialize(data []byte) error {
	if err := json.Unmarshal(data, e); err != nil {
		log.Warn("json unmarshal the service registry entry failed", zap.Error(err))
		return err
	}
	return nil
}
