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

package discovery

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// Discover is used to get all the service instances of the specified service name.
func Discover(cli *clientv3.Client, serviceName string) ([]string, error) {
	key := keypath.ServicePath(serviceName)
	endKey := clientv3.GetPrefixRangeEnd(key)

	withRange := clientv3.WithRange(endKey)
	resp, err := etcdutil.EtcdKVGet(cli, key, withRange)
	if err != nil {
		return nil, err
	}
	values := make([]string, 0, len(resp.Kvs))
	for _, item := range resp.Kvs {
		values = append(values, string(item.Value))
	}
	return values, nil
}

// GetMSMembers returns all the members of the specified service name.
func GetMSMembers(serviceName string, client *clientv3.Client) ([]ServiceRegistryEntry, error) {
	switch serviceName {
	case constant.TSOServiceName, constant.SchedulingServiceName, constant.ResourceManagerServiceName:
		servicePath := keypath.ServicePath(serviceName)
		resps, err := kv.NewSlowLogTxn(client).Then(clientv3.OpGet(servicePath, clientv3.WithPrefix())).Commit()
		if err != nil {
			return nil, errs.ErrEtcdKVGet.Wrap(err).GenWithStackByCause()
		}
		if !resps.Succeeded {
			return nil, errs.ErrEtcdTxnConflict.FastGenByArgs()
		}

		var entries []ServiceRegistryEntry
		for _, resp := range resps.Responses {
			for _, keyValue := range resp.GetResponseRange().GetKvs() {
				var entry ServiceRegistryEntry
				if err = entry.Deserialize(keyValue.Value); err != nil {
					log.Error("try to deserialize service registry entry failed", zap.String("key", string(keyValue.Key)), zap.Error(err))
					continue
				}
				entries = append(entries, entry)
			}
		}
		return entries, nil
	}

	return nil, errors.Errorf("unknown service name %s", serviceName)
}
