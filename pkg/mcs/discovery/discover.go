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
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// Discover is used to get all the service instances of the specified service name.
func Discover(cli *clientv3.Client, clusterID, serviceName string) ([]string, error) {
	key := ServicePath(clusterID, serviceName)
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
func GetMSMembers(name string, client *clientv3.Client) ([]string, error) {
	switch name {
	case utils.TSOServiceName, utils.SchedulingServiceName, utils.ResourceManagerServiceName:
		clusterID, err := etcdutil.GetClusterID(client, utils.ClusterIDPath)
		if err != nil {
			return nil, err
		}
		servicePath := ServicePath(strconv.FormatUint(clusterID, 10), name)
		resps, err := kv.NewSlowLogTxn(client).Then(clientv3.OpGet(servicePath, clientv3.WithPrefix())).Commit()
		if err != nil {
			return nil, errs.ErrEtcdKVGet.Wrap(err).GenWithStackByCause()
		}
		if !resps.Succeeded {
			return nil, errs.ErrEtcdTxnConflict.FastGenByArgs()
		}

		var addrs []string
		for _, resp := range resps.Responses {
			for _, keyValue := range resp.GetResponseRange().GetKvs() {
				var entry ServiceRegistryEntry
				if err = entry.Deserialize(keyValue.Value); err != nil {
					log.Error("try to deserialize service registry entry failed", zap.String("key", string(keyValue.Key)), zap.Error(err))
					continue
				}
				addrs = append(addrs, entry.ServiceAddr)
			}
		}
		return addrs, nil
	}

	return nil, errors.Errorf("unknown service name %s", name)
}
