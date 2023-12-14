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
	"path"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"go.etcd.io/etcd/clientv3"
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

// GetMCSPrimary returns the primary member of the specified service name.
func GetMCSPrimary(name string, client *clientv3.Client, keyspaceGroupID string) (*pdpb.Member, int64, error) {
	keyspaceGroupIDKey := utils.DefaultKeyspaceGroupID
	if keyspaceGroupID != "" {
		isValid := func(id uint32) bool {
			return id >= utils.DefaultKeyspaceGroupID && id <= utils.MaxKeyspaceGroupCountInUse
		}

		keyspaceGroupID, err := strconv.ParseUint(keyspaceGroupID, 10, 64)
		if err != nil || !isValid(uint32(keyspaceGroupID)) {
			return nil, 0, errors.Errorf("invalid keyspace group id %d", keyspaceGroupID)
		}
		keyspaceGroupIDKey = uint32(keyspaceGroupID)
	}

	primaryPath, err := getMCSPrimaryPath(name, client, keyspaceGroupIDKey)
	if err != nil {
		return nil, 0, err
	}

	return election.GetLeader(client, primaryPath)
}

// GetMembers returns all the members of the specified service name.
func GetMembers(name string, client *clientv3.Client) (*clientv3.TxnResponse, error) {
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
		if len(resps.Responses) == 0 {
			return nil, errors.Errorf("no member found for service %s", name)
		}
		return resps, nil
	}
	return nil, errors.Errorf("unknown service name %s", name)
}

func getMCSPrimaryPath(name string, client *clientv3.Client, id uint32) (string, error) {
	switch name {
	case utils.TSOServiceName:
		clusterID, err := etcdutil.GetClusterID(client, utils.ClusterIDPath)
		if err != nil {
			return "", err
		}
		rootPath := endpoint.TSOSvcRootPath(clusterID)
		primaryPath := endpoint.KeyspaceGroupPrimaryPath(rootPath, id)
		return primaryPath, nil
	case utils.SchedulingServiceName:
		clusterID, err := etcdutil.GetClusterID(client, utils.ClusterIDPath)
		if err != nil {
			return "", err
		}
		return path.Join(endpoint.SchedulingSvcRootPath(clusterID), utils.PrimaryKey), nil
	case utils.ResourceManagerServiceName:
		clusterID, err := etcdutil.GetClusterID(client, utils.ClusterIDPath)
		if err != nil {
			return "", err
		}
		return path.Join(endpoint.ResourceManagerSvcRootPath(clusterID), utils.PrimaryKey), nil
	default:
	}
	return "", errors.Errorf("unknown service name %s", name)
}
