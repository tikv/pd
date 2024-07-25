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
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/storage/endpoint"
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
func GetMSMembers(serviceName string, client *clientv3.Client) ([]ServiceRegistryEntry, error) {
	switch serviceName {
	case utils.TSOServiceName, utils.SchedulingServiceName, utils.ResourceManagerServiceName:
		clusterID, err := etcdutil.GetClusterID(client, utils.ClusterIDPath)
		if err != nil {
			return nil, err
		}
		servicePath := ServicePath(strconv.FormatUint(clusterID, 10), serviceName)
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

// TransferPrimary transfers the primary of the specified service.
func TransferPrimary(client *clientv3.Client, serviceName, oldPrimary, newPrimary string, keyspaceGroupID uint32) error {
	log.Info("transfer primary", zap.String("service", serviceName), zap.String("from", oldPrimary), zap.String("to", newPrimary))
	entries, err := GetMSMembers(serviceName, client)
	if err != nil {
		return err
	}

	// Do nothing when I am the only member of cluster.
	if len(entries) == 1 {
		return errors.New(fmt.Sprintf("no valid secondary to transfer primary, the only member is %s", entries[0].Name))
	}

	var primaryIDs []string
	for _, member := range entries {
		if (newPrimary == "" && member.ServiceAddr != oldPrimary) || (newPrimary != "" && member.Name == newPrimary) {
			primaryIDs = append(primaryIDs, member.ServiceAddr)
		}
	}
	if len(primaryIDs) == 0 {
		return errors.New(fmt.Sprintf("no valid secondary to transfer primary, from %s to %s", oldPrimary, newPrimary))
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	nextPrimaryID := r.Intn(len(primaryIDs))

	clusterID, err := etcdutil.GetClusterID(client, utils.ClusterIDPath)
	if err != nil {
		return errors.Errorf("failed to get cluster ID: %v", err)
	}

	var primaryKey string
	switch serviceName {
	case utils.SchedulingServiceName:
		primaryKey = endpoint.SchedulingPrimaryPath(clusterID)
	case utils.TSOServiceName:
		tsoRootPath := endpoint.TSOSvcRootPath(clusterID)
		primaryKey = endpoint.KeyspaceGroupPrimaryPath(tsoRootPath, keyspaceGroupID)
	}

	// remove possible residual value.
	utils.ClearPrimaryExpectationFlag(client, primaryKey)

	// grant the primary lease to the new primary.
	grantResp, err := client.Grant(client.Ctx(), utils.DefaultLeaderLease)
	if err != nil {
		return errors.Errorf("failed to grant lease for %s, err: %v", serviceName, err)
	}
	// update primary key to notify old primary server.
	putResp, err := kv.NewSlowLogTxn(client).
		Then(clientv3.OpPut(primaryKey, primaryIDs[nextPrimaryID], clientv3.WithLease(grantResp.ID))).
		Commit()
	if err != nil || !putResp.Succeeded {
		return errors.Errorf("failed to write primary flag for %s, err: %v", serviceName, err)
	}
	return nil
}
