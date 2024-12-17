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

package endpoint

import (
	"context"
	"math/rand"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

// InitClusterID creates a cluster ID if it hasn't existed.
// This function assumes the cluster ID has already existed and always use a
// cheaper read to retrieve it; if it doesn't exist, invoke the more expensive
// operation initOrGetClusterID().
func InitClusterID(c *clientv3.Client) (uint64, error) {
	clusterID, err := getClusterIDFromEtcd(c)
	if err != nil {
		return 0, err
	}

	if clusterID != 0 {
		log.Info("existed cluster id", zap.Uint64("cluster-id", clusterID))
		return clusterID, nil
	}

	// If no key exist, generate a random cluster ID.
	clusterID, err = initOrGetClusterID(c)
	if err != nil {
		return 0, err
	}
	keypath.SetClusterID(clusterID)
	log.Info("init cluster id", zap.Uint64("cluster-id", clusterID))
	return clusterID, nil
}

// getClusterIDFromEtcd gets the cluster ID from etcd if local cache is not set.
func getClusterIDFromEtcd(c *clientv3.Client) (clusterID uint64, err error) {
	if id := keypath.ClusterID(); id != 0 {
		return id, nil
	}
	// Get any cluster key to parse the cluster ID.
	resp, err := etcdutil.EtcdKVGet(c, keypath.ClusterIDPath)
	if err != nil {
		return 0, err
	}
	// If no key exist, generate a random cluster ID.
	if len(resp.Kvs) == 0 {
		return 0, nil
	}
	id, err := typeutil.BytesToUint64(resp.Kvs[0].Value)
	if err != nil {
		return 0, err
	}
	keypath.SetClusterID(id)
	return id, nil
}

// initOrGetClusterID creates a cluster ID with a CAS operation,
// if the cluster ID doesn't exist.
func initOrGetClusterID(c *clientv3.Client) (uint64, error) {
	ctx, cancel := context.WithTimeout(c.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	var (
		// Generate a random cluster ID.
		r         = rand.New(rand.NewSource(time.Now().UnixNano()))
		ts        = uint64(time.Now().Unix())
		clusterID = (ts << 32) + uint64(r.Uint32())
		value     = typeutil.Uint64ToBytes(clusterID)
		key       = keypath.ClusterIDPath
	)

	// Multiple servers may try to init the cluster ID at the same time.
	// Only one server can commit this transaction, then other servers
	// can get the committed cluster ID.
	resp, err := c.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(value))).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return 0, errs.ErrEtcdTxnInternal.Wrap(err).GenWithStackByCause()
	}

	// Txn commits ok, return the generated cluster ID.
	if resp.Succeeded {
		return clusterID, nil
	}

	// Otherwise, parse the committed cluster ID.
	if len(resp.Responses) == 0 {
		return 0, errs.ErrEtcdTxnConflict.FastGenByArgs()
	}

	response := resp.Responses[0].GetResponseRange()
	if response == nil || len(response.Kvs) != 1 {
		return 0, errs.ErrEtcdTxnConflict.FastGenByArgs()
	}

	return typeutil.BytesToUint64(response.Kvs[0].Value)
}

// InitClusterIDForMs initializes the cluster ID for microservice.
func InitClusterIDForMs(ctx context.Context, client *clientv3.Client) (err error) {
	ticker := time.NewTicker(constant.RetryInterval)
	defer ticker.Stop()
	retryTimes := 0
	for {
		// Microservice should not generate cluster ID by itself.
		if clusterID, err := getClusterIDFromEtcd(client); err == nil && clusterID != 0 {
			keypath.SetClusterID(clusterID)
			log.Info("init cluster id", zap.Uint64("cluster-id", clusterID))
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-ticker.C:
			retryTimes++
			if retryTimes/500 > 0 {
				log.Warn("etcd is not ready, retrying", errs.ZapError(err))
				retryTimes /= 500
			}
		}
	}
}
