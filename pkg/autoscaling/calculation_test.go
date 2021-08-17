// Copyright 2020 TiKV Project Authors.
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

package autoscaling

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/kv"
	"github.com/tikv/pd/server/versioninfo"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

const (
	mockRaftClusterRoot      = "root"
	mockRaftClusterClusterID = 1

	mockTiKVInstanceZero = "tidb-cluster-01-tikv-0_tidb-cluster-01"
	mockTiKVInstanceOne  = "tidb-cluster-01-tikv-1_tidb-cluster-01"
	mockTiKVInstanceTwo  = "tidb-cluster-01-tikv-2_tidb-cluster-01"

	mockTiDBInstanceZero = "tidb-cluster-01-tidb-0_tidb-cluster-01"
	mockTiDBInstanceOne  = "tidb-cluster-01-tidb-1_tidb-cluster-01"
	mockTiDBInstanceTwo  = "tidb-cluster-01-tidb-2_tidb-cluster-01"

	tidbTTLKeySuffix  = "ttl"
	tidbInfoKeySuffix = "info"
	defaultVersion    = "5.0.0"

	mockMaxThreshold = 0.45
	mockMinThreshold = 0.4
	mockResourceType = "storage_small"

	mockCPUQuota      = 1 * milliCores
	mockMemorySize    = 1024 * 1024 * 1024
	mockStoreCapacity = 10 * 1024 * 1024 * 1024
	mockStoreUsedSize = 5 * 1024 * 1024 * 1024
	mockNodeCount     = 5
	mockRegionCount   = 100
)

var (
	strategy = initMockStrategy()
	querier  = &mockQuerier{}
)

func Test(t *testing.T) {
	TestingT(t)
}

func initMockStrategy() *Strategy {
	tikvCPURule := &CPURule{
		MaxThreshold:  mockMaxThreshold,
		MinThreshold:  mockMinThreshold,
		ResourceTypes: []string{mockResourceType, homogeneousTiKVResourceType},
	}
	storageRule := &StorageRule{
		MaxThreshold:  mockMaxThreshold,
		MinThreshold:  mockMinThreshold,
		ResourceTypes: []string{mockResourceType, homogeneousTiKVResourceType},
	}
	tidbCPURule := &CPURule{
		MaxThreshold:  mockMaxThreshold,
		MinThreshold:  mockMinThreshold,
		ResourceTypes: []string{mockResourceType, homogeneousTiDBResourceType},
	}

	tikvRule := &Rule{
		Component:   TiKV.String(),
		CPURule:     tikvCPURule,
		StorageRule: storageRule,
	}
	tidbRule := &Rule{
		Component: TiDB.String(),
		CPURule:   tidbCPURule,
	}

	homogeneousTiKVResource := &Resource{
		ResourceType: homogeneousTiKVResourceType,
		CPU:          mockCPUQuota,
		Memory:       mockMemorySize,
		Storage:      mockStoreCapacity,
	}
	homogeneousTiDBResource := &Resource{
		ResourceType: homogeneousTiDBResourceType,
		CPU:          mockCPUQuota,
		Memory:       mockMemorySize,
		Storage:      mockStoreCapacity,
	}
	heterogeneousResource := &Resource{
		ResourceType: mockResourceType,
		CPU:          mockCPUQuota,
		Memory:       mockMemorySize,
		Storage:      mockStoreCapacity,
	}

	return &Strategy{
		Rules:     []*Rule{tikvRule, tidbRule},
		Resources: []*Resource{homogeneousTiKVResource, homogeneousTiDBResource, heterogeneousResource},
		NodeCount: mockNodeCount,
	}
}

func newTiDBInfo(address string) *TiDBInfo {
	ut := time.Now().Unix()
	version := defaultVersion

	return &TiDBInfo{
		Version:        &version,
		StartTimestamp: &ut,
		Labels:         nil,
		GitHash:        nil,
		Address:        address,
	}
}

func initMockEtcdClient(c *C) (*clientv3.Client, func()) {
	cfg := etcdutil.NewTestSingleConfig()
	etcd, err := embed.StartEtcd(cfg)
	c.Assert(err, IsNil)

	ep := cfg.LCUrls[0].String()
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	c.Assert(err, IsNil)

	<-etcd.Server.ReadyNotify()

	_, err = etcdutil.EtcdKVPutWithTTL(context.TODO(), etcdClient, prometheusAddressKey, "127.0.0.1:9090", 3600)
	c.Assert(err, IsNil)

	_, err = etcdutil.EtcdKVPutWithTTL(context.TODO(), etcdClient, fmt.Sprintf("%s%s/%s", tidbInfoPrefix, "127.0.0.1:4", tidbTTLKeySuffix), "127.0.0.1:4", 3600)
	c.Assert(err, IsNil)
	_, err = etcdutil.EtcdKVPutWithTTL(context.TODO(), etcdClient, fmt.Sprintf("%s%s/%s", tidbInfoPrefix, "127.0.0.1:5", tidbTTLKeySuffix), "127.0.0.1:5", 3600)
	c.Assert(err, IsNil)
	_, err = etcdutil.EtcdKVPutWithTTL(context.TODO(), etcdClient, fmt.Sprintf("%s%s/%s", tidbInfoPrefix, "127.0.0.1:6", tidbTTLKeySuffix), "127.0.0.1:6", 3600)
	c.Assert(err, IsNil)

	tidbInfo := newTiDBInfo("127.0.0.1:4")
	jsonBytes, err := json.Marshal(tidbInfo)
	c.Assert(err, IsNil)
	_, err = etcdutil.EtcdKVPutWithTTL(context.TODO(), etcdClient, fmt.Sprintf("%s%s/%s", tidbInfoPrefix, "127.0.0.1:4", tidbInfoKeySuffix), string(jsonBytes), 3600)
	c.Assert(err, IsNil)
	tidbInfo = newTiDBInfo("127.0.0.1:5")
	jsonBytes, err = json.Marshal(tidbInfo)
	c.Assert(err, IsNil)
	_, err = etcdutil.EtcdKVPutWithTTL(context.TODO(), etcdClient, fmt.Sprintf("%s%s/%s", tidbInfoPrefix, "127.0.0.1:5", tidbInfoKeySuffix), string(jsonBytes), 3600)
	c.Assert(err, IsNil)
	tidbInfo = newTiDBInfo("127.0.0.1:6")
	jsonBytes, err = json.Marshal(tidbInfo)
	c.Assert(err, IsNil)
	_, err = etcdutil.EtcdKVPutWithTTL(context.TODO(), etcdClient, fmt.Sprintf("%s%s/%s", tidbInfoPrefix, "127.0.0.1:6", tidbInfoKeySuffix), string(jsonBytes), 3600)
	c.Assert(err, IsNil)

	// resp, err := etcdutil.EtcdKVGet(client, "test/ttl1")
	// c.Assert(err, IsNil)

	return etcdClient, func() {
		etcd.Close()
		etcdutil.CleanConfig(cfg)
	}
}

func initMockStores(n uint64) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, n)

	for i := uint64(1); i <= n; i++ {
		store := &metapb.Store{
			Id:         i,
			Address:    fmt.Sprintf("127.0.0.1:%d", i),
			State:      metapb.StoreState_Up,
			Version:    "5.0.0",
			DeployPath: fmt.Sprintf("test/store%d", i),
		}
		storeInfo := core.NewStoreInfo(store)
		storeStats := &pdpb.StoreStats{
			StoreId:     store.GetId(),
			Capacity:    mockStoreCapacity,
			UsedSize:    mockStoreUsedSize,
			Available:   mockStoreCapacity - mockStoreUsedSize,
			RegionCount: mockRegionCount,
		}
		opt := core.SetNewStoreStats(storeStats)

		stores = append(stores, storeInfo.ShallowClone(opt))
	}

	return stores
}

func initMockRaftCluster(c *C) (*cluster.RaftCluster, func()) {
	etcdClient, closeEtcdFunc := initMockEtcdClient(c)

	cfg := config.NewConfig()
	cfg.Schedule.TolerantSizeRatio = 5
	err := cfg.Adjust(nil, false)
	c.Assert(err, IsNil)
	opt := config.NewPersistOptions(cfg)
	opt.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version2_0))

	rc := cluster.NewRaftCluster(context.Background(), mockRaftClusterRoot, mockRaftClusterClusterID, nil, etcdClient, nil)
	rc.InitCluster(mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()), core.NewBasicCluster())

	stores := initMockStores(3)
	for _, store := range stores {
		err = rc.PutStore(store.GetMeta())
		c.Assert(err, IsNil)
		err = rc.HandleStoreHeartbeat(store.GetStoreStats())
		c.Assert(err, IsNil)
	}

	return rc, closeEtcdFunc
}

var _ = Suite(&calculationTestSuite{})

type calculationTestSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *calculationTestSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *calculationTestSuite) TearDownTest(c *C) {
	s.cancel()
}

type mockQuerier struct{}

func (q *mockQuerier) Query(options *QueryOptions) (QueryResult, error) {
	qr := make(QueryResult)
	switch options.component {
	case TiKV:
		switch options.metric {
		case CPUUsage:
			qr[mockTiKVInstanceZero] = 60 * mockResultValue
			qr[mockTiKVInstanceOne] = 60 * mockResultValue
			qr[mockTiKVInstanceTwo] = 60 * mockResultValue
		case CPUQuota:
			qr[mockTiKVInstanceZero] = 2 * mockResultValue
			qr[mockTiKVInstanceOne] = 2 * mockResultValue
			qr[mockTiKVInstanceTwo] = 2 * mockResultValue
		}
	case TiDB:
		switch options.metric {
		case CPUUsage:
			qr[mockTiDBInstanceZero] = 60 * mockResultValue
			qr[mockTiDBInstanceOne] = 60 * mockResultValue
			qr[mockTiDBInstanceTwo] = 60 * mockResultValue
		case CPUQuota:
			qr[mockTiDBInstanceZero] = 2 * mockResultValue
			qr[mockTiDBInstanceOne] = 2 * mockResultValue
			qr[mockTiDBInstanceTwo] = 2 * mockResultValue
		}
	}

	return qr, nil
}

func (s *calculationTestSuite) TestGetTiKVPlans(c *C) {
	rc, closeEtcdFunc := initMockRaftCluster(c)
	defer func() {
		closeEtcdFunc()
	}()

	instances, err := getInstancesByComponent(rc, TiKV)
	c.Assert(err, IsNil)
	resourceMap, err := getResourceMapByComponent(rc, instances, TiKV)
	c.Assert(err, IsNil)
	groups := getHeterogeneousGroupsByComponent(resourceMap, TiKV)

	tikvPlans, err := getTiKVPlans(rc, querier, instances, strategy, resourceMap)
	c.Assert(err, IsNil)

	plans := mergePlans(tikvPlans, groups)
	c.Assert(plans[0].Count, Equals, uint64(4))

	fmt.Printf("plans: %v\n", plans)
}

func (s *calculationTestSuite) TestGetTiDBPlans(c *C) {
	rc, closeEtcdFunc := initMockRaftCluster(c)
	defer func() {
		closeEtcdFunc()
	}()

	instances, err := getInstancesByComponent(rc, TiDB)
	c.Assert(err, IsNil)
	resourceMap, err := getResourceMapByComponent(rc, instances, TiDB)
	c.Assert(err, IsNil)
	groups := getHeterogeneousGroupsByComponent(resourceMap, TiDB)

	tiDBPlans, err := getTiDBPlans(querier, instances, strategy, resourceMap)
	c.Assert(err, IsNil)

	plans := mergePlans(tiDBPlans, groups)

	fmt.Printf("plans: %v\n", plans)
}
