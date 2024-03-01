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

package pd

import (
	"context"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/tikv/pd/client/retry"
)

const (
	defaultRPCBaseBackoffInterval  = 100 * time.Millisecond
	defaultRPCMaxBackoffInterval   = 1 * time.Second
	defaultRPCBackoffTotalDuration = 120 * time.Second
)

var _ RPCClient = (*backoffClient)(nil)

type backoffClient struct {
	cli *client
	bo  *retry.Backoffer
}

func (c *backoffClient) GetAllMembers(ctx context.Context) (ret []*pdpb.Member, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.GetAllMembers(ctx)
		return err
	})
	return
}

func (c *backoffClient) GetRegion(ctx context.Context, key []byte, opts ...GetRegionOption) (ret *Region, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.GetRegion(ctx, key, opts...)
		return err
	})
	return
}

func (c *backoffClient) GetRegionFromMember(ctx context.Context, key []byte, memberURLs []string, opts ...GetRegionOption) (ret *Region, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.GetRegionFromMember(ctx, key, memberURLs, opts...)
		return err
	})
	return
}

func (c *backoffClient) GetPrevRegion(ctx context.Context, key []byte, opts ...GetRegionOption) (ret *Region, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.GetPrevRegion(ctx, key, opts...)
		return err
	})
	return
}

func (c *backoffClient) GetRegionByID(ctx context.Context, regionID uint64, opts ...GetRegionOption) (ret *Region, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.GetRegionByID(ctx, regionID, opts...)
		return err
	})
	return
}

func (c *backoffClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int, opts ...GetRegionOption) (ret []*Region, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.ScanRegions(ctx, key, endKey, limit, opts...)
		return err
	})
	return
}

func (c *backoffClient) GetStore(ctx context.Context, storeID uint64) (ret *metapb.Store, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.GetStore(ctx, storeID)
		return err
	})
	return
}

func (c *backoffClient) GetAllStores(ctx context.Context, opts ...GetStoreOption) (ret []*metapb.Store, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.GetAllStores(ctx, opts...)
		return err
	})
	return
}

func (c *backoffClient) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (ret uint64, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.UpdateGCSafePoint(ctx, safePoint)
		return err
	})
	return
}

func (c *backoffClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (ret uint64, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.UpdateServiceGCSafePoint(ctx, serviceID, ttl, safePoint)
		return err
	})
	return
}

func (c *backoffClient) ScatterRegion(ctx context.Context, regionID uint64) (err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		err = c.cli.ScatterRegion(ctx, regionID)
		return err
	})
	return
}

func (c *backoffClient) ScatterRegions(ctx context.Context, regionsID []uint64, opts ...RegionsOption) (ret *pdpb.ScatterRegionResponse, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.ScatterRegions(ctx, regionsID, opts...)
		return err
	})
	return
}

func (c *backoffClient) SplitRegions(ctx context.Context, splitKeys [][]byte, opts ...RegionsOption) (ret *pdpb.SplitRegionsResponse, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.SplitRegions(ctx, splitKeys, opts...)
		return err
	})
	return
}

func (c *backoffClient) SplitAndScatterRegions(ctx context.Context, splitKeys [][]byte, opts ...RegionsOption) (ret *pdpb.SplitAndScatterRegionsResponse, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.SplitAndScatterRegions(ctx, splitKeys, opts...)
		return err
	})
	return
}

func (c *backoffClient) GetOperator(ctx context.Context, regionID uint64) (ret *pdpb.GetOperatorResponse, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.GetOperator(ctx, regionID)
		return err
	})
	return
}

func (c *backoffClient) LoadGlobalConfig(ctx context.Context, names []string, configPath string) (config []GlobalConfigItem, revision int64, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		config, revision, err = c.cli.LoadGlobalConfig(ctx, names, configPath)
		return err
	})
	return
}

func (c *backoffClient) StoreGlobalConfig(ctx context.Context, configPath string, items []GlobalConfigItem) (err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		err = c.cli.StoreGlobalConfig(ctx, configPath, items)
		return err
	})
	return
}

func (c *backoffClient) WatchGlobalConfig(ctx context.Context, configPath string, revision int64) (configChan chan []GlobalConfigItem, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		configChan, err = c.cli.WatchGlobalConfig(ctx, configPath, revision)
		return err
	})
	return
}

func (c *backoffClient) GetExternalTimestamp(ctx context.Context) (ret uint64, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.GetExternalTimestamp(ctx)
		return err
	})
	return
}

func (c *backoffClient) SetExternalTimestamp(ctx context.Context, timestamp uint64) (err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		err = c.cli.SetExternalTimestamp(ctx, timestamp)
		return err
	})
	return
}

func (c *backoffClient) GetTS(ctx context.Context) (int64, int64, error) {
	bo := c.bo.Clone()
	return c.cli.getTSWithRetry(ctx, bo)
}

func (c *backoffClient) GetTSAsync(ctx context.Context) TSFuture {
	bo := c.bo.Clone()
	return c.cli.getTSAsyncWithRetry(ctx, bo)
}

func (c *backoffClient) GetLocalTS(ctx context.Context, dcLocation string) (int64, int64, error) {
	bo := c.bo.Clone()
	return c.cli.getLocalTSWithRetry(ctx, dcLocation, bo)
}

func (c *backoffClient) GetLocalTSAsync(ctx context.Context, dcLocation string) TSFuture {
	bo := c.bo.Clone()
	return c.cli.getLocalTSAsyncWithRetry(ctx, dcLocation, bo)
}

func (c *backoffClient) GetMinTS(ctx context.Context) (physical int64, logical int64, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		physical, logical, err = c.cli.GetMinTS(ctx)
		return err
	})
	return
}

func (c *backoffClient) Watch(ctx context.Context, key []byte, opts ...OpOption) (ret chan []*meta_storagepb.Event, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.Watch(ctx, key, opts...)
		return err
	})
	return
}

func (c *backoffClient) Get(ctx context.Context, key []byte, opts ...OpOption) (ret *meta_storagepb.GetResponse, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.Get(ctx, key, opts...)
		return err
	})
	return
}

func (c *backoffClient) Put(ctx context.Context, key []byte, value []byte, opts ...OpOption) (ret *meta_storagepb.PutResponse, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.Put(ctx, key, value, opts...)
		return err
	})
	return
}

func (c *backoffClient) LoadKeyspace(ctx context.Context, name string) (ret *keyspacepb.KeyspaceMeta, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.LoadKeyspace(ctx, name)
		return err
	})
	return
}

func (c *backoffClient) UpdateKeyspaceState(ctx context.Context, id uint32, state keyspacepb.KeyspaceState) (ret *keyspacepb.KeyspaceMeta, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.UpdateKeyspaceState(ctx, id, state)
		return err
	})
	return
}

func (c *backoffClient) WatchKeyspaces(ctx context.Context) (ret chan []*keyspacepb.KeyspaceMeta, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.WatchKeyspaces(ctx)
		return err
	})
	return
}

func (c *backoffClient) GetAllKeyspaces(ctx context.Context, startID uint32, limit uint32) (ret []*keyspacepb.KeyspaceMeta, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.GetAllKeyspaces(ctx, startID, limit)
		return err
	})
	return
}

func (c *backoffClient) UpdateGCSafePointV2(ctx context.Context, keyspaceID uint32, safePoint uint64) (ret uint64, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.UpdateGCSafePointV2(ctx, keyspaceID, safePoint)
		return err
	})
	return
}

func (c *backoffClient) UpdateServiceSafePointV2(ctx context.Context, keyspaceID uint32, serviceID string, ttl int64, safePoint uint64) (ret uint64, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.UpdateServiceSafePointV2(ctx, keyspaceID, serviceID, ttl, safePoint)
		return err
	})
	return
}

func (c *backoffClient) WatchGCSafePointV2(ctx context.Context, revision int64) (ret chan []*pdpb.SafePointEvent, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.WatchGCSafePointV2(ctx, revision)
		return err
	})
	return
}

func (c *backoffClient) ListResourceGroups(ctx context.Context, opts ...GetResourceGroupOption) (ret []*rmpb.ResourceGroup, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.ListResourceGroups(ctx, opts...)
		return err
	})
	return
}

func (c *backoffClient) GetResourceGroup(ctx context.Context, resourceGroupName string, opts ...GetResourceGroupOption) (ret *rmpb.ResourceGroup, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.GetResourceGroup(ctx, resourceGroupName, opts...)
		return err
	})
	return
}

func (c *backoffClient) AddResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (ret string, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.AddResourceGroup(ctx, metaGroup)
		return err
	})
	return
}

func (c *backoffClient) ModifyResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (ret string, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.ModifyResourceGroup(ctx, metaGroup)
		return err
	})
	return
}

func (c *backoffClient) DeleteResourceGroup(ctx context.Context, resourceGroupName string) (ret string, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.DeleteResourceGroup(ctx, resourceGroupName)
		return err
	})
	return
}

func (c *backoffClient) LoadResourceGroups(ctx context.Context) (rgs []*rmpb.ResourceGroup, revision int64, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		rgs, revision, err = c.cli.LoadResourceGroups(ctx)
		return err
	})
	return
}

func (c *backoffClient) AcquireTokenBuckets(ctx context.Context, request *rmpb.TokenBucketsRequest) (ret []*rmpb.TokenBucketResponse, err error) {
	bo := c.bo.Clone()
	bo.Exec(ctx, func() error {
		ret, err = c.cli.AcquireTokenBuckets(ctx, request)
		return err
	})
	return
}
