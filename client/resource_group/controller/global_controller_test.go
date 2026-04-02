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

// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

// MockResourceGroupProvider is a mock implementation of the ResourceGroupProvider interface.
type MockResourceGroupProvider struct {
	mock.Mock
}

func newMockResourceGroupProvider() *MockResourceGroupProvider {
	mockProvider := &MockResourceGroupProvider{}
	mockProvider.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(&meta_storagepb.GetResponse{}, nil)
	mockProvider.On("LoadResourceGroups", mock.Anything).Return([]*rmpb.ResourceGroup{}, int64(0), nil)
	mockProvider.On("Watch", mock.Anything, mock.Anything, mock.Anything).Return(make(chan []*meta_storagepb.Event), nil)
	return mockProvider
}

func (m *MockResourceGroupProvider) GetResourceGroup(ctx context.Context, resourceGroupName string, opts ...pd.GetResourceGroupOption) (*rmpb.ResourceGroup, error) {
	var err error
	failpoint.Inject("gerResourceGroupError", func() {
		err = errors.New("fake get resource group error")
	})
	if err != nil {
		return nil, &errs.ErrClientGetResourceGroup{ResourceGroupName: resourceGroupName, Cause: err.Error()}
	}

	args := m.Called(ctx, resourceGroupName, opts)
	return args.Get(0).(*rmpb.ResourceGroup), args.Error(1)
}

func (m *MockResourceGroupProvider) ListResourceGroups(ctx context.Context, opts ...pd.GetResourceGroupOption) ([]*rmpb.ResourceGroup, error) {
	args := m.Called(ctx, opts)
	return args.Get(0).([]*rmpb.ResourceGroup), args.Error(1)
}

func (m *MockResourceGroupProvider) AddResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error) {
	args := m.Called(ctx, metaGroup)
	return args.String(0), args.Error(1)
}

func (m *MockResourceGroupProvider) ModifyResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error) {
	args := m.Called(ctx, metaGroup)
	return args.String(0), args.Error(1)
}

func (m *MockResourceGroupProvider) DeleteResourceGroup(ctx context.Context, resourceGroupName string) (string, error) {
	args := m.Called(ctx, resourceGroupName)
	return args.String(0), args.Error(1)
}

func (m *MockResourceGroupProvider) AcquireTokenBuckets(ctx context.Context, request *rmpb.TokenBucketsRequest) ([]*rmpb.TokenBucketResponse, error) {
	args := m.Called(ctx, request)
	return args.Get(0).([]*rmpb.TokenBucketResponse), args.Error(1)
}

func (m *MockResourceGroupProvider) LoadResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, int64, error) {
	args := m.Called(ctx)
	return args.Get(0).([]*rmpb.ResourceGroup), args.Get(1).(int64), args.Error(2)
}

func (m *MockResourceGroupProvider) Watch(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (chan []*meta_storagepb.Event, error) {
	args := m.Called(ctx, key, opts)
	return args.Get(0).(chan []*meta_storagepb.Event), args.Error(1)
}

func (m *MockResourceGroupProvider) Get(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.GetResponse, error) {
	args := m.Called(ctx, key, opts)
	return args.Get(0).(*meta_storagepb.GetResponse), args.Error(1)
}

func (m *MockResourceGroupProvider) Put(ctx context.Context, key []byte, value []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.PutResponse, error) {
	args := m.Called(ctx, key, value, opts)
	return args.Get(0).(*meta_storagepb.PutResponse), args.Error(1)
}

func TestControllerWithTwoGroupRequestConcurrency(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	re.NoError(failpoint.Enable("github.com/tikv/pd/client/resource_group/controller/triggerPeriodicReport", fmt.Sprintf("return(\"%s\")", defaultResourceGroupName)))
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/resource_group/controller/triggerLowRUReport", fmt.Sprintf("return(\"%s\")", "test-group")))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/resource_group/controller/triggerPeriodicReport"))
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/resource_group/controller/triggerLowRUReport"))
	}()

	mockProvider := newMockResourceGroupProvider()
	controller, err := NewResourceGroupController(ctx, 1, mockProvider, nil, constants.NullKeyspaceID)
	re.NoError(err)
	controller.Start(ctx)

	defaultResourceGroup := &rmpb.ResourceGroup{Name: defaultResourceGroupName, Mode: rmpb.GroupMode_RUMode, RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 1000000}}}}
	testResourceGroup := &rmpb.ResourceGroup{Name: "test-group", Mode: rmpb.GroupMode_RUMode, RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 1000000}}}}
	mockProvider.On("GetResourceGroup", mock.Anything, defaultResourceGroupName, mock.Anything).Return(defaultResourceGroup, nil)
	mockProvider.On("GetResourceGroup", mock.Anything, "test-group", mock.Anything).Return(testResourceGroup, nil)

	c1, err := controller.tryGetResourceGroupController(ctx, defaultResourceGroupName, false)
	re.NoError(err)
	re.Equal(defaultResourceGroup, c1.meta)

	c2, err := controller.tryGetResourceGroupController(ctx, "test-group", false)
	re.NoError(err)
	re.Equal(testResourceGroup, c2.meta)

	// test report ru consumption
	var totalConsumption rmpb.Consumption
	c2.mu.Lock()
	totalConsumption = *c2.mu.consumption
	c2.mu.Unlock()
	delta := &rmpb.Consumption{
		RRU:                      1.0,
		WRU:                      2.0,
		ReadBytes:                10,
		WriteBytes:               20,
		TotalCpuTimeMs:           30.0,
		SqlLayerCpuTimeMs:        40.0,
		KvReadRpcCount:           50,
		KvWriteRpcCount:          60,
		ReadCrossAzTrafficBytes:  100,
		WriteCrossAzTrafficBytes: 200,
	}
	controller.ReportConsumption("test-group", delta)
	// check the consumption
	c2.mu.Lock()
	add(&totalConsumption, delta)
	require.Equal(t, c2.mu.consumption, &totalConsumption)
	c2.mu.Unlock()

	controller.ReportRUV2Consumption("test-group", 3.0, 4.0, 5.0)
	c2.mu.Lock()
	totalConsumption.TikvRUV2 += 3.0
	totalConsumption.TidbRUV2 += 4.0
	totalConsumption.TiflashRUV2 += 5.0
	require.Equal(t, c2.mu.consumption, &totalConsumption)
	c2.mu.Unlock()

	// test report with unknown group
	controller.ReportConsumption("unknown-name", delta)
	controller.ReportRUV2Consumption("unknown-name", 1.0, 1.0, 1.0)

	var expectResp []*rmpb.TokenBucketResponse
	recTestGroupAcquireTokenRequest := make(chan bool)
	mockProvider.On("AcquireTokenBuckets", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		request := args.Get(1).(*rmpb.TokenBucketsRequest)
		var responses []*rmpb.TokenBucketResponse
		for _, req := range request.Requests {
			if req.ResourceGroupName == defaultResourceGroupName {
				// no response the default group request, that's mean `len(c.run.currentRequests) != 0` always.
				select {
				case <-ctx.Done():
					return
				case <-time.After(100 * time.Second):
				}
				responses = append(responses, &rmpb.TokenBucketResponse{
					ResourceGroupName: defaultResourceGroupName,
					GrantedRUTokens: []*rmpb.GrantedRUTokenBucket{
						{
							GrantedTokens: &rmpb.TokenBucket{
								Tokens: 100000,
							},
						},
					},
				})
			} else {
				responses = append(responses, &rmpb.TokenBucketResponse{
					ResourceGroupName: req.ResourceGroupName,
					GrantedRUTokens: []*rmpb.GrantedRUTokenBucket{
						{
							GrantedTokens: &rmpb.TokenBucket{
								Tokens: 100000,
							},
						},
					},
				})
			}
		}
		// receive test-group request
		if len(request.Requests) == 1 && request.Requests[0].ResourceGroupName == "test-group" {
			recTestGroupAcquireTokenRequest <- true
		}
		expectResp = responses
	}).Return(expectResp, nil)
	// wait default group request token by PeriodicReport.
	time.Sleep(2 * time.Second)
	counter := c2.run.requestUnitTokens
	counter.limiter.mu.Lock()
	counter.limiter.notify()
	counter.limiter.mu.Unlock()
	select {
	case res := <-recTestGroupAcquireTokenRequest:
		re.True(res)
	case <-time.After(5 * time.Second):
		re.Fail("timeout")
	}
}

func TestTryGetController(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockProvider := newMockResourceGroupProvider()
	controller, err := NewResourceGroupController(ctx, 1, mockProvider, nil, constants.NullKeyspaceID)
	re.NoError(err)
	controller.Start(ctx)

	defaultResourceGroup := &rmpb.ResourceGroup{Name: defaultResourceGroupName, Mode: rmpb.GroupMode_RUMode, RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 1000000}}}}
	testResourceGroup := &rmpb.ResourceGroup{Name: "test-group", Mode: rmpb.GroupMode_RUMode, RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 1000000}}}}
	mockProvider.On("GetResourceGroup", mock.Anything, defaultResourceGroupName, mock.Anything).Return(defaultResourceGroup, nil)
	mockProvider.On("GetResourceGroup", mock.Anything, "test-group", mock.Anything).Return(testResourceGroup, nil)
	mockProvider.On("GetResourceGroup", mock.Anything, "test-group-non-existent", mock.Anything).Return((*rmpb.ResourceGroup)(nil), nil)

	gc, err := controller.tryGetResourceGroupController(ctx, "test-group-non-existent", false)
	re.Error(err)
	re.Nil(gc)
	gc, err = controller.tryGetResourceGroupController(ctx, defaultResourceGroupName, false)
	re.NoError(err)
	re.Equal(defaultResourceGroup, gc.getMeta())
	gc, err = controller.tryGetResourceGroupController(ctx, "test-group", false)
	re.NoError(err)
	re.Equal(testResourceGroup, gc.getMeta())
	requestInfo, responseInfo := NewTestRequestInfo(true, 1, 1, AccessCrossZone), NewTestResponseInfo(1, time.Millisecond, true)
	_, _, _, _, err = controller.OnRequestWait(ctx, "test-group", requestInfo)
	re.NoError(err)
	consumption, err := controller.OnResponse("test-group", requestInfo, responseInfo)
	re.NoError(err)
	re.NotEmpty(consumption)
	// Mark the tombstone manually to test the fallback case.
	gc, err = controller.tryGetResourceGroupController(ctx, "test-group", false)
	re.NoError(err)
	re.NotNil(gc)
	controller.tombstoneGroupCostController("test-group")
	gc, err = controller.tryGetResourceGroupController(ctx, "test-group", false)
	re.Error(err)
	re.Nil(gc)
	gc, err = controller.tryGetResourceGroupController(ctx, "test-group", true)
	re.NoError(err)
	re.Equal(defaultResourceGroup, gc.getMeta())
	_, _, _, _, err = controller.OnRequestWait(ctx, "test-group", requestInfo)
	re.NoError(err)
	consumption, err = controller.OnResponse("test-group", requestInfo, responseInfo)
	re.NoError(err)
	re.NotEmpty(consumption)
	// Test the default group protection.
	gc, err = controller.tryGetResourceGroupController(ctx, defaultResourceGroupName, false)
	re.NoError(err)
	re.Equal(defaultResourceGroup, gc.getMeta())
	controller.tombstoneGroupCostController(defaultResourceGroupName)
	gc, err = controller.tryGetResourceGroupController(ctx, defaultResourceGroupName, false)
	re.NoError(err)
	re.Equal(defaultResourceGroup, gc.getMeta())
	gc, err = controller.tryGetResourceGroupController(ctx, defaultResourceGroupName, true)
	re.NoError(err)
	re.Equal(defaultResourceGroup, gc.getMeta())
	_, _, _, _, err = controller.OnRequestWait(ctx, defaultResourceGroupName, requestInfo)
	re.NoError(err)
	consumption, err = controller.OnResponse(defaultResourceGroupName, requestInfo, responseInfo)
	re.NoError(err)
	re.NotEmpty(consumption)
}

func TestGetResourceGroup(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Enable the failpoint to simulate an error when getting the resource group.
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/resource_group/controller/gerResourceGroupError", `return()`))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/resource_group/controller/gerResourceGroupError"))
	}()

	mockProvider := newMockResourceGroupProvider()

	expectResourceGroupName := "test-group"
	expectRUSettings := &rmpb.GroupRequestUnitSettings{
		RU: &rmpb.TokenBucket{
			Settings: &rmpb.TokenLimitSettings{
				FillRate:   uint64(50),
				BurstLimit: int64(100),
			},
		},
	}
	expectResourceGroup := &rmpb.ResourceGroup{
		Name:       expectResourceGroupName,
		Mode:       rmpb.GroupMode_RUMode,
		RUSettings: expectRUSettings,
	}

	opts := []ResourceControlCreateOption{WithDegradedRUSettings(expectRUSettings)}

	controller, err := NewResourceGroupController(ctx, 1, mockProvider, nil, constants.NullKeyspaceID, opts...)
	re.NoError(err)
	controller.Start(ctx)

	testResourceGroup := &rmpb.ResourceGroup{
		Name: "test-group",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate: 1000000,
				},
			},
		},
	}
	mockProvider.On("GetResourceGroup", mock.Anything, "test-group", mock.Anything).Return(testResourceGroup, nil)

	// case1: when GetResourceGroup return error, it should return the expectResourceGroup.
	gc, err := controller.tryGetResourceGroupController(ctx, "test-group", false)
	re.NoError(err)
	re.Equal(expectResourceGroup, gc.getMeta())

	// case2: when GetResourceGroup return error again, It should still return the expectResourceGroup.
	gc, err = controller.tryGetResourceGroupController(ctx, "test-group", false)
	re.NoError(err)
	re.Equal(expectResourceGroup, gc.getMeta())

	// case3: If `GetResourceGroup` returns no error (`nil`), it should return the testResourceGroup.
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/resource_group/controller/gerResourceGroupError"))
	gc, err = controller.tryGetResourceGroupController(ctx, "test-group", false)
	re.NoError(err)
	re.Equal(testResourceGroup, gc.getMeta())

	// case4: when we don't set degradedRUSettings and GetResourceGroup return error, tryGetResourceGroupController will return err.
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/resource_group/controller/gerResourceGroupError", `return()`))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/resource_group/controller/gerResourceGroupError"))
	}()

	controller02, err := NewResourceGroupController(ctx, 1, mockProvider, nil, constants.NullKeyspaceID)
	re.NoError(err)
	controller02.Start(ctx)

	gc02, err := controller02.tryGetResourceGroupController(ctx, "test-group", false)
	re.Error(err)
	re.Nil(gc02)
}

func TestReloadResourceGroupMetaWatch(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockProvider := &MockResourceGroupProvider{}
	mockProvider.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(&meta_storagepb.GetResponse{}, nil)
	controller, err := NewResourceGroupController(ctx, 1, mockProvider, nil, constants.NullKeyspaceID)
	re.NoError(err)

	defaultGroup := &rmpb.ResourceGroup{
		Name:       defaultResourceGroupName,
		Mode:       rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 1000000}}},
	}
	staleGroup := &rmpb.ResourceGroup{
		Name:       "stale-group",
		Mode:       rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 100}}},
	}
	newGroup := &rmpb.ResourceGroup{
		Name:       "new-group",
		Mode:       rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 300}}},
	}
	updatedDefaultGroup := &rmpb.ResourceGroup{
		Name:       defaultResourceGroupName,
		Mode:       rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 2000}}},
	}

	mockProvider.On("GetResourceGroup", mock.Anything, defaultResourceGroupName, mock.Anything).Return(defaultGroup, nil)
	mockProvider.On("GetResourceGroup", mock.Anything, "stale-group", mock.Anything).Return(staleGroup, nil)

	defaultGC, err := controller.tryGetResourceGroupController(ctx, defaultResourceGroupName, false)
	re.NoError(err)
	re.NotNil(defaultGC)

	staleGC, err := controller.tryGetResourceGroupController(ctx, "stale-group", false)
	re.NoError(err)
	re.NotNil(staleGC)
	re.False(staleGC.tombstone.Load())

	watchCh := make(chan []*meta_storagepb.Event)
	mockProvider.On("LoadResourceGroups", mock.Anything).
		Return([]*rmpb.ResourceGroup{updatedDefaultGroup, newGroup}, int64(88), nil).
		Once()
	mockProvider.On("Watch", mock.Anything, pd.GroupSettingsPathPrefixBytes(constants.NullKeyspaceID), mock.Anything).
		Run(func(args mock.Arguments) {
			opts := args.Get(2).([]opt.MetaStorageOption)
			metaOp := &opt.MetaStorageOp{}
			for _, apply := range opts {
				apply(metaOp)
			}
			re.Equal(int64(89), metaOp.Revision)
			re.True(metaOp.IsOptsWithPrefix)
			re.True(metaOp.PrevKv)
		}).
		Return(watchCh, nil).
		Once()

	reloadedWatchCh, err := controller.reloadResourceGroupMetaWatch(ctx)
	re.NoError(err)
	re.Equal(watchCh, reloadedWatchCh)
	re.Equal(updatedDefaultGroup, defaultGC.getMeta())
	staleGC, err = controller.tryGetResourceGroupController(ctx, "stale-group", true)
	re.NoError(err)
	re.True(staleGC.tombstone.Load())
	newGC, ok := controller.loadGroupController("new-group")
	re.True(ok)
	re.Equal(newGroup, newGC.getMeta())
}

func TestTokenBucketsRequestWithKeyspaceID(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checkKeyspace := func(keyspaceID uint32) {
		mockProvider := newMockResourceGroupProvider()
		controller, err := NewResourceGroupController(ctx, 1, mockProvider, nil, keyspaceID)
		re.NoError(err)
		controller.Start(ctx)

		testResourceGroup := &rmpb.ResourceGroup{
			Name: "test-group",
			Mode: rmpb.GroupMode_RUMode,
			RUSettings: &rmpb.GroupRequestUnitSettings{
				RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 1000000}},
			},
		}
		mockProvider.On("GetResourceGroup", mock.Anything, "test-group", mock.Anything).Return(testResourceGroup, nil)

		gc, err := controller.tryGetResourceGroupController(ctx, "test-group", false)
		re.NoError(err)
		re.NotNil(gc)

		requestReceived := make(chan bool, 1)

		mockProvider.On("AcquireTokenBuckets", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			request := args.Get(1).(*rmpb.TokenBucketsRequest)
			re.Len(request.Requests, 1)
			req := request.Requests[0]
			re.NotNil(req.KeyspaceId)
			re.Equal(keyspaceID, req.GetKeyspaceId().GetValue())
			requestReceived <- true
		}).Return([]*rmpb.TokenBucketResponse{}, nil)

		// Trigger a low token report to ensure collectTokenBucketRequests is called
		counter := gc.run.requestUnitTokens
		counter.limiter.mu.Lock()
		counter.limiter.notify()
		counter.limiter.mu.Unlock()

		select {
		case <-requestReceived:
			// Success
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for AcquireTokenBuckets to be called")
		}
	}
	checkKeyspace(constants.NullKeyspaceID)
	checkKeyspace(1)
}

func TestGetRUVersionDefault(t *testing.T) {
	re := require.New(t)
	mockProvider := newMockResourceGroupProvider()
	gc, err := NewResourceGroupController(context.Background(), 1, mockProvider, nil, 1)
	re.NoError(err)

	// Default should return 1 (v1) when no policy is set.
	re.Equal(int32(1), gc.GetRUVersion())
}

func TestGetRUVersionAfterSet(t *testing.T) {
	re := require.New(t)
	mockProvider := newMockResourceGroupProvider()
	gc, err := NewResourceGroupController(context.Background(), 1, mockProvider, nil, 1)
	re.NoError(err)

	// Simulate ru_version update via atomic store.
	gc.ruVersion.Store(3)
	re.Equal(int32(3), gc.GetRUVersion())

	// Zero should return 1 (default).
	gc.ruVersion.Store(0)
	re.Equal(int32(1), gc.GetRUVersion())

	// Negative should also return 1 (default).
	gc.ruVersion.Store(-1)
	re.Equal(int32(1), gc.GetRUVersion())
}

func TestRUVersionFromControllerConfig(t *testing.T) {
	re := require.New(t)
	mockProvider := newMockResourceGroupProvider()
	// keyspaceID = 42
	gc, err := NewResourceGroupController(context.Background(), 1, mockProvider, nil, 42)
	re.NoError(err)

	// Simulate a controller config with RUVersionPolicy containing an override for keyspace 42.
	config := DefaultConfig()
	config.RUVersionPolicy = &RUVersionPolicy{
		Default:   1,
		Overrides: map[uint32]RUVersion{42: 3},
	}
	gc.updateRUVersionFromConfig(config)
	re.Equal(int32(3), gc.GetRUVersion())
}

func TestRUVersionOverrideFromControllerConfig(t *testing.T) {
	re := require.New(t)
	mockProvider := newMockResourceGroupProvider()
	// keyspaceID = 42
	gc, err := NewResourceGroupController(context.Background(), 1, mockProvider, nil, 42)
	re.NoError(err)

	// Override takes precedence over default.
	config := DefaultConfig()
	config.RUVersionPolicy = &RUVersionPolicy{
		Default:   5,
		Overrides: map[uint32]RUVersion{42: 3, 100: 7},
	}
	gc.updateRUVersionFromConfig(config)
	re.Equal(int32(3), gc.GetRUVersion())
}

func TestRUVersionDefaultFallback(t *testing.T) {
	re := require.New(t)
	mockProvider := newMockResourceGroupProvider()
	// keyspaceID = 42, no override for 42
	gc, err := NewResourceGroupController(context.Background(), 1, mockProvider, nil, 42)
	re.NoError(err)

	config := DefaultConfig()
	config.RUVersionPolicy = &RUVersionPolicy{
		Default:   5,
		Overrides: map[uint32]RUVersion{100: 7},
	}
	gc.updateRUVersionFromConfig(config)
	// No override for keyspace 42, use default.
	re.Equal(int32(5), gc.GetRUVersion())
}

func TestRUVersionNilPolicy(t *testing.T) {
	re := require.New(t)
	mockProvider := newMockResourceGroupProvider()
	gc, err := NewResourceGroupController(context.Background(), 1, mockProvider, nil, 42)
	re.NoError(err)

	// Set a non-default version first.
	gc.ruVersion.Store(5)
	re.Equal(int32(5), gc.GetRUVersion())

	// Nil policy resets to 0 (GetRUVersion returns 1 as default).
	config := DefaultConfig()
	config.RUVersionPolicy = nil
	gc.updateRUVersionFromConfig(config)
	re.Equal(int32(1), gc.GetRUVersion())
}

func TestRUVersionFromInitialControllerConfig(t *testing.T) {
	re := require.New(t)

	// Simulate a provider that returns a controller config with RUVersionPolicy.
	configWithPolicy := &Config{
		BaseConfig: BaseConfig{
			RUVersionPolicy: &RUVersionPolicy{
				Default:   1,
				Overrides: map[uint32]RUVersion{42: 3},
			},
		},
	}
	configBytes, err := json.Marshal(configWithPolicy)
	re.NoError(err)

	mockProvider := &MockResourceGroupProvider{}
	mockProvider.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(&meta_storagepb.GetResponse{
		Header: &meta_storagepb.ResponseHeader{Revision: 1},
		Kvs: []*meta_storagepb.KeyValue{
			{Value: configBytes},
		},
	}, nil)
	mockProvider.On("LoadResourceGroups", mock.Anything).Return([]*rmpb.ResourceGroup{}, int64(0), nil)
	mockProvider.On("Watch", mock.Anything, mock.Anything, mock.Anything).Return(make(chan []*meta_storagepb.Event), nil)

	gc, err := NewResourceGroupController(context.Background(), 1, mockProvider, nil, 42)
	re.NoError(err)
	// The initial load should set ruVersion from the policy.
	re.Equal(int32(3), gc.GetRUVersion())
}

func TestRUVersionWatchViaControllerConfig(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockProvider := &MockResourceGroupProvider{}
	mockProvider.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(&meta_storagepb.GetResponse{
		Header: &meta_storagepb.ResponseHeader{Revision: 1},
	}, nil)
	mockProvider.On("LoadResourceGroups", mock.Anything).Return([]*rmpb.ResourceGroup{}, int64(0), nil)

	watchConfigChan := make(chan []*meta_storagepb.Event, 1)
	mockProvider.On("Watch", mock.Anything, pd.ControllerConfigPathPrefixBytes, mock.Anything).Return(watchConfigChan, nil)
	mockProvider.On("Watch", mock.Anything, mock.Anything, mock.Anything).Return(make(chan []*meta_storagepb.Event), nil)

	controller, err := NewResourceGroupController(ctx, 1, mockProvider, nil, 42)
	re.NoError(err)
	controller.Start(ctx)

	// Case 1: Config with RUVersionPolicy containing override for keyspace 42
	configWithPolicy := &Config{
		BaseConfig: BaseConfig{
			RUVersionPolicy: &RUVersionPolicy{
				Default:   1,
				Overrides: map[uint32]RUVersion{42: 3},
			},
		},
	}
	val, _ := json.Marshal(configWithPolicy)
	watchConfigChan <- []*meta_storagepb.Event{
		{
			Type: meta_storagepb.Event_PUT,
			Kv:   &meta_storagepb.KeyValue{Value: val},
		},
	}
	testutil.Eventually(re, func() bool {
		return controller.GetRUVersion() == 3
	})

	// Case 2: Config without RUVersionPolicy (nil) resets to default
	configNoPolicy := &Config{}
	val, _ = json.Marshal(configNoPolicy)
	watchConfigChan <- []*meta_storagepb.Event{
		{
			Type: meta_storagepb.Event_PUT,
			Kv:   &meta_storagepb.KeyValue{Value: val},
		},
	}
	testutil.Eventually(re, func() bool {
		return controller.GetRUVersion() == 1 // Reset to default
	})
}
