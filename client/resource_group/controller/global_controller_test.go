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

	// test report with unknown group
	controller.ReportConsumption("unknown-name", delta)

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
