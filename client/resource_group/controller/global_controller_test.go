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
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/metastorage"
	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// MockResourceGroupProvider is a mock implementation of the ResourceGroupProvider interface.
type MockResourceGroupProvider struct {
	mock.Mock
}

func newMockResourceGroupProvider() *MockResourceGroupProvider {
	mockProvider := &MockResourceGroupProvider{}
	mockProvider.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(&meta_storagepb.GetResponse{}, nil)
	mockProvider.On("LoadResourceGroups", mock.Anything).Return([]*rmpb.ResourceGroup{}, int64(0), nil)
	mockProvider.On("Watch", mock.Anything, mock.Anything, mock.Anything).Return(make(chan *metastorage.WatchResponse), nil)
	return mockProvider
}

func (m *MockResourceGroupProvider) GetResourceGroup(ctx context.Context, resourceGroupName string, opts ...pd.GetResourceGroupOption) (*rmpb.ResourceGroup, error) {
	var err error
	failpoint.Inject("gerResourceGroupError", func() {
		err = errors.New("fake get resource group error")
	})
	if err != nil {
		return nil, &errs.ErrClientGetResourceGroup{ResourceGroupName: resourceGroupName, Cause: err.Error(), Err: err}
	}

	args := m.Called(ctx, resourceGroupName, opts)
	var group *rmpb.ResourceGroup
	if ret := args.Get(0); ret != nil {
		group = ret.(*rmpb.ResourceGroup)
	}
	return group, args.Error(1)
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

func (m *MockResourceGroupProvider) Watch(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (chan *metastorage.WatchResponse, error) {
	args := m.Called(ctx, key, opts)
	return args.Get(0).(chan *metastorage.WatchResponse), args.Error(1)
}

func (m *MockResourceGroupProvider) Get(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.GetResponse, error) {
	args := m.Called(ctx, key, opts)
	return args.Get(0).(*meta_storagepb.GetResponse), args.Error(1)
}

func (m *MockResourceGroupProvider) Put(ctx context.Context, key []byte, value []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.PutResponse, error) {
	args := m.Called(ctx, key, value, opts)
	return args.Get(0).(*meta_storagepb.PutResponse), args.Error(1)
}

func TestSendTokenBucketRequestsStopsWhenContextIsCanceled(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mockProvider := newMockResourceGroupProvider()
	called := make(chan struct{})
	var response []*rmpb.TokenBucketResponse
	mockProvider.On("AcquireTokenBuckets", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { close(called) }).
		Return(response, context.Canceled).
		Once()
	controller, err := NewResourceGroupController(ctx, 1, mockProvider, nil, constants.NullKeyspaceID)
	require.NoError(t, err)
	// Make a plain send block so the canceled context is the only exit path.
	controller.tokenResponseChan <- nil
	controller.sendTokenBucketRequests(ctx, nil, nil, FromLowRU, notifyMsg{})
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("AcquireTokenBuckets was not called")
	}
}

func TestControllerWithTwoGroupRequestConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		ctx := t.Context()

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
		synctest.Sleep(2 * time.Second)
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
	})
}

func TestTryGetController(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

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

func TestTryGetControllerConcurrentCreation(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	mockProvider := newMockResourceGroupProvider()
	controller, err := NewResourceGroupController(ctx, 1, mockProvider, nil, constants.NullKeyspaceID)
	re.NoError(err)
	const callers = 32
	var fetched sync.WaitGroup
	fetched.Add(callers)
	group := &rmpb.ResourceGroup{Name: "test-group", Mode: rmpb.GroupMode_RUMode, RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 1000000}}}}
	// Every caller misses the cache before any of them stores a controller.
	mockProvider.On("GetResourceGroup", mock.Anything, "test-group", mock.Anything).
		Run(func(mock.Arguments) {
			fetched.Done()
			fetched.Wait()
		}).
		Return(group, nil)

	results := make(chan *groupCostController, callers)
	for range callers {
		go func() {
			gc, _ := controller.tryGetResourceGroupController(ctx, "test-group", false)
			results <- gc
		}()
	}
	gcs := make([]*groupCostController, 0, callers)
	for range callers {
		gcs = append(gcs, <-results)
	}
	// A caller that loses the race must use the registered controller, or
	// what it records is never reported.
	registered, ok := controller.loadGroupController("test-group")
	re.True(ok)
	for _, gc := range gcs {
		re.Same(registered, gc)
	}
}

func TestCleanUpResourceGroupJudgesWholeInterval(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	mockProvider := newMockResourceGroupProvider()
	controller, err := NewResourceGroupController(ctx, 1, mockProvider, nil, 0)
	re.NoError(err)

	newGroup := func(name string) *rmpb.ResourceGroup {
		return &rmpb.ResourceGroup{Name: name, Mode: rmpb.GroupMode_RUMode, RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 1000000}}}}
	}
	for _, name := range []string{defaultResourceGroupName, "bursty", "idle", "deleted"} {
		mockProvider.On("GetResourceGroup", mock.Anything, name, mock.Anything).Return(newGroup(name), nil)
		_, err = controller.tryGetResourceGroupController(ctx, name, false)
		re.NoError(err)
	}
	controller.tombstoneGroupCostController("deleted")

	requestInfo, responseInfo := NewTestRequestInfo(true, 1, 1, AccessCrossZone), NewTestResponseInfo(1, time.Millisecond, true)
	consume := func(name string) {
		gc, err := controller.tryGetResourceGroupController(ctx, name, true)
		re.NoError(err)
		_, _, _, _, err = gc.onRequestWaitImpl(ctx, requestInfo)
		re.NoError(err)
		_, err = gc.onResponseImpl(requestInfo, responseInfo)
		re.NoError(err)
	}
	// cleanUpAfterIdleSecond simulates a cleanup pass that sees no consumption since the last state update.
	cleanUpAfterIdleSecond := func() {
		controller.executeOnAllGroups((*groupCostController).updateRunState)
		controller.cleanUpResourceGroup()
	}
	exists := func(name string) bool {
		_, ok := controller.loadGroupController(name)
		return ok
	}

	// The bursty group consumes during every cleanup interval, the tombstone only during the first one.
	consume("bursty")
	consume("deleted")
	cleanUpAfterIdleSecond()
	re.True(exists("bursty"))
	re.True(exists("idle"))
	re.True(exists("deleted"))

	consume("bursty")
	cleanUpAfterIdleSecond()
	re.True(exists("bursty"))
	re.False(exists("idle"))
	re.False(exists("deleted"))

	// The bursty group is deleted after two cleanup intervals without consumption.
	cleanUpAfterIdleSecond()
	re.True(exists("bursty"))
	cleanUpAfterIdleSecond()
	re.False(exists("bursty"))
}

func TestGetResourceGroup(t *testing.T) {
	ctx := t.Context()

	newResourceGroup := func(name string, fillRate uint64, burstLimit int64) *rmpb.ResourceGroup {
		return &rmpb.ResourceGroup{
			Name: name,
			Mode: rmpb.GroupMode_RUMode,
			RUSettings: &rmpb.GroupRequestUnitSettings{
				RU: &rmpb.TokenBucket{
					Settings: &rmpb.TokenLimitSettings{
						FillRate:   fillRate,
						BurstLimit: burstLimit,
					},
				},
			},
		}
	}
	wrapGetResourceGroupErr := func(name string, err error) error {
		if err == nil {
			return nil
		}
		return &errs.ErrClientGetResourceGroup{
			ResourceGroupName: name,
			Cause:             err.Error(),
			Err:               err,
		}
	}
	newController := func(t *testing.T, provider *MockResourceGroupProvider, opts ...ResourceControlCreateOption) *ResourceGroupsController {
		re := require.New(t)
		controller, err := NewResourceGroupController(ctx, 1, provider, nil, constants.NullKeyspaceID, opts...)
		re.NoError(err)
		return controller
	}

	degradedRUSettings := &rmpb.GroupRequestUnitSettings{
		RU: &rmpb.TokenBucket{
			Settings: &rmpb.TokenLimitSettings{
				FillRate:   50,
				BurstLimit: 100,
			},
		},
	}
	degradedResourceGroup := newResourceGroup("test-group", 50, 100)

	t.Run("transient-rm-unavailability-uses-degraded-and-recovers", func(t *testing.T) {
		re := require.New(t)
		mockProvider := newMockResourceGroupProvider()
		controller := newController(t, mockProvider, WithDegradedRUSettings(degradedRUSettings))

		realGroup := newResourceGroup("test-group", 1000000, 0)
		mockProvider.On("GetResourceGroup", mock.Anything, "test-group", mock.Anything).
			Return((*rmpb.ResourceGroup)(nil), wrapGetResourceGroupErr("test-group", status.Error(codes.Unavailable, "resource manager unavailable"))).
			Once()
		mockProvider.On("GetResourceGroup", mock.Anything, "test-group", mock.Anything).
			Return(realGroup, nil).
			Once()

		gc, err := controller.tryGetResourceGroupController(ctx, "test-group", false)
		re.NoError(err)
		re.Equal(degradedResourceGroup, gc.getMeta())

		gc, err = controller.tryGetResourceGroupController(ctx, "test-group", false)
		re.NoError(err)
		re.Equal(realGroup, gc.getMeta())
		mockProvider.AssertNumberOfCalls(t, "GetResourceGroup", 2)
	})

	t.Run("resource-group-not-found-returns-original-error", func(t *testing.T) {
		re := require.New(t)
		mockProvider := newMockResourceGroupProvider()
		controller := newController(t, mockProvider, WithDegradedRUSettings(degradedRUSettings))

		notFoundErr := wrapGetResourceGroupErr("test-group",
			status.Error(codes.Unknown, "[PD:resourcemanager:ErrGroupNotExists]the test-group resource group does not exist"))
		mockProvider.On("GetResourceGroup", mock.Anything, "test-group", mock.Anything).
			Return((*rmpb.ResourceGroup)(nil), notFoundErr).
			Once()

		gc, err := controller.tryGetResourceGroupController(ctx, "test-group", false)
		re.Error(err)
		re.Same(notFoundErr, err)
		re.Nil(gc)
		mockProvider.AssertNumberOfCalls(t, "GetResourceGroup", 1)
	})

	t.Run("caller-cancellation-returns-context-canceled", func(t *testing.T) {
		re := require.New(t)
		mockProvider := newMockResourceGroupProvider()
		controller := newController(t, mockProvider, WithDegradedRUSettings(degradedRUSettings))

		mockProvider.On("GetResourceGroup", mock.Anything, "test-group", mock.Anything).
			Return((*rmpb.ResourceGroup)(nil), wrapGetResourceGroupErr("test-group", context.Canceled)).
			Once()

		gc, err := controller.tryGetResourceGroupController(ctx, "test-group", false)
		re.ErrorIs(err, context.Canceled)
		re.Equal(context.Canceled, err)
		re.Nil(gc)
		mockProvider.AssertNumberOfCalls(t, "GetResourceGroup", 1)
	})

	t.Run("caller-deadline-returns-context-deadline-exceeded", func(t *testing.T) {
		re := require.New(t)
		mockProvider := newMockResourceGroupProvider()
		controller := newController(t, mockProvider, WithDegradedRUSettings(degradedRUSettings))

		mockProvider.On("GetResourceGroup", mock.Anything, "test-group", mock.Anything).
			Return((*rmpb.ResourceGroup)(nil), wrapGetResourceGroupErr("test-group", context.DeadlineExceeded)).
			Once()

		gc, err := controller.tryGetResourceGroupController(ctx, "test-group", false)
		re.ErrorIs(err, context.DeadlineExceeded)
		re.Equal(context.DeadlineExceeded, err)
		re.Nil(gc)
		mockProvider.AssertNumberOfCalls(t, "GetResourceGroup", 1)
	})

	t.Run("server-grpc-status-without-degraded-settings-preserves-original-error", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			code codes.Code
		}{
			{name: "canceled", code: codes.Canceled},
			{name: "deadline-exceeded", code: codes.DeadlineExceeded},
		} {
			t.Run(tc.name, func(t *testing.T) {
				re := require.New(t)
				activeCtx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)

				mockProvider := newMockResourceGroupProvider()
				controller := newController(t, mockProvider)

				serverErr := status.Error(tc.code, "resource manager returned an error")
				wrappedErr := &errs.ErrClientGetResourceGroup{
					ResourceGroupName: "test-group",
					Cause:             serverErr.Error(),
					Err:               serverErr,
				}
				mockProvider.On("GetResourceGroup", mock.Anything, "test-group", mock.Anything).
					Return((*rmpb.ResourceGroup)(nil), wrappedErr).
					Once()

				gc, err := controller.tryGetResourceGroupController(activeCtx, "test-group", false)
				re.Nil(gc)
				re.Same(wrappedErr, err)
				re.Same(serverErr, wrappedErr.Err)
				re.Equal(tc.code, status.Code(err))
				mockProvider.AssertNumberOfCalls(t, "GetResourceGroup", 1)
			})
		}
	})

	t.Run("grpc-deadline-exceeded-uses-degraded-group", func(t *testing.T) {
		re := require.New(t)
		mockProvider := newMockResourceGroupProvider()
		controller := newController(t, mockProvider, WithDegradedRUSettings(degradedRUSettings))

		mockProvider.On("GetResourceGroup", mock.Anything, "test-group", mock.Anything).
			Return((*rmpb.ResourceGroup)(nil), wrapGetResourceGroupErr("test-group", status.Error(codes.DeadlineExceeded, "rpc deadline exceeded"))).
			Once()

		gc, err := controller.tryGetResourceGroupController(ctx, "test-group", false)
		re.NoError(err)
		re.Equal(degradedResourceGroup, gc.getMeta())
		mockProvider.AssertNumberOfCalls(t, "GetResourceGroup", 1)
	})

	t.Run("generic-non-retryable-error-returns-original-error", func(t *testing.T) {
		re := require.New(t)
		mockProvider := newMockResourceGroupProvider()
		controller := newController(t, mockProvider, WithDegradedRUSettings(degradedRUSettings))

		permissionErr := wrapGetResourceGroupErr("test-group", status.Error(codes.PermissionDenied, "permission denied"))
		mockProvider.On("GetResourceGroup", mock.Anything, "test-group", mock.Anything).
			Return((*rmpb.ResourceGroup)(nil), permissionErr).
			Once()

		gc, err := controller.tryGetResourceGroupController(ctx, "test-group", false)
		re.Error(err)
		re.Same(permissionErr, err)
		re.Nil(gc)
		mockProvider.AssertNumberOfCalls(t, "GetResourceGroup", 1)
	})

	t.Run("without-degraded-settings-propagates-transient-error", func(t *testing.T) {
		re := require.New(t)
		mockProvider := newMockResourceGroupProvider()
		controller := newController(t, mockProvider)

		unavailableErr := wrapGetResourceGroupErr("test-group", status.Error(codes.Unavailable, "resource manager unavailable"))
		mockProvider.On("GetResourceGroup", mock.Anything, "test-group", mock.Anything).
			Return((*rmpb.ResourceGroup)(nil), unavailableErr).
			Once()

		gc, err := controller.tryGetResourceGroupController(ctx, "test-group", false)
		re.Error(err)
		re.Same(unavailableErr, err)
		re.Nil(gc)
		mockProvider.AssertNumberOfCalls(t, "GetResourceGroup", 1)
	})
}

func TestGetResourceGroupRuntimeState(t *testing.T) {
	testCases := []struct {
		name                            string
		responseBurstLimit              int64
		sendResponse                    bool
		resourceGroupName               string
		expectOK                        bool
		expectResourceGroupRuntimeState ResourceGroupRuntimeState
	}{
		{
			name:              "unknown before first token response",
			resourceGroupName: "test-group",
		},
		{
			name:               "unlimited response remains burstable",
			responseBurstLimit: -1,
			sendResponse:       true,
			resourceGroupName:  "test-group",
			expectOK:           true,
		},
		{
			name:               "override limited burst from token response",
			responseBurstLimit: 100,
			sendResponse:       true,
			resourceGroupName:  "test-group",
			expectOK:           true,
			expectResourceGroupRuntimeState: ResourceGroupRuntimeState{
				HasLimitedBurst: true,
			},
		},
		{
			name:               "unknown resource group",
			responseBurstLimit: 100,
			sendResponse:       true,
			resourceGroupName:  "unknown",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)

			group := &rmpb.ResourceGroup{
				Name: "test-group",
				Mode: rmpb.GroupMode_RUMode,
				RUSettings: &rmpb.GroupRequestUnitSettings{
					RU: &rmpb.TokenBucket{
						Settings: &rmpb.TokenLimitSettings{
							FillRate:   1000,
							BurstLimit: -1,
						},
					},
				},
			}
			gc, err := newGroupCostController(
				group,
				DefaultRUConfig(),
				make(chan notifyMsg),
				make(chan *groupCostController),
				newRequestSourceMetricsState(group.Name),
			)
			re.NoError(err)

			controller := &ResourceGroupsController{}
			controller.groupsController.Store(group.Name, gc)

			if tc.sendResponse {
				controller.handleTokenBucketResponse([]*rmpb.TokenBucketResponse{
					{
						ResourceGroupName: group.Name,
						GrantedRUTokens: []*rmpb.GrantedRUTokenBucket{
							{
								GrantedTokens: &rmpb.TokenBucket{
									Settings: &rmpb.TokenLimitSettings{
										FillRate:   1000,
										BurstLimit: tc.responseBurstLimit,
									},
									Tokens: 1000,
								},
							},
						},
					},
				})
				// Mirror the main loop, which refreshes the derived state
				// (e.g. burstable) after handling token bucket responses.
				gc.updateRunState()
				gc.updateAvgRequestResourcePerSec()
			}

			state, ok := controller.GetResourceGroupRuntimeState(tc.resourceGroupName)
			re.Equal(tc.expectOK, ok)
			re.Equal(tc.expectResourceGroupRuntimeState, state)
		})
	}
}

func TestTokenBucketsRequestWithKeyspaceID(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

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
		RUVersionPolicy: &RUVersionPolicy{
			Default:   1,
			Overrides: map[uint32]RUVersion{42: 3},
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
	mockProvider.On("Watch", mock.Anything, mock.Anything, mock.Anything).Return(make(chan *metastorage.WatchResponse), nil)

	gc, err := NewResourceGroupController(context.Background(), 1, mockProvider, nil, 42)
	re.NoError(err)
	// The initial load should set ruVersion from the policy.
	re.Equal(int32(3), gc.GetRUVersion())
}

func TestRUVersionWatchViaControllerConfig(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	mockProvider := &MockResourceGroupProvider{}
	mockProvider.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(&meta_storagepb.GetResponse{
		Header: &meta_storagepb.ResponseHeader{Revision: 1},
	}, nil)
	mockProvider.On("LoadResourceGroups", mock.Anything).Return([]*rmpb.ResourceGroup{}, int64(0), nil)

	watchConfigChan := make(chan *metastorage.WatchResponse, 1)
	mockProvider.On("Watch", mock.Anything, pd.ControllerConfigPathPrefixBytes, mock.Anything).Return(watchConfigChan, nil)
	mockProvider.On("Watch", mock.Anything, mock.Anything, mock.Anything).Return(make(chan *metastorage.WatchResponse), nil)

	controller, err := NewResourceGroupController(ctx, 1, mockProvider, nil, 42)
	re.NoError(err)
	controller.Start(ctx)

	// Case 1: Config with RUVersionPolicy containing override for keyspace 42
	configWithPolicy := &Config{
		RUVersionPolicy: &RUVersionPolicy{
			Default:   1,
			Overrides: map[uint32]RUVersion{42: 3},
		},
	}
	val, _ := json.Marshal(configWithPolicy)
	watchConfigChan <- &metastorage.WatchResponse{
		Events: []*meta_storagepb.Event{{
			Type: meta_storagepb.Event_PUT,
			Kv:   &meta_storagepb.KeyValue{Value: val},
		}},
	}
	testutil.Eventually(re, func() bool {
		return controller.GetRUVersion() == 3
	})

	// Case 2: Config without RUVersionPolicy (nil) resets to default
	configNoPolicy := &Config{}
	val, _ = json.Marshal(configNoPolicy)
	watchConfigChan <- &metastorage.WatchResponse{
		Events: []*meta_storagepb.Event{{
			Type: meta_storagepb.Event_PUT,
			Kv:   &meta_storagepb.KeyValue{Value: val},
		}},
	}
	testutil.Eventually(re, func() bool {
		return controller.GetRUVersion() == 1 // Reset to default
	})
}

func matchWatchRevision(revision int64) any {
	return mock.MatchedBy(func(opts []opt.MetaStorageOption) bool {
		var options opt.MetaStorageOp
		for _, apply := range opts {
			apply(&options)
		}
		return options.Revision == revision
	})
}

func newWatchRecoveryProvider() *MockResourceGroupProvider {
	provider := &MockResourceGroupProvider{}
	provider.On("Get", mock.Anything, []byte(controllerConfigPath), mock.Anything).
		Return(&meta_storagepb.GetResponse{Header: &meta_storagepb.ResponseHeader{Revision: 10}}, nil).Once()
	provider.On("AcquireTokenBuckets", mock.Anything, mock.Anything).
		Return([]*rmpb.TokenBucketResponse{}, nil).Maybe()
	return provider
}

func watchRecoveryGroup(name string, fillRate uint64) *rmpb.ResourceGroup {
	return &rmpb.ResourceGroup{
		Name: name,
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{
			Settings: &rmpb.TokenLimitSettings{FillRate: fillRate},
		}},
	}
}

func TestResourceGroupWatchResumesAfterDisconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		ctx := t.Context()
		provider := newWatchRecoveryProvider()
		first, second := make(chan *metastorage.WatchResponse, 1), make(chan *metastorage.WatchResponse, 1)
		prefix := pd.GroupSettingsPathPrefixBytes(constants.NullKeyspaceID)
		provider.On("Watch", mock.Anything, prefix, matchWatchRevision(11)).Return(first, nil).Once()
		provider.On("Watch", mock.Anything, prefix, matchWatchRevision(13)).Return(second, nil).Once()
		provider.On("Watch", mock.Anything, pd.ControllerConfigPathPrefixBytes, matchWatchRevision(11)).
			Return(make(chan *metastorage.WatchResponse), nil).Once()
		group := watchRecoveryGroup("watched", 100)
		provider.On("GetResourceGroup", mock.Anything, group.Name, mock.Anything).Return(group, nil)
		provider.On("GetResourceGroup", mock.Anything, defaultResourceGroupName, mock.Anything).
			Return(watchRecoveryGroup(defaultResourceGroupName, 1000), nil)
		controller, err := NewResourceGroupController(ctx, 1, provider, nil, constants.NullKeyspaceID)
		re.NoError(err)
		_, err = controller.tryGetResourceGroupController(ctx, group.Name, false)
		re.NoError(err)
		controller.Start(ctx)
		defer func() { re.NoError(controller.Stop()) }()
		synctest.Wait()

		updated := watchRecoveryGroup(group.Name, 200)
		value, err := proto.Marshal(updated)
		re.NoError(err)
		first <- &metastorage.WatchResponse{Events: []*meta_storagepb.Event{{
			Type: meta_storagepb.Event_PUT,
			Kv:   &meta_storagepb.KeyValue{ModRevision: 12, Value: value},
		}}}
		close(first)
		synctest.Wait()
		re.Equal(uint64(200), controller.GetActiveResourceGroup(group.Name).GetRUSettings().GetRU().GetSettings().GetFillRate())

		// The retry must include changes made while the stream was disconnected.
		time.Sleep(watchRetryInterval)
		synctest.Wait()
		second <- &metastorage.WatchResponse{Events: []*meta_storagepb.Event{{
			Type: meta_storagepb.Event_DELETE,
			Kv:   &meta_storagepb.KeyValue{Key: append(prefix, []byte("/"+group.Name)...), ModRevision: 14},
			// etcd may have compacted the previous value while retaining this delete.
		}}}
		synctest.Wait()
		gc, ok := controller.loadGroupController(group.Name)
		re.True(ok)
		re.True(gc.tombstone.Load())
		provider.AssertNotCalled(t, "LoadResourceGroups", mock.Anything)
		provider.AssertExpectations(t)
	})
}

func TestResourceGroupWatchReloadsAfterCompaction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		ctx := t.Context()
		provider := newWatchRecoveryProvider()
		first := make(chan *metastorage.WatchResponse, 1)
		prefix := pd.GroupSettingsPathPrefixBytes(constants.NullKeyspaceID)
		provider.On("Watch", mock.Anything, prefix, matchWatchRevision(11)).Return(first, nil).Once()
		provider.On("Watch", mock.Anything, prefix, matchWatchRevision(31)).
			Return(make(chan *metastorage.WatchResponse), nil).Once()
		provider.On("Watch", mock.Anything, pd.ControllerConfigPathPrefixBytes, matchWatchRevision(11)).
			Return(make(chan *metastorage.WatchResponse), nil).Once()
		provider.On("LoadResourceGroups", mock.Anything).
			Return([]*rmpb.ResourceGroup(nil), int64(0), errors.New("snapshot temporarily unavailable")).Once()
		provider.On("LoadResourceGroups", mock.Anything).Return([]*rmpb.ResourceGroup{
			watchRecoveryGroup(defaultResourceGroupName, 1000),
			watchRecoveryGroup("updated", 200),
			watchRecoveryGroup("recreated", 300),
			watchRecoveryGroup("uncached", 400),
		}, int64(30), nil).Once()
		controller, err := NewResourceGroupController(ctx, 1, provider, nil, constants.NullKeyspaceID)
		re.NoError(err)
		for _, name := range []string{defaultResourceGroupName, "updated", "deleted", "recreated"} {
			group := watchRecoveryGroup(name, 100)
			provider.On("GetResourceGroup", mock.Anything, name, mock.Anything).Return(group, nil)
			_, err := controller.tryGetResourceGroupController(ctx, name, false)
			re.NoError(err)
		}
		gc, ok := controller.loadGroupController("recreated")
		re.True(ok)
		gc.tombstone.Store(true)
		controller.Start(ctx)
		defer func() { re.NoError(controller.Stop()) }()
		synctest.Wait()
		first <- &metastorage.WatchResponse{CompactRevision: 20}
		close(first)
		synctest.Wait()

		// Failed snapshots must not advance the cursor or create another watch.
		time.Sleep(watchRetryInterval)
		synctest.Wait()
		provider.AssertNumberOfCalls(t, "Watch", 2)
		time.Sleep(watchRetryInterval)
		synctest.Wait()
		re.Equal(uint64(200), controller.GetActiveResourceGroup("updated").GetRUSettings().GetRU().GetSettings().GetFillRate())
		re.Equal(uint64(300), controller.GetActiveResourceGroup("recreated").GetRUSettings().GetRU().GetSettings().GetFillRate())
		gc, ok = controller.loadGroupController("deleted")
		re.True(ok)
		re.True(gc.tombstone.Load())
		_, ok = controller.loadGroupController("uncached")
		re.False(ok, "snapshot reconciliation must preserve lazy group loading")
		provider.AssertExpectations(t)
	})
}

func TestControllerConfigWatchReloadsAfterCompaction(t *testing.T) {
	for _, removed := range []bool{false, true} {
		t.Run(fmt.Sprintf("removed=%t", removed), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				re := require.New(t)
				ctx := t.Context()
				provider := newWatchRecoveryProvider()
				first := make(chan *metastorage.WatchResponse, 1)
				provider.On("Watch", mock.Anything, pd.ControllerConfigPathPrefixBytes, matchWatchRevision(11)).Return(first, nil).Once()
				provider.On("Watch", mock.Anything, pd.ControllerConfigPathPrefixBytes, matchWatchRevision(31)).
					Return(make(chan *metastorage.WatchResponse), nil).Once()
				provider.On("Get", mock.Anything, []byte(controllerConfigPath), mock.Anything).
					Return((*meta_storagepb.GetResponse)(nil), errors.New("snapshot temporarily unavailable")).Once()
				snapshot := &meta_storagepb.GetResponse{Header: &meta_storagepb.ResponseHeader{Revision: 30}}
				if !removed {
					config := DefaultConfig()
					config.RUVersionPolicy = &RUVersionPolicy{Default: RUVersionV2}
					value, err := json.Marshal(config)
					re.NoError(err)
					snapshot.Kvs = []*meta_storagepb.KeyValue{{Value: value}}
				}
				provider.On("Get", mock.Anything, []byte(controllerConfigPath), mock.Anything).Return(snapshot, nil).Once()
				controller, err := NewResourceGroupController(ctx, 1, provider, nil, 42, EnableSingleGroupByKeyspace())
				re.NoError(err)
				controller.Start(ctx)
				defer func() { re.NoError(controller.Stop()) }()
				synctest.Wait()
				// Make a stale cached version observable even when the key was deleted.
				controller.ruVersion.Store(3)
				first <- &metastorage.WatchResponse{CompactRevision: 20}
				close(first)
				synctest.Wait()
				time.Sleep(watchRetryInterval)
				synctest.Wait()
				re.Equal(RUVersion(3), controller.GetRUVersion())
				provider.AssertNumberOfCalls(t, "Watch", 1)
				time.Sleep(watchRetryInterval)
				synctest.Wait()
				if removed {
					re.Equal(DefaultRUVersion, controller.GetRUVersion())
				} else {
					re.Equal(RUVersionV2, controller.GetRUVersion())
				}
				provider.AssertExpectations(t)
			})
		})
	}
}

func TestControllerStopCancelsWatches(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		ctx := t.Context()
		provider := newWatchRecoveryProvider()
		// Allow repeated config reads so this test only constrains cancellation.
		provider.On("Get", mock.Anything, []byte(controllerConfigPath), mock.Anything).
			Return(&meta_storagepb.GetResponse{Header: &meta_storagepb.ResponseHeader{Revision: 10}}, nil).Maybe()
		var watchContexts []context.Context
		provider.On("Watch", mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				watchContexts = append(watchContexts, args.Get(0).(context.Context))
			}).Return(make(chan *metastorage.WatchResponse), nil).Twice()
		controller, err := NewResourceGroupController(ctx, 1, provider, nil, constants.NullKeyspaceID)
		re.NoError(err)
		controller.Start(ctx)
		synctest.Wait()
		re.Len(watchContexts, 2)
		re.NoError(controller.Stop())
		for _, watchCtx := range watchContexts {
			re.ErrorIs(watchCtx.Err(), context.Canceled)
		}
		re.NoError(ctx.Err(), "the caller's context must remain usable after Stop")
		provider.AssertExpectations(t)
	})
}
