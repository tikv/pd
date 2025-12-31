// Copyright 2025 TiKV Project Authors.
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

package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/client/errs"
)

func createTestGroupCostController(re *require.Assertions) *groupCostController {
	group := &rmpb.ResourceGroup{
		Name:     "test",
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 1,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate: 1000,
				},
			},
		},
		BackgroundSettings: &rmpb.BackgroundSettings{
			JobTypes: []string{"lightning", "br"},
		},
	}
	ch1 := make(chan notifyMsg)
	ch2 := make(chan *groupCostController)
	gc, err := newGroupCostController(group, DefaultRUConfig(), ch1, ch2)
	re.NoError(err)
	return gc
}

func TestGroupControlBurstable(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	args := tokenBucketReconfigureArgs{
		newFillRate: 1000,
		newBurst:    -1,
	}
	gc.run.requestUnitTokens.limiter.Reconfigure(time.Now(), args)
	gc.updateAvgRequestResourcePerSec()
	re.True(gc.burstable.Load())
}

func TestRequestAndResponseConsumption(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	testCases := []struct {
		req  *TestRequestInfo
		resp *TestResponseInfo
	}{
		// Write request
		{
			req: &TestRequestInfo{
				isWrite:     true,
				writeBytes:  100,
				numReplicas: 3,
				accessType:  AccessUnknown,
			},
			resp: &TestResponseInfo{
				readBytes: 100,
				succeed:   true,
			},
		},
		// Write request local AZ
		{
			req: &TestRequestInfo{
				isWrite:     true,
				writeBytes:  100,
				numReplicas: 3,
				accessType:  AccessLocalZone,
			},
			resp: &TestResponseInfo{
				readBytes: 100,
				succeed:   true,
			},
		},
		// Write request cross AZ
		{
			req: &TestRequestInfo{
				isWrite:     true,
				writeBytes:  100,
				numReplicas: 3,
				accessType:  AccessCrossZone,
			},
			resp: &TestResponseInfo{
				readBytes: 100,
				succeed:   true,
			},
		},
		// Read request
		{
			req: &TestRequestInfo{
				isWrite:     false,
				writeBytes:  0,
				numReplicas: 3,
				accessType:  AccessLocalZone,
			},
			resp: &TestResponseInfo{
				readBytes: 100,
				kvCPU:     100 * time.Millisecond,
				succeed:   true,
			},
		},
		// Read request cross AZ
		{
			req: &TestRequestInfo{
				isWrite:     false,
				writeBytes:  0,
				numReplicas: 3,
				accessType:  AccessCrossZone,
			},
			resp: &TestResponseInfo{
				readBytes: 100,
				kvCPU:     100 * time.Millisecond,
				succeed:   true,
			},
		},
	}
	kvCalculator := gc.getKVCalculator()
	for idx, testCase := range testCases {
		caseNum := fmt.Sprintf("case %d", idx)
		consumption, _, _, priority, err := gc.onRequestWaitImpl(context.TODO(), testCase.req)
		re.NoError(err, caseNum)
		re.Equal(priority, gc.meta.Priority)
		expectedConsumption := &rmpb.Consumption{}
		if testCase.req.IsWrite() {
			kvCalculator.calculateWriteCost(expectedConsumption, testCase.req)
			re.Equal(expectedConsumption.WRU, consumption.WRU)
			if testCase.req.AccessLocationType() != AccessUnknown {
				re.Positive(expectedConsumption.WriteCrossAzTrafficBytes, caseNum)
			}
		}
		consumption, err = gc.onResponseImpl(testCase.req, testCase.resp)
		re.NoError(err, caseNum)
		kvCalculator.calculateReadCost(expectedConsumption, testCase.resp)
		kvCalculator.calculateCPUCost(expectedConsumption, testCase.resp)
		calculateCrossAZTraffic(expectedConsumption, testCase.req, testCase.resp)
		re.Equal(expectedConsumption.RRU, consumption.RRU, caseNum)
		re.Equal(expectedConsumption.TotalCpuTimeMs, consumption.TotalCpuTimeMs, caseNum)
		if testCase.req.IsWrite() && testCase.req.AccessLocationType() != AccessUnknown {
			re.Positive(expectedConsumption.WriteCrossAzTrafficBytes, caseNum)
		} else if !testCase.req.IsWrite() && testCase.req.AccessLocationType() == AccessCrossZone {
			re.Positive(expectedConsumption.ReadCrossAzTrafficBytes, caseNum)
		} else {
			re.Equal(expectedConsumption.ReadCrossAzTrafficBytes, uint64(0), caseNum)
			re.Equal(expectedConsumption.WriteCrossAzTrafficBytes, uint64(0), caseNum)
		}
	}
}

func TestOnResponseWaitConsumption(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)

	req := &TestRequestInfo{
		isWrite: false,
	}
	resp := &TestResponseInfo{
		readBytes: 2000 * 64 * 1024, // 2000RU
		succeed:   true,
	}

	consumption, waitTIme, err := gc.onResponseWaitImpl(context.TODO(), req, resp)
	re.NoError(err)
	re.Zero(waitTIme)
	verify := func() {
		expectedConsumption := &rmpb.Consumption{}
		kvCalculator := gc.getKVCalculator()
		kvCalculator.calculateReadCost(expectedConsumption, resp)
		re.Equal(expectedConsumption.RRU, consumption.RRU)
	}
	verify()

	// modify the counter, then on response should has wait time.
	gc.modifyTokenCounter(gc.run.requestUnitTokens, &rmpb.TokenBucket{
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   1000,
			BurstLimit: 1000,
		},
	},
		int64(5*time.Second/time.Millisecond),
	)

	consumption, waitTIme, err = gc.onResponseWaitImpl(context.TODO(), req, resp)
	re.NoError(err)
	re.NotZero(waitTIme)
	verify()
}

func TestResourceGroupThrottledError(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	req := &TestRequestInfo{
		isWrite:    true,
		writeBytes: 10000000,
	}
	// The group is throttled
	_, _, _, _, err := gc.onRequestWaitImpl(context.TODO(), req)
	re.Error(err)
	re.True(errs.ErrClientResourceGroupThrottled.Equal(err))
}
