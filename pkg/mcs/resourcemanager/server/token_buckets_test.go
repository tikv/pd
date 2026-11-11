// Copyright 2022 TiKV Project Authors.
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

package server

import (
	"fmt"
	"math"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
)

const testResourceGroupName = "test"

func TestGroupTokenBucketUpdateAndPatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		tbSetting := &rmpb.TokenBucket{
			Tokens: 200000,
			Settings: &rmpb.TokenLimitSettings{
				FillRate:   2000,
				BurstLimit: 20000000,
			},
		}

		clientUniqueID := uint64(0)
		tb := NewGroupTokenBucket(testResourceGroupName, tbSetting)
		time1 := time.Now()
		tb.request(time1, 0, 0, clientUniqueID)
		re.LessOrEqual(math.Abs(tbSetting.Tokens-tb.Tokens), 1e-7)
		re.Equal(float64(tbSetting.Settings.FillRate), tb.getFillRate())

		tbSetting = &rmpb.TokenBucket{
			Tokens: -100000,
			Settings: &rmpb.TokenLimitSettings{
				FillRate:   1000,
				BurstLimit: 10000000,
			},
		}
		tb.patch(tbSetting)
		synctest.Sleep(10 * time.Millisecond)
		time2 := time.Now()
		tb.request(time2, 0, 0, clientUniqueID)
		re.LessOrEqual(math.Abs(100000-tb.Tokens), time2.Sub(time1).Seconds()*float64(tbSetting.Settings.FillRate)+1e7)
		re.Equal(float64(tbSetting.Settings.FillRate), tb.getFillRate())

		tbSetting = &rmpb.TokenBucket{
			Tokens: 0,
			Settings: &rmpb.TokenLimitSettings{
				FillRate:   2000,
				BurstLimit: -1,
			},
		}
		tb = NewGroupTokenBucket(testResourceGroupName, tbSetting)
		tb.request(time2, 0, 0, clientUniqueID)
		re.LessOrEqual(math.Abs(tbSetting.Tokens), 1e-7)
		time3 := time.Now()
		tb.request(time3, 0, 0, clientUniqueID)
		re.LessOrEqual(math.Abs(tbSetting.Tokens), 1e-7)

		tbSetting = &rmpb.TokenBucket{
			Tokens: 200000,
			Settings: &rmpb.TokenLimitSettings{
				FillRate:   2000,
				BurstLimit: -1,
			},
		}
		tb = NewGroupTokenBucket(testResourceGroupName, tbSetting)
		tb.request(time3, 0, 0, clientUniqueID)
		re.LessOrEqual(math.Abs(tbSetting.Tokens-200000), 1e-7)
		synctest.Sleep(10 * time.Millisecond)
		time4 := time.Now()
		tb.request(time4, 0, 0, clientUniqueID)
		re.LessOrEqual(math.Abs(tbSetting.Tokens-200000), 1e-7)
	})
}

func TestGroupTokenBucketZeroFillRateGuard(t *testing.T) {
	re := require.New(t)
	tbSetting := &rmpb.TokenBucket{
		Tokens: 10000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   2000,
			BurstLimit: 200000,
		},
	}
	gtb := NewGroupTokenBucket(testResourceGroupName, tbSetting)
	gtb.overrideFillRate = 0
	gtb.overrideBurstLimit = 0

	now := time.Now()
	targetPeriodMs := uint64((5 * time.Second) / time.Millisecond)
	for _, elapsed := range []time.Duration{0, 5 * time.Second} {
		for _, clientID := range []uint64{1, 2} {
			tb, trickle := gtb.request(now.Add(elapsed), 1000, targetPeriodMs, clientID)
			re.NotNil(tb)
			re.Equal(0.0, tb.Tokens)
			re.Equal(int64(targetPeriodMs), trickle)
		}
	}
	for _, slot := range gtb.tokenSlots {
		re.False(math.IsInf(slot.curTokenCapacity, 0))
		re.False(math.IsNaN(slot.curTokenCapacity))
	}

	t.Run("assign-basic-fillrate-to-uninitialized-ru-tracker-slot", func(t *testing.T) {
		re := require.New(t)
		gtb := NewGroupTokenBucket(testResourceGroupName, &rmpb.TokenBucket{
			Tokens: 0,
			Settings: &rmpb.TokenLimitSettings{
				FillRate:   100,
				BurstLimit: 100,
			},
		})
		gtb.grt = newGroupRUTracker()

		now := time.Now()
		targetPeriodMs := uint64((5 * time.Second) / time.Millisecond)
		const highDemandClientID uint64 = 1
		const newClientID uint64 = 2

		// 1) Create the first slot.
		_, _ = gtb.request(now, 1, targetPeriodMs, highDemandClientID)
		// 2) Mock the first slot as a high-demand initialized RU tracker.
		rt := gtb.grt.getOrCreateRUTracker(highDemandClientID)
		rt.initialized = true
		rt.lastSampleTime = now
		rt.lastEMA = 100 // > basicFillRate (100 / 2)
		// 3) Request for a new slot whose RU tracker isn't initialized yet.
		tb, trickle := gtb.request(now, 1, targetPeriodMs, newClientID)
		re.NotNil(tb)
		re.Equal(1.0, tb.Tokens)
		re.Equal(int64(targetPeriodMs), trickle)

		// The new slot must get the basic fill rate instead of 0.
		re.Contains(gtb.tokenSlots, newClientID)
		re.Equal(50.0, gtb.tokenSlots[newClientID].fillRate)
	})
}

func TestGroupTokenBucketRequest(t *testing.T) {
	re := require.New(t)
	tbSetting := &rmpb.TokenBucket{
		Tokens: 200000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   2000,
			BurstLimit: 20000000,
		},
	}

	gtb := NewGroupTokenBucket(testResourceGroupName, tbSetting)
	time1 := time.Now()
	clientUniqueID := uint64(0)
	tb, trickle := gtb.request(time1, 190000, uint64(time.Second)*10/uint64(time.Millisecond), clientUniqueID)
	re.LessOrEqual(math.Abs(tb.Tokens-190000), 1e-7)
	re.Zero(trickle)
	// need to lend token
	tb, trickle = gtb.request(time1, 11000, uint64(time.Second)*10/uint64(time.Millisecond), clientUniqueID)
	re.LessOrEqual(math.Abs(tb.Tokens-11000), 1e-7)
	re.Equal(int64(time.Second)*11000./4000./int64(time.Millisecond), trickle)
	tb, trickle = gtb.request(time1, 35000, uint64(time.Second)*10/uint64(time.Millisecond), clientUniqueID)
	re.LessOrEqual(math.Abs(tb.Tokens-35000), 1e-7)
	re.Equal(int64(time.Second)*10/int64(time.Millisecond), trickle)
	tb, trickle = gtb.request(time1, 60000, uint64(time.Second)*10/uint64(time.Millisecond), clientUniqueID)
	re.LessOrEqual(math.Abs(tb.Tokens-22000), 1e-7)
	re.Equal(int64(time.Second)*10/int64(time.Millisecond), trickle)
	// Get reserved 10000 tokens = fillrate(2000) * 10 * defaultReserveRatio(0.5)
	// Max loan tokens is 60000.
	tb, trickle = gtb.request(time1, 3000, uint64(time.Second)*10/uint64(time.Millisecond), clientUniqueID)
	re.LessOrEqual(math.Abs(tb.Tokens-3000), 1e-7)
	re.Equal(int64(time.Second)*10/int64(time.Millisecond), trickle)
	tb, trickle = gtb.request(time1, 12000, uint64(time.Second)*10/uint64(time.Millisecond), clientUniqueID)
	re.LessOrEqual(math.Abs(tb.Tokens-10000), 1e-7)
	re.Equal(int64(time.Second)*10/int64(time.Millisecond), trickle)
	time2 := time1.Add(20 * time.Second)
	tb, trickle = gtb.request(time2, 20000, uint64(time.Second)*10/uint64(time.Millisecond), clientUniqueID)
	re.LessOrEqual(math.Abs(tb.Tokens-20000), 1e-7)
	re.Equal(int64(time.Second)*10/int64(time.Millisecond), trickle)
}

func TestGroupTokenBucketRequestBurstLimit(t *testing.T) {
	re := require.New(t)
	testGroupSetting := func(tbSetting *rmpb.TokenBucket, expectedFillRate, expectedBurstLimit int64) {
		gtb := NewGroupTokenBucket(testResourceGroupName, tbSetting)
		time1 := time.Now()
		clientUniqueID := uint64(0)
		gtb.request(time1, 190000, uint64(time.Second)*10/uint64(time.Millisecond), clientUniqueID)
		re.Contains(gtb.tokenSlots, clientUniqueID)
		// it should not be able to change group settings
		groupSetting := gtb.tokenSlots[clientUniqueID]
		re.Equal(expectedBurstLimit, groupSetting.burstLimit)
		re.Equal(float64(expectedFillRate), groupSetting.fillRate)
		// it should not be able to change gtb settings
		re.Equal(float64(tbSetting.GetSettings().FillRate), gtb.getFillRate())
		re.Equal(tbSetting.GetSettings().BurstLimit, gtb.getBurstLimitSetting())
	}

	// case 1: fillrate = 2000, burstLimit = 2000,0,-1,-2
	testGroupSetting(&rmpb.TokenBucket{
		Tokens: 200000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   2000,
			BurstLimit: 2000,
		},
	}, 2000, 2000)

	testGroupSetting(&rmpb.TokenBucket{
		Tokens: 200000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   2000,
			BurstLimit: 0,
		},
	}, 2000, 0)

	testGroupSetting(&rmpb.TokenBucket{
		Tokens: 200000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   2000,
			BurstLimit: UnlimitedBurstLimit,
		},
	}, 2000, UnlimitedBurstLimit)

	testGroupSetting(&rmpb.TokenBucket{
		Tokens: 200000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   2000,
			BurstLimit: -2,
		},
	}, 2000+defaultModeratedBurstRate, 2000+defaultModeratedBurstRate)

	// case 2: fillrate = unlimited, burstLimit = 2000,0,-1,-2
	testGroupSetting(&rmpb.TokenBucket{
		Tokens: 200000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   UnlimitedRate,
			BurstLimit: 2000,
		},
	}, UnlimitedRate, 2000)

	testGroupSetting(&rmpb.TokenBucket{
		Tokens: 200000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   UnlimitedRate,
			BurstLimit: 0,
		},
	}, UnlimitedRate, 0) // burstLimit = 0 is a special case

	testGroupSetting(&rmpb.TokenBucket{
		Tokens: 200000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   UnlimitedRate,
			BurstLimit: UnlimitedBurstLimit,
		},
	}, UnlimitedRate, UnlimitedBurstLimit)

	testGroupSetting(&rmpb.TokenBucket{
		Tokens: 200000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   UnlimitedRate,
			BurstLimit: -2,
		},
	}, UnlimitedRate, UnlimitedRate)
}

func TestGroupTokenBucketRequestLoop(t *testing.T) {
	re := require.New(t)
	tbSetting := &rmpb.TokenBucket{
		Tokens: 50000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   2000,
			BurstLimit: 200000,
		},
	}

	gtb := NewGroupTokenBucket(testResourceGroupName, tbSetting)
	clientUniqueID := uint64(0)
	initialTime := time.Now()

	// Initialize the token bucket
	gtb.init(initialTime)
	gtb.Tokens = 50000

	const timeIncrement = 5 * time.Second
	const targetPeriod = 5 * time.Second
	const defaultTrickleMs = int64(targetPeriod) / int64(time.Millisecond)

	// Define the test cases in a table
	testCases := []struct {
		requestTokens                 float64
		assignedTokens                float64
		globalBucketTokensAfterAssign float64
		expectedTrickleMs             int64
	}{
		/* requestTokens, assignedTokens, globalBucketTokensAfterAssign, TrickleMs  */
		{50000, 50000, 0, 0},
		{50000, 30000, -20000, defaultTrickleMs},
		{30000, 15000, -25000, defaultTrickleMs},
		{15000, 12500, -27500, defaultTrickleMs},
		{12500, 11250, -28750, defaultTrickleMs},
		{11250, 10625, -29375, defaultTrickleMs},
		// RU_PER_SEC is close to 2000, RU_PER_SEC =  assignedTokens / TrickleMs / 1000.
		{10625, 10312.5, -29687.5, defaultTrickleMs},
		{10312.5, 10156.25, -29843.75, defaultTrickleMs},
		{10156.25, 10078.125, -29921.875, defaultTrickleMs},
		{10078.125, 10039.0625, -29960.9375, defaultTrickleMs},
		{10039.0625, 10019.53125, -29980.46875, defaultTrickleMs},
		{10019.53125, 10009.765625, -29990.234375, defaultTrickleMs},
		{10009.765625, 10004.8828125, -29995.1171875, defaultTrickleMs},
		{10004.8828125, 10002.44140625, -29997.55859375, defaultTrickleMs},
		{10002.44140625, 10001.220703125, -29998.779296875, defaultTrickleMs},
		{10001.220703125, 10000.6103515625, -29999.3896484375, defaultTrickleMs},
		{10000.6103515625, 10000.30517578125, -29999.69482421875, defaultTrickleMs},
		{10000.30517578125, 10000.152587890625, -29999.847412109375, defaultTrickleMs},
		{10000.152587890625, 10000.0762939453125, -29999.9237060546875, defaultTrickleMs},
		{10000.0762939453125, 10000.038146972656, -29999.961853027343, defaultTrickleMs},
	}

	currentTime := initialTime
	for i, tc := range testCases {
		tb, trickle := gtb.request(currentTime, tc.requestTokens, uint64(targetPeriod)/uint64(time.Millisecond), clientUniqueID)
		re.Equalf(tc.globalBucketTokensAfterAssign, gtb.GetTokenBucket().Tokens, "Test case %d failed: expected bucket tokens %f, got %f", i, tc.globalBucketTokensAfterAssign, gtb.GetTokenBucket().Tokens)
		re.LessOrEqualf(math.Abs(tb.Tokens-tc.assignedTokens), 1e-7, "Test case %d failed: expected tokens %f, got %f", i, tc.assignedTokens, tb.Tokens)
		re.Equalf(tc.expectedTrickleMs, trickle, "Test case %d failed: expected trickle %d, got %d", i, tc.expectedTrickleMs, trickle)
		currentTime = currentTime.Add(timeIncrement)
	}
}

func TestBalanceSlotTokensFillRateAllocation(t *testing.T) {
	re := require.New(t)

	now := time.Now()
	testCases := []struct {
		name          string
		fillRate      uint64
		clientDemands map[uint64]float64
		expectedFill  map[uint64]uint64
	}{
		{
			name:     "One low demand with two high demands",
			fillRate: 180,
			clientDemands: map[uint64]float64{
				1: 80,
				2: 30,
				3: 70,
			},
			expectedFill: map[uint64]uint64{
				1: 80,
				2: 30,
				3: 70,
			},
		},
		{
			name:     "One high demand with two low demands",
			fillRate: 180,
			clientDemands: map[uint64]float64{
				1: 15,
				2: 120,
				3: 45,
			},
			expectedFill: map[uint64]uint64{
				1: 15,
				2: 120,
				3: 45,
			},
		},
		{
			name:     "Three low demands with even allocation",
			fillRate: 180,
			clientDemands: map[uint64]float64{
				1: 10,
				2: 10,
				3: 10,
			},
			expectedFill: map[uint64]uint64{
				1: 60,
				2: 60,
				3: 60,
			},
		},
		{
			name:     "Three high demands with even allocation",
			fillRate: 90,
			clientDemands: map[uint64]float64{
				1: 60,
				2: 60,
				3: 60,
			},
			expectedFill: map[uint64]uint64{
				1: 30,
				2: 30,
				3: 30,
			},
		},
	}

	for _, tc := range testCases {
		// Create a new group token bucket.
		gtb := NewGroupTokenBucket(testResourceGroupName, &rmpb.TokenBucket{
			Tokens: 0,
			Settings: &rmpb.TokenLimitSettings{
				FillRate:   tc.fillRate,
				BurstLimit: int64(tc.fillRate),
			},
		})
		gtb.grt = newGroupRUTracker()
		// Mock the RU demand for each client RU tracker.
		for clientID, demand := range tc.clientDemands {
			gtb.tokenSlots[clientID] = newTokenSlot(clientID, now)
			rt := gtb.grt.getOrCreateRUTracker(clientID)
			rt.initialized = true
			rt.lastSampleTime = now
			rt.lastEMA = demand
		}
		// Balance the slots.
		gtb.balanceSlotTokens(now, 1, 1, 0, 5000)
		// Check the fill rate of each slot.
		for clientID, expected := range tc.expectedFill {
			re.InDelta(float64(expected), gtb.tokenSlots[clientID].fillRate, 1e-12, "%s - client %d", tc.name, clientID)
		}
	}
}

func TestFractionalDemandAfterSlotRecreation(t *testing.T) {
	for _, required := range []float64{0.1, 1, 5} {
		t.Run(fmt.Sprintf("request-%g", required), func(t *testing.T) {
			re := require.New(t)
			gtb := NewGroupTokenBucket(testResourceGroupName, &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: 100, BurstLimit: 100},
			})
			gtb.grt = newGroupRUTracker()
			now := time.Now()
			request := func(second int, id uint64, tokens float64) *rmpb.TokenBucket {
				at := now.Add(time.Duration(second) * time.Second)
				// Match AcquireTokenBuckets: sample the request before allocating.
				gtb.grt.sample(id, at, tokens)
				result, _ := gtb.request(at, tokens, 5000, id)
				return result
			}
			request(0, 1, 400)
			request(1, 2, 10)
			request(5, 1, 400)
			request(6, 2, 10)
			request(26, 1, 1680)
			request(26, 2, 0)
			re.NotContains(gtb.tokenSlots, uint64(2))
			re.True(gtb.grt.getOrCreateRUTracker(2).isInitialized())
			re.Zero(gtb.grt.getOrCreateRUTracker(2).getRUPerSec())
			for second := 31; second <= 76; second += 5 {
				request(second, 1, 400)
				result := request(second, 2, required)
				re.NotNil(result)
				re.Greater(result.Tokens, 0.0)
				re.LessOrEqual(result.Tokens, required)
				re.InDelta(100.0, gtb.tokenSlots[1].fillRate+gtb.tokenSlots[2].fillRate, 1e-12)
			}
		})
	}
}

func TestFractionalSlotBorrowing(t *testing.T) {
	for _, capacity := range []float64{0, -10} {
		t.Run(fmt.Sprintf("capacity-%g", capacity), func(t *testing.T) {
			re := require.New(t)
			slot := tokenSlot{fillRate: 0.2, curTokenCapacity: capacity}
			result, trickle := slot.assignSlotTokens(1, 5000)
			re.Greater(result.Tokens, 0.0)
			re.LessOrEqual(result.Tokens, 1.0)
			re.Equal(int64(5000), trickle)
			re.InDelta(capacity-result.Tokens, slot.curTokenCapacity, 1e-12)
		})
	}
}

func TestFractionalAllocationWithinSmallGroupBudget(t *testing.T) {
	re := require.New(t)
	gtb := NewGroupTokenBucket(testResourceGroupName, &rmpb.TokenBucket{
		Settings: &rmpb.TokenLimitSettings{FillRate: 1, BurstLimit: 1},
	})
	gtb.grt = newGroupRUTracker()
	now := time.Now()
	for id := uint64(1); id <= 3; id++ {
		gtb.tokenSlots[id] = newTokenSlot(id, now)
	}
	gtb.balanceSlotTokens(now, 1, 0.1, 0, 5000)
	var sum float64
	for _, slot := range gtb.tokenSlots {
		re.InDelta(1.0/3, slot.fillRate, 1e-12)
		sum += slot.fillRate
		result, _ := slot.assignSlotTokens(0.1, 5000)
		re.InDelta(0.1, result.Tokens, 1e-12)
	}
	re.InDelta(1.0, sum, 1e-12)
}

func TestServiceLimitedClientAllocation(t *testing.T) {
	for _, tc := range []struct {
		name          string
		fillRate      uint64
		burstLimit    int64
		overrideFill  float64
		overrideBurst int64
		wantHot       int64
		wantCold      int64
	}{
		{"unlimited group", UnlimitedRate, -1, -1, 160000, 92500, 67500},
		{"lower service budget", UnlimitedRate, -1, -1, 120000, 72500, 47500},
		{"hot demand above equal share", UnlimitedRate, -1, -1, 80000, 55000, 25000},
		{"both demands above equal share", UnlimitedRate, -1, -1, 40000, 20000, 20000},
		{"moderated group", UnlimitedRate, -2, -1, 160000, 92500, 67500},
		{"finite refill below budget", 100000, -1, -1, 160000, 100000, 60000},
		{"overridden refill", UnlimitedRate, -1, 100000, 100000, 62500, 37500},
		{"fractional overridden refill", UnlimitedRate, -1, 100000.5, 100000, 62500, 37500},
		{"explicit burst", UnlimitedRate, 160000, -1, 120000, 60000, 59999},
		{"rate controlled", UnlimitedRate, 0, -1, 120000, 60000, 59999},
		{"service disabled", UnlimitedRate, -1, -1, -1, -1, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			gtb := NewGroupTokenBucket(testResourceGroupName, &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: tc.fillRate, BurstLimit: tc.burstLimit},
			})
			gtb.overrideFillRate = tc.overrideFill
			gtb.overrideBurstLimit = tc.overrideBurst
			gtb.grt = newGroupRUTracker()
			now := time.Now()
			for i, demand := range []float64{50000, 25000} {
				id := uint64(i + 1)
				gtb.tokenSlots[id] = newTokenSlot(id, now)
				rt := gtb.grt.getOrCreateRUTracker(id)
				rt.initialized = true
				rt.lastSampleTime = now
				rt.lastEMA = demand
			}
			fillRate := gtb.getFillRate()
			const tokensForBalance = 10000.0
			gtb.balanceSlotTokens(now, 1, 1, tokensForBalance, 5000)
			re.Equal(fillRate, gtb.getFillRate())
			re.Equal(tc.fillRate, gtb.Settings.FillRate)
			re.Equal(tc.burstLimit, gtb.Settings.BurstLimit)
			re.Equal(tc.wantHot, gtb.tokenSlots[1].burstLimit)
			re.Equal(tc.wantCold, gtb.tokenSlots[2].burstLimit)
			re.InDelta(fillRate, gtb.tokenSlots[1].fillRate+gtb.tokenSlots[2].fillRate, 1e-6)
			if tc.overrideBurst > 0 {
				for _, slot := range gtb.tokenSlots {
					// Refill and newly assigned tokens must use the same share as capacity.
					ratio := float64(slot.burstLimit) / float64(tc.overrideBurst)
					re.InDelta(ratio, slot.fillRate/fillRate, 1/float64(tc.overrideBurst))
					re.InDelta(ratio, slot.curTokenCapacity/tokensForBalance, 1/float64(tc.overrideBurst))
					re.Equal(slot.curTokenCapacity, slot.lastTokenCapacity)
				}
				re.InDelta(tokensForBalance, gtb.tokenSlots[1].curTokenCapacity+gtb.tokenSlots[2].curTokenCapacity, 1e-7)
				// Removing the other client restores the whole group allocation.
				gtb.Tokens = tokensForBalance
				gtb.balanceSlotTokens(now, 2, 0, 0, 5000)
				re.Len(gtb.tokenSlots, 1)
				re.Equal(fillRate, gtb.tokenSlots[1].fillRate)
				re.Equal(tc.overrideBurst, gtb.tokenSlots[1].burstLimit)
				re.Equal(tokensForBalance, gtb.tokenSlots[1].curTokenCapacity)
			}
		})
	}
}

func TestGroupTokenBucketRequestUnlimitedFillRateLoan(t *testing.T) {
	re := require.New(t)
	// The default group keeps its unlimited fill rate while the Service Limit
	// only overrides the burst limit.
	gtb := NewGroupTokenBucket(testResourceGroupName, &rmpb.TokenBucket{
		Settings: &rmpb.TokenLimitSettings{FillRate: UnlimitedRate, BurstLimit: -1},
	})
	gtb.overrideBurstLimit = 100000
	now := time.Now()
	_, trickle := gtb.request(now, 100, 5000, 1)
	re.Zero(trickle)
	// A zero-token request deletes the slot and the bucket refills to the burst limit.
	gtb.request(now.Add(time.Second), 0, 5000, 99)
	re.Equal(100000.0, gtb.Tokens)
	// A new slot joining a full bucket has no tokens of its own yet, so its
	// grant is a loan. The unlimited fill rate repays it on the next refill,
	// so the grant must not be trickled over the whole target period.
	grant, trickle := gtb.request(now.Add(2*time.Second), 364.3, 5000, 2)
	re.InDelta(364.3, grant.Tokens, 1e-9)
	re.Zero(trickle)
	// The same holds for a slot that is already in debt.
	grant, trickle = gtb.request(now.Add(2*time.Second), 100, 5000, 2)
	re.InDelta(100, grant.Tokens, 1e-9)
	re.Zero(trickle)
	re.Less(gtb.tokenSlots[2].curTokenCapacity, 0.0)
	// The next refill repays the debt, so no runaway loan builds up.
	grant, trickle = gtb.request(now.Add(3*time.Second), 100, 5000, 2)
	re.InDelta(100, grant.Tokens, 1e-9)
	re.Zero(trickle)
	re.Greater(gtb.tokenSlots[2].curTokenCapacity, 0.0)
	// A limited fill rate keeps smoothing such a loan over the target period.
	gtb.overrideFillRate = 50000
	grant, trickle = gtb.request(now.Add(3*time.Second), 100, 5000, 3)
	re.InDelta(100, grant.Tokens, 1e-9)
	re.Equal(int64(5000), trickle)
}

func mockRUTracker(gtb *GroupTokenBucket, clientUniqueID uint64, ruPerSec float64, lastSampleTime time.Time) {
	rt := gtb.grt.getOrCreateRUTracker(clientUniqueID)
	rt.initialized = true
	rt.lastSampleTime = lastSampleTime
	rt.lastEMA = ruPerSec
}

func TestBalanceSlotTokensIdleSlotStartsRequesting(t *testing.T) {
	re := require.New(t)
	gtb := NewGroupTokenBucket(testResourceGroupName, &rmpb.TokenBucket{
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   2000,
			BurstLimit: 2000,
		},
	})
	gtb.grt = newGroupRUTracker()

	now := time.Now()
	targetPeriodMs := uint64((5 * time.Second) / time.Millisecond)
	for clientID := uint64(1); clientID <= 3; clientID++ {
		gtb.tokenSlots[clientID] = newTokenSlot(clientID, now)
		mockRUTracker(gtb, clientID, 1000, now)
	}
	// The idle client has served the group before, so its RU tracker is initialized and its RU/s is 0.
	const idleClientID uint64 = 4
	mockRUTracker(gtb, idleClientID, 0, now.Add(-time.Minute))
	// Sample and request as AcquireTokenBuckets does. The first request after idling is small.
	gtb.grt.sample(idleClientID, now, 10)
	re.Less(gtb.grt.getOrCreateRUTracker(idleClientID).getRUPerSec(), 1.0)
	tb, _ := gtb.request(now, 10, targetPeriodMs, idleClientID)
	re.NotNil(tb)

	for clientID := uint64(1); clientID <= 4; clientID++ {
		re.InDelta(500.0, gtb.tokenSlots[clientID].fillRate, 1e-9, "client %d", clientID)
	}
}

func TestBalanceSlotTokensRequesterFloorTakesFromSlotsAboveBasic(t *testing.T) {
	re := require.New(t)
	gtb := NewGroupTokenBucket(testResourceGroupName, &rmpb.TokenBucket{
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   2000,
			BurstLimit: 2000,
		},
	})
	gtb.grt = newGroupRUTracker()

	now := time.Now()
	for clientID, ruPerSec := range map[uint64]float64{1: 1000, 2: 700, 3: 300} {
		gtb.tokenSlots[clientID] = newTokenSlot(clientID, now)
		mockRUTracker(gtb, clientID, ruPerSec, now)
	}
	const requesterID uint64 = 4
	gtb.tokenSlots[requesterID] = newTokenSlot(requesterID, now.Add(-time.Minute))
	mockRUTracker(gtb, requesterID, 0, now.Add(-time.Minute))
	gtb.balanceSlotTokens(now, requesterID, 10, 0, 5000)

	// Without the floor, every slot gets exactly its RU/s. The requester's 500 comes from the two slots
	// above the basic fill rate, 5/7 of each one's excess, and the slot below it is left alone.
	re.Equal(500.0, gtb.tokenSlots[requesterID].fillRate)
	re.InDelta(500+500*2.0/7, gtb.tokenSlots[1].fillRate, 1e-9)
	re.InDelta(500+200*2.0/7, gtb.tokenSlots[2].fillRate, 1e-9)
	re.InDelta(300.0, gtb.tokenSlots[3].fillRate, 1e-9)
}

func TestBalanceSlotTokensDecaysAndFloorsOnlyWithoutExtraBurst(t *testing.T) {
	for _, tc := range []struct {
		name          string
		burstLimit    int64
		overrideBurst int64
		wantFill      [3]float64
	}{
		// The overdue client is decayed for the 15s beyond a target period, 3 time constants, to 125, the on-time
		// client takes the rest, and the requester is then floored to the basic fill rate of 2000/3.
		{"burst limit at the fill rate", 2000, -1, [3]float64{2000.0 / 3, 3625.0 / 3, 125}},
		// A MODERATED group's effective burst limit is its fill rate raised by 10,000, so the same applies with a
		// basic fill rate of 4,000. The 10,775 left after the RU/s is spread evenly before the floor.
		{"moderated", -2, -1, [3]float64{4000, 12850.0 / 3, 11150.0 / 3}},
		// With a burst limit above the fill rate, or no effective burst limit, nothing is decayed or floored: each
		// slot gets its RU/s, and the 566.7 left is split between the two slots above the basic fill rate.
		{"burst limit above the fill rate", 4000, -1, [3]float64{100, 950, 950}},
		{"zero burst limit override", 2000, 0, [3]float64{100, 950, 950}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			gtb := NewGroupTokenBucket(testResourceGroupName, &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: 2000, BurstLimit: tc.burstLimit},
			})
			gtb.overrideBurstLimit = tc.overrideBurst
			gtb.grt = newGroupRUTracker()
			now := time.Now()
			// The requester last requested a minute ago and has just sampled 100 RU/s. The other two were last
			// sampled at 1,000 RU/s. The on-time client last requested 4s ago, within a target period, so it isn't
			// decayed although its RU tracker was last sampled 20s ago. The overdue client last requested 20s ago.
			for i, seen := range []struct {
				request, sample time.Time
				ruPerSec        float64
			}{
				{now.Add(-time.Minute), now, 100},
				{now.Add(-4 * time.Second), now.Add(-20 * time.Second), 1000},
				{now.Add(-20 * time.Second), now.Add(-20 * time.Second), 1000},
			} {
				clientID := uint64(i + 1)
				gtb.tokenSlots[clientID] = newTokenSlot(clientID, seen.request)
				mockRUTracker(gtb, clientID, seen.ruPerSec, seen.sample)
			}
			gtb.balanceSlotTokens(now, 1, 1, 0, 5000)
			for i, want := range tc.wantFill {
				re.InDelta(want, gtb.tokenSlots[uint64(i+1)].fillRate, 1e-9, "client %d", i+1)
			}
		})
	}
}

func TestBalanceSlotTokensFloorsOnlyIdleRequester(t *testing.T) {
	for _, tc := range []struct {
		name string
		// idleFor is the time since the requester's last request. 0 means it has no slot yet.
		idleFor  time.Duration
		wantFill [3]float64
	}{
		{"active within two target periods", 9 * time.Second, [3]float64{15, 120, 45}},
		{"idle for two target periods", 10 * time.Second, [3]float64{60, 75, 45}},
		{"new", 0, [3]float64{60, 75, 45}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			gtb := NewGroupTokenBucket(testResourceGroupName, &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: 180, BurstLimit: 180},
			})
			gtb.grt = newGroupRUTracker()
			now := time.Now()
			for clientID, ruPerSec := range map[uint64]float64{1: 15, 2: 120, 3: 45} {
				gtb.tokenSlots[clientID] = newTokenSlot(clientID, now)
				mockRUTracker(gtb, clientID, ruPerSec, now)
			}
			// Client 1 requests at 15 RU/s, below the basic fill rate of 60.
			if tc.idleFor > 0 {
				gtb.tokenSlots[1].lastReqTime = now.Add(-tc.idleFor)
			} else {
				delete(gtb.tokenSlots, 1)
			}
			gtb.balanceSlotTokens(now, 1, 1, 0, 5000)
			for i, want := range tc.wantFill {
				re.InDelta(want, gtb.tokenSlots[uint64(i+1)].fillRate, 1e-12, "client %d", i+1)
			}
		})
	}
}

func TestBalanceSlotTokensSteadyDemand(t *testing.T) {
	re := require.New(t)
	targetPeriodMs := uint64((5 * time.Second) / time.Millisecond)
	newBucket := func() *GroupTokenBucket {
		gtb := NewGroupTokenBucket(testResourceGroupName, &rmpb.TokenBucket{
			Settings: &rmpb.TokenLimitSettings{
				FillRate:   1800,
				BurstLimit: 1800,
			},
		})
		gtb.grt = newGroupRUTracker()
		return gtb
	}
	now := time.Now()

	// A single slot always gets the whole group, however stale its RU/s is.
	gtb := newBucket()
	mockRUTracker(gtb, 1, 100, now.Add(-time.Minute))
	_, _ = gtb.request(now, 1, targetPeriodMs, 1)
	re.Equal(1800.0, gtb.tokenSlots[1].fillRate)
	re.Equal(int64(1800), gtb.tokenSlots[1].burstLimit)

	// Three clients asking for twice the basic fill rate in turn, each every 3s, keep an even split.
	gtb = newBucket()
	const requestInterval = 3 * time.Second
	for i := range 30 {
		clientID := uint64(i%3 + 1)
		requiredToken := 1200 * requestInterval.Seconds()
		gtb.grt.sample(clientID, now, requiredToken)
		_, _ = gtb.request(now, requiredToken, targetPeriodMs, clientID)
		now = now.Add(requestInterval / 3)
		if i < 2 {
			continue
		}
		re.Len(gtb.tokenSlots, 3)
		for id, slot := range gtb.tokenSlots {
			re.InDelta(600.0, slot.fillRate, 1e-9, "request %d, client %d", i, id)
		}
	}
}

func TestBalanceSlotTokensRequesterFloorWithinServiceLimit(t *testing.T) {
	for _, tc := range []struct {
		name          string
		fillRate      uint64
		burstLimit    int64
		overrideFill  float64
		overrideBurst int64
		wantFill      float64
		wantBurst     int64
	}{
		// The floor is the equal share of the Service Limit budget, not of the unlimited fill rate.
		{"unlimited group", UnlimitedRate, -1, -1, 100000, UnlimitedRate / 4.0, 25000},
		{"overridden refill", 2000, 2000, 400, 400, 100, 100},
		{"throttled to zero", 2000, 2000, 0, 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			gtb := NewGroupTokenBucket(testResourceGroupName, &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: tc.fillRate, BurstLimit: tc.burstLimit},
			})
			gtb.overrideFillRate = tc.overrideFill
			gtb.overrideBurstLimit = tc.overrideBurst
			gtb.grt = newGroupRUTracker()
			now := time.Now()
			for clientID := uint64(1); clientID <= 3; clientID++ {
				gtb.tokenSlots[clientID] = newTokenSlot(clientID, now)
				mockRUTracker(gtb, clientID, 100000, now)
			}
			const idleClientID uint64 = 4
			gtb.tokenSlots[idleClientID] = newTokenSlot(idleClientID, now.Add(-time.Minute))
			mockRUTracker(gtb, idleClientID, 0, now.Add(-time.Minute))
			gtb.balanceSlotTokens(now, idleClientID, 10, 0, 5000)

			idleSlot := gtb.tokenSlots[idleClientID]
			re.InDelta(tc.wantFill, idleSlot.fillRate, 1e-6)
			re.Equal(tc.wantBurst, idleSlot.burstLimit)
			var fillRateSum float64
			var burstLimitSum int64
			for _, slot := range gtb.tokenSlots {
				fillRateSum += slot.fillRate
				burstLimitSum += slot.burstLimit
			}
			re.InDelta(gtb.getFillRate(), fillRateSum, 1e-6)
			re.Equal(tc.overrideBurst, burstLimitSum)
		})
	}
}

func TestBalanceSlotTokensRequesterInDebtIsNotFloored(t *testing.T) {
	for _, tc := range []struct {
		name      string
		capacity  float64
		wantFill  float64
		wantOther float64
	}{
		// The basic fill rate is 500 RU/s, so a target period of it is 2,500 RU.
		{"debt within a target period", -2500, 500, 500},
		{"debt above a target period", -2501, 1, 500 + 499.0/3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			gtb := NewGroupTokenBucket(testResourceGroupName, &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: 2000, BurstLimit: 2000},
			})
			gtb.grt = newGroupRUTracker()
			now := time.Now()
			for clientID := uint64(1); clientID <= 3; clientID++ {
				gtb.tokenSlots[clientID] = newTokenSlot(clientID, now)
				mockRUTracker(gtb, clientID, 1000, now)
			}
			// The requester was idle for a minute and has just sampled a small request.
			const requesterID uint64 = 4
			requester := newTokenSlot(requesterID, now.Add(-time.Minute))
			requester.curTokenCapacity = tc.capacity
			requester.lastTokenCapacity = tc.capacity
			gtb.tokenSlots[requesterID] = requester
			mockRUTracker(gtb, requesterID, 1, now)
			gtb.balanceSlotTokens(now, requesterID, 10, 0, 5000)
			re.InDelta(tc.wantFill, gtb.tokenSlots[requesterID].fillRate, 1e-9)
			for clientID := uint64(1); clientID <= 3; clientID++ {
				re.InDelta(tc.wantOther, gtb.tokenSlots[clientID].fillRate, 1e-9, "client %d", clientID)
			}
		})
	}
}

func TestBalanceSlotTokensContendedGroupStaysWithinFillRate(t *testing.T) {
	defer log.SetLevel(log.GetLevel())
	log.SetLevel(zapcore.FatalLevel)
	re := require.New(t)
	const (
		fillRate       = 2000
		targetPeriodMs = 5000
		rounds         = 720
		warmUpRounds   = 12
	)
	gtb := NewGroupTokenBucket(testResourceGroupName, &rmpb.TokenBucket{
		Settings: &rmpb.TokenLimitSettings{FillRate: fillRate, BurstLimit: fillRate},
	})
	gtb.grt = newGroupRUTracker()
	// One client wants more than the whole group, and five steady clients want less than half the basic
	// fill rate. Each client asks for a target period of its demand once per target period, sampling and
	// then requesting as AcquireTokenBuckets does.
	demands := []float64{3000, 150, 150, 150, 150, 150}
	period := time.Duration(targetPeriodMs) * time.Millisecond
	start := time.Now()
	var granted, excessAtHalfTime float64
	var warmedUp time.Time
	excess := func(round int) float64 {
		return granted - fillRate*start.Add(time.Duration(round)*period).Sub(warmedUp).Seconds()
	}
	for round := range rounds {
		for i, demand := range demands {
			now := start.Add(time.Duration(round)*period + time.Duration(i)*period/time.Duration(len(demands)))
			clientID := uint64(i + 1)
			requiredToken := demand * period.Seconds()
			gtb.grt.sample(clientID, now, requiredToken)
			tb, _ := gtb.request(now, requiredToken, targetPeriodMs, clientID)
			// The first minute spends the group's initial tokens.
			if round < warmUpRounds {
				continue
			}
			if warmedUp.IsZero() {
				warmedUp = now
			}
			granted += tb.GetTokens()
		}
		if round+1 == rounds/2 {
			excessAtHalfTime = excess(round + 1)
		}
	}
	// Over an hour, the group grants at most its fill rate plus a target period of it, and the excess
	// doesn't grow in the second half hour.
	re.LessOrEqual(excess(rounds), fillRate*period.Seconds())
	re.LessOrEqual(excess(rounds)-excessAtHalfTime, fillRate*period.Seconds()/10)
}

func BenchmarkBalanceSlotTokens(b *testing.B) {
	// Keep the per-slot RU tracker creation logs out of the benchmark output.
	defer log.SetLevel(log.GetLevel())
	log.SetLevel(zapcore.FatalLevel)
	for _, slotNum := range []int{6, 64, 512} {
		for _, state := range []string{"active", "overdue"} {
			b.Run(fmt.Sprintf("%d-slots/%s", slotNum, state), func(b *testing.B) {
				gtb := NewGroupTokenBucket(testResourceGroupName, &rmpb.TokenBucket{
					Settings: &rmpb.TokenLimitSettings{FillRate: 2000, BurstLimit: 2000},
				})
				gtb.grt = newGroupRUTracker()
				now := time.Now()
				// Active slots requested just now, so nothing is decayed or floored. Overdue slots last requested
				// a minute ago, so every slot is decayed and the requester is floored when it is below the basic
				// fill rate.
				lastSeen := now
				if state == "overdue" {
					lastSeen = now.Add(-time.Minute)
				}
				basicFillRate := 2000 / float64(slotNum)
				for i := range slotNum {
					clientID := uint64(i + 1)
					gtb.tokenSlots[clientID] = newTokenSlot(clientID, lastSeen)
					// Mix idle, low and high demand slots with different last sample times.
					mockRUTracker(gtb, clientID, float64(i%3)*basicFillRate, lastSeen.Add(-time.Duration(i%5)*time.Second))
				}
				b.ReportAllocs()
				var i int
				for b.Loop() {
					clientID := uint64(i%slotNum + 1)
					gtb.balanceSlotTokens(now, clientID, 1, 0, 5000)
					gtb.tokenSlots[clientID].lastReqTime = lastSeen
					i++
				}
			})
		}
	}
}
