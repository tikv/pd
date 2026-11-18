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
	"cmp"
	"fmt"
	"maps"
	"math"
	"strings"
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

// mockRUTracker sets a client's RU tracker as if it had last been sampled at ruPerSec at lastSampleTime.
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
	const idleClientID uint64 = 4
	mockRUTracker(gtb, idleClientID, 0, now.Add(-time.Minute))
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

	// The floor takes 500/700 of each donor's excess; the 300 RU/s slot keeps its share.
	re.Equal(500.0, gtb.tokenSlots[requesterID].fillRate)
	re.InDelta(500+500*2.0/7, gtb.tokenSlots[1].fillRate, 1e-9)
	re.InDelta(500+200*2.0/7, gtb.tokenSlots[2].fillRate, 1e-9)
	re.InDelta(300.0, gtb.tokenSlots[3].fillRate, 1e-9)
}

func TestBalanceSlotTokensDecaysAndFloorsOnlyWithoutExtraBurst(t *testing.T) {
	// Beyond the 6.25 s grace, 13.75 s is 2.75 EMA half-lives.
	overdue := 1000 * math.Pow(2, -2.75)
	moderatedExtra := (12000 - 1100 - overdue) / 3
	moderatedOnTime := 1000 + moderatedExtra - (4000 - 100 - moderatedExtra)
	for _, tc := range []struct {
		name          string
		burstLimit    int64
		overrideBurst int64
		wantFill      [3]float64
	}{
		{"burst limit at the fill rate", 2000, -1, [3]float64{2000.0 / 3, 2000 - 2000.0/3 - overdue, overdue}},
		{"moderated", -2, -1, [3]float64{4000, moderatedOnTime, overdue + moderatedExtra}},
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
			for i, seen := range []struct {
				request, sample time.Time
				ruPerSec        float64
			}{
				{now.Add(-time.Minute), now, 100},
				{now.Add(-6 * time.Second), now.Add(-20 * time.Second), 1000},
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

func TestBalanceSlotTokensOverdueUninitializedSlot(t *testing.T) {
	for _, tc := range []struct {
		name       string
		burstLimit int64
		lastReq    time.Duration
		slotPeriod time.Duration
		wantFill   [3]float64
	}{
		{"overdue", 2000, 20 * time.Second, 0, [3]float64{1000, 1000, 0}},
		{"within a target period and a quarter", 2000, 6 * time.Second, 0, [3]float64{2000.0 / 3, 2000.0 / 3, 2000.0 / 3}},
		{"burst limit above the fill rate", 4000, 20 * time.Second, 0, [3]float64{2000.0 / 3, 2000.0 / 3, 2000.0 / 3}},
		{"longer target period", 2000, 20 * time.Second, 20 * time.Second, [3]float64{2000.0 / 3, 2000.0 / 3, 2000.0 / 3}},
		{"explicit requester period", 2000, 20 * time.Second, 5 * time.Second, [3]float64{1000, 1000, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			gtb := NewGroupTokenBucket(testResourceGroupName, &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: 2000, BurstLimit: tc.burstLimit},
			})
			gtb.grt = newGroupRUTracker()
			now := time.Now()
			for clientID := uint64(1); clientID <= 2; clientID++ {
				gtb.tokenSlots[clientID] = newTokenSlot(clientID, now)
				mockRUTracker(gtb, clientID, 1000, now)
			}
			const quietClientID uint64 = 3
			gtb.tokenSlots[quietClientID] = newTokenSlot(quietClientID, now.Add(-tc.lastReq))
			gtb.tokenSlots[quietClientID].targetPeriod = tc.slotPeriod
			gtb.grt.sample(quietClientID, now.Add(-tc.lastReq), 100)
			re.False(gtb.grt.getOrCreateRUTracker(quietClientID).isInitialized())
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
		// Zero means the requester has no slot yet.
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

	gtb := newBucket()
	mockRUTracker(gtb, 1, 100, now.Add(-time.Minute))
	_, _ = gtb.request(now, 1, targetPeriodMs, 1)
	re.Equal(1800.0, gtb.tokenSlots[1].fillRate)
	re.Equal(int64(1800), gtb.tokenSlots[1].burstLimit)

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
			// Exclude the initial token allowance.
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
	re.LessOrEqual(excess(rounds), fillRate*period.Seconds())
	re.LessOrEqual(excess(rounds)-excessAtHalfTime, fillRate*period.Seconds()/10)
}

// A negative amount skips a request; targetPeriod defaults to 5 s.
type sequencedClient struct {
	id           uint64
	offset       time.Duration
	period       time.Duration
	targetPeriod time.Duration
	amount       func(k int) float64
}

type sequenceResult struct {
	granted     []float64
	byClient    []map[uint64]float64
	groupTokens []float64
	gtb         *GroupTokenBucket
}

// The sequence starts at the real clock because slot expiry uses time.Since.
func runRequestSequence(burstLimit int64, clients []sequencedClient, checkpoints ...time.Duration) sequenceResult {
	gtb := NewGroupTokenBucket(testResourceGroupName, &rmpb.TokenBucket{
		Settings: &rmpb.TokenLimitSettings{FillRate: 2000, BurstLimit: burstLimit},
	})
	gtb.grt = newGroupRUTracker()
	start := time.Now()
	var (
		res      = sequenceResult{gtb: gtb}
		total    float64
		byClient = make(map[uint64]float64)
	)
	for sec := time.Duration(0); sec <= checkpoints[len(checkpoints)-1]; sec += time.Second {
		now := start.Add(sec)
		for _, c := range clients {
			if sec < c.offset || (sec-c.offset)%c.period != 0 {
				continue
			}
			requiredToken := c.amount(int((sec - c.offset) / c.period))
			targetPeriod := cmp.Or(c.targetPeriod, 5*time.Second)
			if requiredToken < 0 {
				continue
			}
			gtb.grt.sample(c.id, now, requiredToken)
			if tb, _ := gtb.request(now, requiredToken, uint64(targetPeriod.Milliseconds()), c.id); tb != nil {
				total += tb.GetTokens()
				byClient[c.id] += tb.GetTokens()
			}
		}
		if len(res.granted) < len(checkpoints) && sec == checkpoints[len(res.granted)] {
			res.granted = append(res.granted, total)
			res.byClient = append(res.byClient, maps.Clone(byClient))
			res.groupTokens = append(res.groupTokens, gtb.Tokens)
		}
	}
	return res
}

func TestBalanceSlotTokensRecreatedSlotsStayWithinFillRate(t *testing.T) {
	defer log.SetLevel(log.GetLevel())
	log.SetLevel(zapcore.FatalLevel)
	re := require.New(t)
	const (
		fillRate = 2000.0
		period   = 5 * time.Second
	)
	clients := []sequencedClient{{id: 0, period: period, amount: func(int) float64 { return 15000 }}}
	for i := 1; i <= 9; i++ {
		clients = append(clients, sequencedClient{
			id: uint64(i), offset: time.Duration(i*5/10) * time.Second, period: period,
			amount: func(k int) float64 {
				if k%2 == 0 {
					return 2500
				}
				return 0
			},
		})
	}
	checkpoints := []time.Duration{600 * time.Second, 1800 * time.Second}
	res := runRequestSequence(int64(fillRate), clients, checkpoints...)
	var excess []float64
	for i, checkpoint := range checkpoints {
		excess = append(excess, res.granted[i]-fillRate*checkpoint.Seconds())
		re.GreaterOrEqual(res.groupTokens[i], -2*fillRate*period.Seconds(), "group tokens at %v", checkpoint)
	}
	re.LessOrEqual(excess[1]-excess[0], fillRate*period.Seconds()/10)
}

func TestBalanceSlotTokensSmallRepeatedRequesterKeepsOthersCapacity(t *testing.T) {
	defer log.SetLevel(log.GetLevel())
	log.SetLevel(zapcore.FatalLevel)
	for _, tc := range []struct {
		name   string
		amount float64
	}{
		{"below the fill rate", 9500},
		{"above the fill rate", 15000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			busy := sequencedClient{id: 0, period: 5 * time.Second, amount: func(int) float64 { return tc.amount }}
			small := sequencedClient{id: 1, offset: 4 * time.Second, period: 10 * time.Second, amount: func(int) float64 { return 10 }}
			checkpoints := []time.Duration{600 * time.Second, 1800 * time.Second}
			alone := runRequestSequence(2000, []sequencedClient{busy}, checkpoints...)
			shared := runRequestSequence(2000, []sequencedClient{busy, small}, checkpoints...)
			re.InEpsilon(alone.granted[1]-alone.granted[0], shared.granted[1]-shared.granted[0], 0.005)
		})
	}
}

func TestBalanceSlotTokensMixedTargetPeriods(t *testing.T) {
	defer log.SetLevel(log.GetLevel())
	log.SetLevel(zapcore.FatalLevel)
	re := require.New(t)
	const fillRate = 2000.0
	slow := sequencedClient{
		id: 1, period: 20 * time.Second, targetPeriod: 20 * time.Second,
		amount: func(int) float64 { return 38000 },
	}
	fast := sequencedClient{
		id: 2, offset: 4 * time.Second, period: 5 * time.Second, targetPeriod: 5 * time.Second,
		amount: func(int) float64 { return 50 },
	}
	checkpoints := []time.Duration{600 * time.Second, 3600 * time.Second}
	alone := runRequestSequence(2000, []sequencedClient{slow}, checkpoints...)
	shared := runRequestSequence(2000, []sequencedClient{slow, fast}, checkpoints...)
	window := (checkpoints[1] - checkpoints[0]).Seconds()
	aloneRate := (alone.byClient[1][slow.id] - alone.byClient[0][slow.id]) / window
	sharedRate := (shared.byClient[1][slow.id] - shared.byClient[0][slow.id]) / window
	re.InDelta(1900, aloneRate, 1)
	// Allow the loan of one target period at the window boundary.
	re.GreaterOrEqual(sharedRate, 1900-fillRate*20/window)
}

func TestBalanceSlotTokensModeratedFloorAfterGrants(t *testing.T) {
	defer log.SetLevel(log.GetLevel())
	log.SetLevel(zapcore.FatalLevel)
	busy := sequencedClient{id: 1, period: 5 * time.Second, amount: func(int) float64 { return 150000 }}
	returning := sequencedClient{id: 2, period: 5 * time.Second, amount: func(k int) float64 {
		switch k {
		case 0:
			return 1
		case 1:
			return 0
		case 12:
			return 10
		}
		return -1
	}}
	for name, clients := range map[string][]sequencedClient{
		"busy first":      {busy, returning},
		"returning first": {returning, busy},
	} {
		t.Run(name, func(t *testing.T) {
			re := require.New(t)
			res := runRequestSequence(-2, clients, 60*time.Second)
			gtb := res.gtb
			fillRate, _ := gtb.getFillRateAndBurstLimit()
			re.Less(gtb.Tokens, -fillRate*5)
			re.LessOrEqual(gtb.moderatedDebt, fillRate*5)
			re.InDelta(11.0, res.byClient[0][returning.id], 1e-9)
			if name == "busy first" {
				// A later busy-client request would rebalance this share.
				re.InDelta(fillRate/2, gtb.tokenSlots[returning.id].fillRate, 1e-6)
			}
		})
	}
}

func TestModeratedDebtSurvivesSlotRemoval(t *testing.T) {
	re := require.New(t)
	gtb := NewGroupTokenBucket(testResourceGroupName, &rmpb.TokenBucket{
		Settings: &rmpb.TokenLimitSettings{FillRate: 2000, BurstLimit: -2},
	})
	now := time.Now()
	first, _ := gtb.request(now, 60000, 5000, 1)
	re.Equal(60000.0, first.Tokens)
	gtb.request(now, 0, 5000, 1)
	re.Empty(gtb.tokenSlots)
	re.Equal(first.Tokens, gtb.moderatedDebt)
	second, _ := gtb.request(now, 10, 5000, 1)
	re.Equal(60000.0+second.Tokens, gtb.moderatedDebt)
	re.False(gtb.debtWithin(60000))
	gtb.resetLoan()
	re.False(gtb.debtWithin(60000))
	gtb.settleModeratedDebt(now.Add(time.Second))
	re.True(gtb.debtWithin(60000))
	re.Equal(48000.0+second.Tokens, gtb.moderatedDebt)
	gtb.settleModeratedDebt(now)
	gtb.settleModeratedDebt(now.Add(time.Second))
	re.Equal(48000.0+second.Tokens, gtb.moderatedDebt)
	gtb.settleModeratedDebt(now.Add(time.Hour))
	re.Zero(gtb.moderatedDebt)
	third, _ := gtb.request(now.Add(time.Hour), 10, 5000, 1)
	re.Equal(third.Tokens, gtb.moderatedDebt, "idle time must not bank credit")
}

func TestModeratedDebtGatesReturningFloor(t *testing.T) {
	re := require.New(t)
	gtb := NewGroupTokenBucket(testResourceGroupName, &rmpb.TokenBucket{
		Settings: &rmpb.TokenLimitSettings{FillRate: 2000, BurstLimit: -2},
	})
	gtb.grt = newGroupRUTracker()
	now := time.Now()
	gtb.request(now, 60000, 5000, 1)
	gtb.request(now, 10, 5000, 1)
	mockRUTracker(gtb, 1, 12000, now)
	mockRUTracker(gtb, 2, 1, now)
	gtb.request(now, 10, 5000, 2)
	re.InDelta(1.0, gtb.tokenSlots[2].fillRate, 1e-9, "outstanding grants must close the floor")
	gtb.request(now, 0, 5000, 2)
	gtb.request(now.Add(time.Second), 10, 5000, 2)
	re.InDelta(6000.0, gtb.tokenSlots[2].fillRate, 1e-9, "repayment must reopen the floor")
}

func TestModeratedDebtSettingsAndOverrides(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		rg := FromProtoResourceGroup(&rmpb.ResourceGroup{
			Name: testResourceGroupName, Mode: rmpb.GroupMode_RUMode,
			RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: 2000, BurstLimit: -2},
			}},
		})
		gtb := rg.RUSettings.RU
		gtb.request(time.Now(), 50000, 5000, 1)
		time.Sleep(time.Second)
		gtb.applySettings(&rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 3000, BurstLimit: -2}})
		re.Equal(38000.0, gtb.moderatedDebt, "settle the old rate before replacing settings")
		time.Sleep(time.Second)
		gtb.request(time.Now(), 10, 5000, 1)
		re.Equal(25010.0, gtb.moderatedDebt, "resetLoan must preserve issuance history")
		time.Sleep(time.Second)
		rg.overrideFillRateAndBurstLimit(500, 500)
		re.Equal(12010.0, gtb.moderatedDebt)
		time.Sleep(time.Second)
		grant, _ := gtb.request(time.Now(), 10, 5000, 1)
		re.Equal(10.0, grant.Tokens)
		re.Equal(11520.0, gtb.moderatedDebt, "track grants during the temporary LIMITED interval")
		time.Sleep(time.Second)
		rg.overrideFillRateAndBurstLimit(-1, -1)
		re.Equal(11020.0, gtb.moderatedDebt)
		time.Sleep(time.Second)
		gtb.request(time.Now(), 0, 5000, 1)
		re.Zero(gtb.moderatedDebt)

		gtb.request(time.Now(), 1000, 5000, 1)
		gtb.patch(&rmpb.TokenBucket{Tokens: 100000})
		re.Equal(1000.0, gtb.moderatedDebt, "a token patch is not repayment")
		gtb.applySettings(&rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 3000, BurstLimit: -1}})
		re.Zero(gtb.moderatedDebt, "an explicit mode switch ends the accounting epoch")
		re.True(gtb.moderatedAt.IsZero())
	})
}

func TestModeratedDebtIndividualOverrides(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		rg := FromProtoResourceGroup(&rmpb.ResourceGroup{
			Name: testResourceGroupName, Mode: rmpb.GroupMode_RUMode,
			RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: 2000, BurstLimit: -2},
			}},
		})
		gtb := rg.RUSettings.RU
		gtb.request(time.Now(), 50000, 5000, 1)
		time.Sleep(time.Second)
		rg.overrideFillRate(1000)
		re.Equal(38000.0, gtb.moderatedDebt)
		time.Sleep(time.Second)
		rg.overrideBurstLimit(1000)
		re.Equal(27000.0, gtb.moderatedDebt)
		time.Sleep(time.Second)
		rg.overrideBurstLimit(-1)
		re.Equal(26000.0, gtb.moderatedDebt)
		time.Sleep(time.Second)
		rg.overrideFillRate(-1)
		re.Equal(15000.0, gtb.moderatedDebt)
	})
}

func TestModeratedDebtAfterServiceLimit(t *testing.T) {
	for _, override := range []bool{false, true} {
		t.Run(fmt.Sprintf("override=%t", override), func(t *testing.T) {
			re := require.New(t)
			rg := FromProtoResourceGroup(&rmpb.ResourceGroup{
				Name: testResourceGroupName, Mode: rmpb.GroupMode_RUMode,
				RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{
					Settings: &rmpb.TokenLimitSettings{FillRate: 2000, BurstLimit: -2},
				}},
			})
			if override {
				rg.overrideFillRateAndBurstLimit(1000, 1000)
			}
			now := time.Now()
			sl := &serviceLimiter{ServiceLimit: 100, AvailableTokens: 250, LastUpdate: now}
			res := rg.RequestRU(now, 50000, 5000, 1, newGroupRUTracker(), sl)
			re.Equal(250.0, res.GrantedTokens.Tokens)
			gtb := rg.RUSettings.RU
			re.Equal(250.0, gtb.moderatedDebt)
			res = rg.RequestRU(now, 100, 5000, 1, gtb.grt, sl)
			re.Zero(res.GrantedTokens.Tokens)
			re.Equal(250.0, gtb.moderatedDebt)
		})
	}
}

func TestModeratedDebtCloneAndState(t *testing.T) {
	re := require.New(t)
	gtb := NewGroupTokenBucket(testResourceGroupName, &rmpb.TokenBucket{
		Settings: &rmpb.TokenLimitSettings{BurstLimit: -2},
	})
	now := time.Now()
	gtb.request(now, 50000, 5000, 1)
	re.Equal(uint64(defaultRefillRate), gtb.Settings.FillRate)
	clone := gtb.clone()
	re.Equal(50000.0, clone.moderatedDebt)
	re.Equal(now, clone.moderatedAt)
	restored := NewGroupTokenBucket(testResourceGroupName, gtb.GetTokenBucket())
	restored.setState(gtb.GroupTokenBucketState.clone())
	re.Equal(50000.0, restored.moderatedDebt)
	re.Equal(now, restored.moderatedAt)
	clone.settleModeratedDebt(now.Add(time.Second))
	re.Equal(30000.0, clone.moderatedDebt)
	re.Equal(50000.0, gtb.moderatedDebt)
}

func BenchmarkBalanceSlotTokens(b *testing.B) {
	defer log.SetLevel(log.GetLevel())
	log.SetLevel(zapcore.FatalLevel)
	for _, slotNum := range []int{6, 64, 512} {
		for _, state := range []string{"active", "overdue", "idle-requester", "idle-requester-refill"} {
			b.Run(fmt.Sprintf("%d-slots/%s", slotNum, state), func(b *testing.B) {
				gtb := NewGroupTokenBucket(testResourceGroupName, &rmpb.TokenBucket{
					Settings: &rmpb.TokenLimitSettings{FillRate: 2000, BurstLimit: 2000},
				})
				gtb.grt = newGroupRUTracker()
				now := time.Now()
				lastSeen := now
				if state == "overdue" {
					lastSeen = now.Add(-time.Minute)
				}
				idleRequester := strings.HasPrefix(state, "idle-requester")
				var tokensForBalance float64
				if state == "idle-requester-refill" {
					tokensForBalance = 100
				}
				basicFillRate := 2000 / float64(slotNum)
				for i := range slotNum {
					clientID := uint64(i + 1)
					gtb.tokenSlots[clientID] = newTokenSlot(clientID, lastSeen)
					mockRUTracker(gtb, clientID, float64(i%3)*basicFillRate, lastSeen.Add(-time.Duration(i%5)*time.Second))
				}
				b.ReportAllocs()
				var i int
				for b.Loop() {
					clientID := uint64(i%slotNum + 1)
					if idleRequester {
						clientID = 1
						gtb.tokenSlots[clientID].lastReqTime = now.Add(-time.Minute)
					}
					// Keep unused benchmark refill from closing the floor's debt guard.
					gtb.Tokens, gtb.reservedBurstTokens = 0, 0
					gtb.balanceSlotTokens(now, clientID, 1, tokensForBalance, 5000)
					gtb.tokenSlots[clientID].lastReqTime = lastSeen
					i++
				}
			})
		}
	}
}
