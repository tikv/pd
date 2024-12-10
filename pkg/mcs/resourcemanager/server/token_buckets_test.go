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
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/stretchr/testify/require"
)

func TestGroupTokenBucketUpdateAndPatch(t *testing.T) {
	re := require.New(t)
	tbSetting := &rmpb.TokenBucket{
		Tokens: 200000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   2000,
			BurstLimit: 20000000,
		},
	}

	clientUniqueID := uint64(0)
	tb := NewGroupTokenBucket(tbSetting)
	time1 := time.Now()
	tb.request(time1, 0, 0, clientUniqueID)
	re.LessOrEqual(math.Abs(tbSetting.Tokens-tb.Tokens), 1e-7)
	re.Equal(tbSetting.Settings.FillRate, tb.Settings.FillRate)

	tbSetting = &rmpb.TokenBucket{
		Tokens: -100000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   1000,
			BurstLimit: 10000000,
		},
	}
	tb.patch(tbSetting)
	time.Sleep(10 * time.Millisecond)
	time2 := time.Now()
	tb.request(time2, 0, 0, clientUniqueID)
	re.LessOrEqual(math.Abs(100000-tb.Tokens), time2.Sub(time1).Seconds()*float64(tbSetting.Settings.FillRate)+1e7)
	re.Equal(tbSetting.Settings.FillRate, tb.Settings.FillRate)

	tbSetting = &rmpb.TokenBucket{
		Tokens: 0,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   2000,
			BurstLimit: -1,
		},
	}
	tb = NewGroupTokenBucket(tbSetting)
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
	tb = NewGroupTokenBucket(tbSetting)
	tb.request(time3, 0, 0, clientUniqueID)
	re.LessOrEqual(math.Abs(tbSetting.Tokens-200000), 1e-7)
	time.Sleep(10 * time.Millisecond)
	time4 := time.Now()
	tb.request(time4, 0, 0, clientUniqueID)
	re.LessOrEqual(math.Abs(tbSetting.Tokens-200000), 1e-7)
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

	gtb := NewGroupTokenBucket(tbSetting)
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

func TestGroupTokenBucketRequestLoop(t *testing.T) {
	re := require.New(t)
	tbSetting := &rmpb.TokenBucket{
		Tokens: 50000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   2000,
			BurstLimit: 200000,
		},
	}

	gtb := NewGroupTokenBucket(tbSetting)
	clientUniqueID := uint64(0)
	initialTime := time.Now()

	// Initialize the token bucket
	gtb.init(initialTime, clientUniqueID)
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
		re.Equal(tc.globalBucketTokensAfterAssign, gtb.GetTokenBucket().Tokens, fmt.Sprintf("Test case %d failed: expected bucket tokens %f, got %f", i, tc.globalBucketTokensAfterAssign, gtb.GetTokenBucket().Tokens))
		re.LessOrEqual(math.Abs(tb.Tokens-tc.assignedTokens), 1e-7, fmt.Sprintf("Test case %d failed: expected tokens %f, got %f", i, tc.assignedTokens, tb.Tokens))
		re.Equal(tc.expectedTrickleMs, trickle, fmt.Sprintf("Test case %d failed: expected trickle %d, got %d", i, tc.expectedTrickleMs, trickle))
		currentTime = currentTime.Add(timeIncrement)
	}
}
