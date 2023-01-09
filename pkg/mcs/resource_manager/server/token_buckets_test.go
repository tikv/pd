// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,g
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
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

	tb := NewGroupTokenBucket(tbSetting)
	time1 := time.Now()
	tb.request(time1, 0, 0)
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

	time2 := time.Now()
	tb.request(time2, 0, 0)
	re.LessOrEqual(math.Abs(100000-tb.Tokens), time2.Sub(time1).Seconds()*float64(tbSetting.Settings.FillRate)+1e7)
	re.Equal(tbSetting.Settings.FillRate, tb.Settings.FillRate)
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
	tb, trickle := gtb.request(time1, 100000, uint64(time.Second)*10/uint64(time.Millisecond))
	re.LessOrEqual(math.Abs(tb.Tokens-100000), 1e-7)
	re.Equal(trickle, int64(0))
	tb, trickle = gtb.request(time1, 101000, uint64(time.Second)*10/uint64(time.Millisecond))
	re.LessOrEqual(math.Abs(tb.Tokens-101000), 1e-7)
	re.Equal(trickle, int64(time.Second)*10/int64(time.Millisecond))
	tb, trickle = gtb.request(time1, 17000, uint64(time.Second)*10/uint64(time.Millisecond))
	re.LessOrEqual(math.Abs(tb.Tokens-17000), 1e-7)
	re.Equal(trickle, int64(time.Second)*10/int64(time.Millisecond))
	tb, trickle = gtb.request(time1, 4000, uint64(time.Second)*10/uint64(time.Millisecond))
	re.LessOrEqual(math.Abs(tb.Tokens-4000), 1e-7)
	re.Equal(trickle, int64(time.Second)*10/int64(time.Millisecond))
	tb, trickle = gtb.request(time1, 19000, uint64(time.Second)*10/uint64(time.Millisecond))
	re.LessOrEqual(math.Abs(tb.Tokens-18000), 1e-7)
	re.Equal(trickle, int64(time.Second)*10/int64(time.Millisecond))
	time2 := time1.Add(10 * time.Second)
	tb, trickle = gtb.request(time2, 20000, uint64(time.Second)*10/uint64(time.Millisecond))
	re.LessOrEqual(math.Abs(tb.Tokens-20000), 1e-7)
	re.Equal(trickle, int64(time.Second)*10/int64(time.Millisecond))
}
