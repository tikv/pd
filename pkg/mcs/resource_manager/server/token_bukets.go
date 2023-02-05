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
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

const (
	defaultRefillRate    = 10000
	defaultInitialTokens = 10 * 10000
	defaultMaxTokens     = 1e7
)

const (
	defaultReserveRatio    = 0.05
	defaultLoanCoefficient = 2
)

// GroupTokenBucket is a token bucket for a resource group.
// Now we don't save consumption in `GroupTokenBucket`, only statistics it in prometheus.
type GroupTokenBucket struct {
	// Settings is the setting of TokenBucket.
	// BurstLimit is used as below:
	//   - If b == 0, that means the Token Bucket is unlimited burst within token capacity.
	//   - If b < 0, that means the Token Bucket is unlimited burst and capacity is ignored.
	//   - If b > 0, that means the Token Bucket is limited burst. (current not used).
	// MaxTokens limits the number of tokens that can be accumulated
	Settings *rmpb.TokenLimitSettings `json:"settings,omitempty"`
	// State is the running state of TokenBucket.
	GroupTokenBucketState `json:"state,omitempty"`
}

type GroupTokenBucketState struct {
	Tokens      float64    `json:"tokens,omitempty"`
	LastUpdate  *time.Time `json:"last_update,omitempty"`
	Initialized bool       `json:"initialized"`
}

func (s *GroupTokenBucketState) Clone() *GroupTokenBucketState {
	return &GroupTokenBucketState{
		Tokens:      s.Tokens,
		LastUpdate:  s.LastUpdate,
		Initialized: s.Initialized,
	}
}

// NewGroupTokenBucket returns a new GroupTokenBucket
func NewGroupTokenBucket(tokenBucket *rmpb.TokenBucket) GroupTokenBucket {
	if tokenBucket == nil {
		return GroupTokenBucket{
			Settings: &rmpb.TokenLimitSettings{},
		}
	}
	if tokenBucket.Settings.MaxTokens == 0 {
		tokenBucket.Settings.MaxTokens = defaultMaxTokens
	}
	return GroupTokenBucket{
		Settings: tokenBucket.Settings,
		GroupTokenBucketState: GroupTokenBucketState{
			Tokens: tokenBucket.Tokens,
		},
	}
}

func (t *GroupTokenBucket) GetTokenBucket() *rmpb.TokenBucket {
	if t.Settings == nil {
		return nil
	}
	return &rmpb.TokenBucket{
		Settings: t.Settings,
		Tokens:   t.Tokens,
	}
}

// patch patches the token bucket settings.
func (t *GroupTokenBucket) patch(tb *rmpb.TokenBucket) {
	if tb == nil {
		return
	}
	if setting := tb.GetSettings(); setting != nil {
		if t.Settings != nil {
			// If not patch MaxTokens, use past value.
			if setting.MaxTokens == 0 {
				setting.MaxTokens = t.Settings.MaxTokens
			}
		}
		if setting.MaxTokens == 0 {
			setting.MaxTokens = defaultMaxTokens
		}
		t.Settings = setting
	}

	// the settings in token is delta of the last update and now.
	t.Tokens += tb.GetTokens()
}

// init initializes the group token bucket.
func (t *GroupTokenBucket) init(now time.Time) {
	if t.Settings.FillRate == 0 {
		t.Settings.FillRate = defaultRefillRate
	}
	if t.Tokens < defaultInitialTokens {
		t.Tokens = defaultInitialTokens
	}
	t.LastUpdate = &now
	t.Initialized = true
}

// request requests tokens from the group token bucket.
func (t *GroupTokenBucket) request(now time.Time, neededTokens float64, targetPeriodMs uint64) (*rmpb.TokenBucket, int64) {
	if !t.Initialized {
		t.init(now)
	} else {
		delta := now.Sub(*t.LastUpdate)
		if delta > 0 {
			t.Tokens += float64(t.Settings.FillRate) * delta.Seconds()
			t.LastUpdate = &now
		}
		if t.Tokens > t.Settings.MaxTokens {
			t.Tokens = t.Settings.MaxTokens
		}
	}

	var res rmpb.TokenBucket
	res.Settings = &rmpb.TokenLimitSettings{BurstLimit: t.Settings.GetBurstLimit()}
	// FillRate is used for the token server unavailable in abnormal situation.
	if neededTokens <= 0 {
		return &res, 0
	}
	// If the current tokens can directly meet the requirement, returns the need token
	if t.Tokens >= neededTokens {
		t.Tokens -= neededTokens
		// granted the total request tokens
		res.Tokens = neededTokens
		return &res, 0
	}

	// Firstly allocate the remaining tokens
	var grantedTokens float64
	hasRemaining := false
	if t.Tokens > 0 {
		grantedTokens = t.Tokens
		neededTokens -= grantedTokens
		t.Tokens = 0
		hasRemaining = true
	}

	var targetPeriodTime = time.Duration(targetPeriodMs) * time.Millisecond
	var trickleTime = 0.

	// When there are loan, the allotment will match the fill rate.
	// We will have k threshold, beyond which the token allocation will be a minimum.
	// The threshold unit is `fill rate * target period`.
	//               |
	// k*fill_rate   |* * * * * *     *
	//               |                        *
	//     ***       |                                 *
	//               |                                           *
	//               |                                                     *
	//   fill_rate   |                                                                 *
	// reserve_rate  |                                                                              *
	//               |
	// grant_rate 0  ------------------------------------------------------------------------------------
	//         loan      ***    k*period_token    (k+k-1)*period_token    ***      (k+k+1...+1)*period_token
	p := make([]float64, defaultLoanCoefficient)
	p[0] = float64(defaultLoanCoefficient) * float64(t.Settings.FillRate) * targetPeriodTime.Seconds()
	for i := 1; i < defaultLoanCoefficient; i++ {
		p[i] = float64(defaultLoanCoefficient-i)*float64(t.Settings.FillRate)*targetPeriodTime.Seconds() + p[i-1]
	}
	for i := 0; i < defaultLoanCoefficient && neededTokens > 0 && trickleTime < targetPeriodTime.Seconds(); i++ {
		loan := -t.Tokens
		if loan > p[i] {
			continue
		}
		roundReserveTokens := p[i] - loan
		fillRate := float64(defaultLoanCoefficient-i) * float64(t.Settings.FillRate)
		if roundReserveTokens > neededTokens {
			t.Tokens -= neededTokens
			grantedTokens += neededTokens
			trickleTime += grantedTokens / fillRate
			neededTokens = 0
		} else {
			roundReserveTime := roundReserveTokens / fillRate
			if roundReserveTime+trickleTime >= targetPeriodTime.Seconds() {
				roundTokens := (targetPeriodTime.Seconds() - trickleTime) * fillRate
				neededTokens -= roundTokens
				t.Tokens -= roundTokens
				grantedTokens += roundTokens
				trickleTime = targetPeriodTime.Seconds()
			} else {
				grantedTokens += roundReserveTokens
				neededTokens -= roundReserveTokens
				t.Tokens -= roundReserveTokens
				trickleTime += roundReserveTime
			}
		}
	}
	if grantedTokens < defaultReserveRatio*float64(t.Settings.FillRate)*targetPeriodTime.Seconds() {
		t.Tokens -= defaultReserveRatio*float64(t.Settings.FillRate)*targetPeriodTime.Seconds() - grantedTokens
		grantedTokens = defaultReserveRatio * float64(t.Settings.FillRate) * targetPeriodTime.Seconds()
	}
	res.Tokens = grantedTokens
	var trickleDuration time.Duration
	// can't directly treat targetPeriodTime as trickleTime when there is a token remaining.
	// If treat, client consumption will be slowed down (actually cloud be increased).
	if hasRemaining {
		trickleDuration = time.Duration(math.Min(trickleTime, targetPeriodTime.Seconds()) * float64(time.Second))
	} else {
		trickleDuration = targetPeriodTime
	}
	return &res, trickleDuration.Milliseconds()
}
