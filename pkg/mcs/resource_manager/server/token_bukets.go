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

	"github.com/gogo/protobuf/proto"
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
// TODO: statistics consumption @JmPotato
type GroupTokenBucket struct {
	*rmpb.TokenBucket `json:"token_bucket,omitempty"`
	// MaxTokens limits the number of tokens that can be accumulated
	MaxTokens float64 `json:"max_tokens,omitempty"`

	Consumption *rmpb.Consumption `json:"consumption,omitempty"`
	LastUpdate  *time.Time        `json:"last_update,omitempty"`
	Initialized bool              `json:"initialized"`
}

// NewGroupTokenBucket returns a new GroupTokenBucket
func NewGroupTokenBucket(tokenBucket *rmpb.TokenBucket) GroupTokenBucket {
	return GroupTokenBucket{
		TokenBucket: tokenBucket,
		MaxTokens:   defaultMaxTokens,
	}
}

// patch patches the token bucket settings.
func (t *GroupTokenBucket) patch(settings *rmpb.TokenBucket) {
	if settings == nil {
		return
	}
	tb := proto.Clone(t.TokenBucket).(*rmpb.TokenBucket)
	if settings.GetSettings() != nil {
		if tb == nil {
			tb = &rmpb.TokenBucket{}
		}
		tb.Settings = settings.GetSettings()
	}

	// the settings in token is delta of the last update and now.
	tb.Tokens += settings.GetTokens()
	t.TokenBucket = tb
}

// init initializes the group token bucket.
func (t *GroupTokenBucket) init(now time.Time) {
	if t.Settings.FillRate == 0 {
		t.Settings.FillRate = defaultRefillRate
	}
	if t.Tokens < defaultInitialTokens {
		t.Tokens = defaultInitialTokens
	}
	// TODO: If we support init or modify MaxTokens in the future, we can move following code.
	if t.Tokens > t.MaxTokens {
		t.MaxTokens = t.Tokens
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
		if t.Tokens > t.MaxTokens {
			t.Tokens = t.MaxTokens
		}
	}

	var res rmpb.TokenBucket
	res.Settings = &rmpb.TokenLimitSettings{}
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
