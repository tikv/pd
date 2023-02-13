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
	//   - If b == 0, that means the limiter is unlimited capacity. default use in resource controller (burst with a rate within an unlimited capacity).
	//   - If b < 0, that means the limiter is unlimited capacity and fillrate(r) is ignored, can be seen as r == Inf (burst within a unlimited capacity).
	//   - If b > 0, that means the limiter is limited capacity.
	// MaxTokens limits the number of tokens that can be accumulated
	Settings              *rmpb.TokenLimitSettings `json:"settings,omitempty"`
	GroupTokenBucketState `json:"state,omitempty"`
}

// GroupTokenBucketState is the running state of TokenBucket.
type GroupTokenBucketState struct {
	Tokens      float64    `json:"tokens,omitempty"`
	LastUpdate  *time.Time `json:"last_update,omitempty"`
	Initialized bool       `json:"initialized"`
	// settingChanged is used to avoid that the number of tokens returned is jitter because of changing fill rate.
	settingChanged bool
}

// Clone returns the copy of GroupTokenBucketState
func (s *GroupTokenBucketState) Clone() *GroupTokenBucketState {
	return &GroupTokenBucketState{
		Tokens:      s.Tokens,
		LastUpdate:  s.LastUpdate,
		Initialized: s.Initialized,
	}
}

// NewGroupTokenBucket returns a new GroupTokenBucket
func NewGroupTokenBucket(tokenBucket *rmpb.TokenBucket) GroupTokenBucket {
	if tokenBucket == nil || tokenBucket.Settings == nil {
		return GroupTokenBucket{}
	}
	return GroupTokenBucket{
		Settings: tokenBucket.Settings,
		GroupTokenBucketState: GroupTokenBucketState{
			Tokens: tokenBucket.Tokens,
		},
	}
}

// GetTokenBucket returns the grpc protoc struct of GroupTokenBucket.
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
	if setting := proto.Clone(tb.GetSettings()).(*rmpb.TokenLimitSettings); setting != nil {
		t.Settings = setting
		t.settingChanged = true
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
	}
	// reloan when setting changed
	if t.settingChanged && t.Tokens <= 0 {
		t.Tokens = 0
	}
	t.settingChanged = false
	if t.Settings.BurstLimit != 0 {
		if burst := float64(t.Settings.BurstLimit); t.Tokens > burst {
			t.Tokens = burst
		}
	}

	var res rmpb.TokenBucket
	res.Settings = &rmpb.TokenLimitSettings{BurstLimit: t.Settings.GetBurstLimit()}
	// If BurstLimit is -1, just return.
	if res.Settings.BurstLimit < 0 {
		res.Tokens = neededTokens
		return &res, 0
	}
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

	LoanCoefficient := defaultLoanCoefficient
	// when BurstLimit less or equal FillRate, the server does not accumulate a significant number of tokens.
	// So we don't need to smooth the token allocation speed.
	if t.Settings.BurstLimit > 0 && t.Settings.BurstLimit <= int64(t.Settings.FillRate) {
		LoanCoefficient = 1
	}
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
	p := make([]float64, LoanCoefficient)
	p[0] = float64(LoanCoefficient) * float64(t.Settings.FillRate) * targetPeriodTime.Seconds()
	for i := 1; i < LoanCoefficient; i++ {
		p[i] = float64(LoanCoefficient-i)*float64(t.Settings.FillRate)*targetPeriodTime.Seconds() + p[i-1]
	}
	for i := 0; i < LoanCoefficient && neededTokens > 0 && trickleTime < targetPeriodTime.Seconds(); i++ {
		loan := -t.Tokens
		if loan > p[i] {
			continue
		}
		roundReserveTokens := p[i] - loan
		fillRate := float64(LoanCoefficient-i) * float64(t.Settings.FillRate)
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
