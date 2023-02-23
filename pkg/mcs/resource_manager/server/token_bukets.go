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
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/gogo/protobuf/proto"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
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

func (gtb *GroupTokenBucket) setState(state *GroupTokenBucketState) {
	gtb.mu.Lock()
	defer gtb.mu.Unlock()

	gtb.Tokens = state.Tokens
	gtb.LastUpdate = state.LastUpdate
	gtb.Initialized = state.Initialized
	log.Info("update group token bucket state", zap.Any("state", state))
}

// TokenSlot is used to split a token bucket into multiple slots to
// server different clients within the same resource group.
type TokenSlot struct {
	// settings is the token limit settings for the slot.
	settings *rmpb.TokenLimitSettings
	// tokens is the number of tokens in the slot.
	assignTokens float64
	neededTokens float64

	lastTokens float64
}

// GroupTokenBucketState is the running state of TokenBucket.
type GroupTokenBucketState struct {
	Tokens float64 `json:"tokens,omitempty"`
	// ClientUniqueID -> TokenSlot
	tokenSlots sync.Map

	mu struct {
		sync.Mutex
		needTokenSum float64
	}

	LastUpdate  *time.Time `json:"last_update,omitempty"`
	Initialized bool       `json:"initialized"`
	// settingChanged is used to avoid that the number of tokens returned is jitter because of changing fill rate.
	settingChanged bool
}

// Clone returns the copy of GroupTokenBucketState
func (gts *GroupTokenBucketState) Clone() *GroupTokenBucketState {
	return &GroupTokenBucketState{
		Tokens:      gts.Tokens,
		LastUpdate:  gts.LastUpdate,
		Initialized: gts.Initialized,
	}
}

func (gts *GroupTokenBucketState) balanceSlotTokens(
	neededTokens float64,
	clientUniqueID uint64,
	settings *rmpb.TokenLimitSettings) {
	gts.mu.Lock()
	defer gts.mu.Unlock()

	slot, ok := gts.tokenSlots.LoadOrStore(clientUniqueID, &TokenSlot{assignTokens: 0, neededTokens: neededTokens})
	prevNeed := slot.(*TokenSlot).neededTokens
	if !ok {
		gts.mu.needTokenSum += neededTokens
	} else {
		gts.mu.needTokenSum = gts.mu.needTokenSum - prevNeed + neededTokens
		slot.(*TokenSlot).neededTokens = neededTokens
	}

	gts.tokenSlots.Range(func(key, value interface{}) bool {
		slot := value.(*TokenSlot)
		ratio := slot.neededTokens / gts.mu.needTokenSum
		var (
			fillRate    = settings.GetFillRate() * uint64(ratio)
			burstLimit  = settings.GetBurstLimit() * int64(ratio)
			assignToken = gts.Tokens * ratio
		)

		slot.assignTokens = assignToken
		slot.lastTokens = assignToken
		slot.settings = &rmpb.TokenLimitSettings{
			FillRate:   fillRate,
			BurstLimit: burstLimit,
		}

		return true
	})
}

// NewGroupTokenBucket returns a new GroupTokenBucket
func NewGroupTokenBucket(tokenBucket *rmpb.TokenBucket) GroupTokenBucket {
	if tokenBucket == nil || tokenBucket.Settings == nil {
		return GroupTokenBucket{}
	}
	return GroupTokenBucket{
		Settings: tokenBucket.GetSettings(),
		GroupTokenBucketState: GroupTokenBucketState{
			Tokens:     tokenBucket.GetTokens(),
			tokenSlots: sync.Map{},
		},
	}
}

// GetTokenBucket returns the grpc protoc struct of GroupTokenBucket.
func (gtb *GroupTokenBucket) GetTokenBucket() *rmpb.TokenBucket {
	if gtb.Settings == nil {
		return nil
	}
	return &rmpb.TokenBucket{
		Settings: gtb.Settings,
		Tokens:   gtb.Tokens,
	}
}

// patch patches the token bucket settings.
func (gtb *GroupTokenBucket) patch(tb *rmpb.TokenBucket) {
	if tb == nil {
		return
	}
	if setting := proto.Clone(tb.GetSettings()).(*rmpb.TokenLimitSettings); setting != nil {
		gtb.Settings = setting
		gtb.settingChanged = true
	}

	// the settings in token is delta of the last update and now.
	gtb.Tokens += tb.GetTokens()
}

// init initializes the group token bucket.
func (gtb *GroupTokenBucket) init(now time.Time) {
	if gtb.Settings.FillRate == 0 {
		gtb.Settings.FillRate = defaultRefillRate
	}
	if gtb.Tokens < defaultInitialTokens {
		gtb.Tokens = defaultInitialTokens
	}
	gtb.LastUpdate = &now
	gtb.Initialized = true
}

// updateTokens updates the tokens and settings.
func (gtb *GroupTokenBucket) updateTokens(now time.Time, burstLimit int64, neededTokens float64, clientUniqueID uint64) {
	if !gtb.Initialized {
		gtb.init(now)
	} else if delta := now.Sub(*gtb.LastUpdate); delta > 0 {
		gtb.Tokens += float64(gtb.Settings.GetFillRate()) * delta.Seconds()
		gtb.LastUpdate = &now
	}
	if burst := float64(burstLimit); burst != 0 && gtb.Tokens > burst {
		gtb.Tokens = burst
	}
	// Balance resource group bucket and each slots.
	gtb.balanceSlotTokens(neededTokens, clientUniqueID, gtb.Settings)
}

// request requests tokens from the group token bucket.
func (gtb *GroupTokenBucket) request(now time.Time, neededTokens float64, targetPeriodMs, clientUniqueID uint64) (*rmpb.TokenBucket, int64) {
	// Update tokens
	gtb.updateTokens(now, gtb.Settings.GetBurstLimit(), neededTokens, clientUniqueID)
	// serve by each client
	slot, _ := gtb.tokenSlots.Load(clientUniqueID)

	res, trickleDuration := slot.(*TokenSlot).serve(neededTokens, targetPeriodMs)

	gtb.mu.Lock()
	defer gtb.mu.Unlock()
	// Tokens need to be subbed from each slot consume.
	var delta float64
	gtb.tokenSlots.Range(func(key, value interface{}) bool {
		slot := value.(*TokenSlot)
		delta += slot.lastTokens - slot.assignTokens
		return true
	})
	gtb.Tokens -= delta

	return res, trickleDuration
}

func (ts *TokenSlot) serve(neededTokens float64, targetPeriodMs uint64) (*rmpb.TokenBucket, int64) {
	var res rmpb.TokenBucket
	burstLimit := ts.settings.GetBurstLimit()
	res.Settings = &rmpb.TokenLimitSettings{BurstLimit: burstLimit}
	// If BurstLimit is -1, just return.
	if burstLimit < 0 {
		res.Tokens = neededTokens
		return &res, 0
	}
	// FillRate is used for the token server unavailable in abnormal situation.
	if neededTokens <= 0 {
		return &res, 0
	}
	// If the current tokens can directly meet the requirement, returns the need token
	if ts.assignTokens >= neededTokens {
		ts.assignTokens -= neededTokens
		// granted the total request tokens
		res.Tokens = neededTokens
		return &res, 0
	}

	// Firstly allocate the remaining tokens
	var grantedTokens float64
	hasRemaining := false
	if ts.assignTokens > 0 {
		grantedTokens = ts.assignTokens
		neededTokens -= grantedTokens
		ts.assignTokens = 0
		hasRemaining = true
	}

	var (
		targetPeriodTime    = time.Duration(targetPeriodMs) * time.Millisecond
		targetPeriodTimeSec = targetPeriodTime.Seconds()
		trickleTime         = 0.
		fillRate            = ts.settings.GetFillRate()
	)

	loanCoefficient := defaultLoanCoefficient
	// When BurstLimit less or equal FillRate, the server does not accumulate a significant number of tokens.
	// So we don't need to smooth the token allocation speed.
	if burstLimit > 0 && burstLimit <= int64(fillRate) {
		loanCoefficient = 1
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
	p := make([]float64, loanCoefficient)
	p[0] = float64(loanCoefficient) * float64(fillRate) * targetPeriodTimeSec
	for i := 1; i < loanCoefficient; i++ {
		p[i] = float64(loanCoefficient-i)*float64(fillRate)*targetPeriodTimeSec + p[i-1]
	}
	for i := 0; i < loanCoefficient && neededTokens > 0 && trickleTime < targetPeriodTimeSec; i++ {
		loan := -ts.assignTokens
		if loan > p[i] {
			continue
		}
		roundReserveTokens := p[i] - loan
		fillRate := float64(loanCoefficient-i) * float64(fillRate)
		if roundReserveTokens > neededTokens {
			ts.assignTokens -= neededTokens
			grantedTokens += neededTokens
			trickleTime += grantedTokens / fillRate
			neededTokens = 0
		} else {
			roundReserveTime := roundReserveTokens / fillRate
			if roundReserveTime+trickleTime >= targetPeriodTimeSec {
				roundTokens := (targetPeriodTimeSec - trickleTime) * fillRate
				neededTokens -= roundTokens
				ts.assignTokens -= roundTokens
				grantedTokens += roundTokens
				trickleTime = targetPeriodTimeSec
			} else {
				grantedTokens += roundReserveTokens
				neededTokens -= roundReserveTokens
				ts.assignTokens -= roundReserveTokens
				trickleTime += roundReserveTime
			}
		}
	}
	if grantedTokens < defaultReserveRatio*float64(fillRate)*targetPeriodTimeSec {
		ts.assignTokens -= defaultReserveRatio*float64(fillRate)*targetPeriodTimeSec - grantedTokens
		grantedTokens = defaultReserveRatio * float64(fillRate) * targetPeriodTimeSec
	}
	res.Tokens = grantedTokens

	var trickleDuration time.Duration
	// Can't directly treat targetPeriodTime as trickleTime when there is a token remaining.
	// If treated, client consumption will be slowed down (actually could be increased).
	if hasRemaining {
		trickleDuration = time.Duration(math.Min(trickleTime, targetPeriodTimeSec) * float64(time.Second))
	} else {
		trickleDuration = targetPeriodTime
	}
	return &res, trickleDuration.Milliseconds()
}
