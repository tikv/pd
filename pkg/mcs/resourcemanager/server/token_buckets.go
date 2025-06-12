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
	"math"
	"time"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
)

const (
	defaultRefillRate         = 10000
	defaultModeratedBurstRate = 10000
	defaultInitialTokens      = 10 * 10000
	defaultReserveRatio       = 0.5
	defaultLoanCoefficient    = 2
	maxAssignTokens           = math.MaxFloat64 / 1024 // assume max client connect is 1024
	slotExpireTimeout         = 10 * time.Minute
)

type burstableMode int

const (
	limited        burstableMode = iota // burstlimit is greater than 0
	rateControlled                      // burstlimit is 0
	unlimited                           // burstlimit is -1
	moderated                           // burstlimit is -2
)

func getBurstableMode(settings *rmpb.TokenLimitSettings) burstableMode {
	if settings == nil {
		return limited
	}
	// BurstLimit is used as below:
	//   - If b == 0, that means the limiter is unlimited capacity. default use in resource controller (burst with a rate within an unlimited capacity).
	//   - If b == -1, that means the limiter is unlimited capacity and fillrate(r) is ignored, can be seen as r == Inf (burst within an unlimited capacity).
	//   - If b == -2, that means the limiter is limited capacity and fillrate(r) is ignored, can be seen as r == defaultBurstLimitFactor * fillrate (burst within a limited capacity).
	//   - If b > 0, that means the limiter is limited capacity.
	burst := settings.GetBurstLimit()
	switch {
	case burst == -1:
		return unlimited
	case burst == -2:
		return moderated
	case burst == 0:
		return rateControlled
	case burst > 0:
		return limited
	default:
		log.Warn("invalid burst limit, fallback to limited mode",
			zap.Int64("burst-limit", burst))
		return limited
	}
}

// GroupTokenBucket is a token bucket for a resource group.
// Now we don't save consumption in `GroupTokenBucket`, only statistics it in prometheus.
type GroupTokenBucket struct {
	// Settings is the setting of TokenBucket.
	// MaxTokens limits the number of tokens that can be accumulated
	Settings              *rmpb.TokenLimitSettings `json:"settings,omitempty"`
	GroupTokenBucketState `json:"state,omitempty"`
}

func (gtb *GroupTokenBucket) getFillRateSetting() float64 {
	return float64(gtb.Settings.GetFillRate())
}

func (gtb *GroupTokenBucket) setFillRateSetting(fillRate uint64) {
	gtb.Settings.FillRate = fillRate
}

func (gtb *GroupTokenBucket) getBurstLimitSetting() int64 {
	return gtb.Settings.GetBurstLimit()
}

func (gtb *GroupTokenBucket) clone() *GroupTokenBucket {
	if gtb == nil {
		return nil
	}
	var settings *rmpb.TokenLimitSettings
	if gtb.Settings != nil {
		settings = proto.Clone(gtb.Settings).(*rmpb.TokenLimitSettings)
	}
	stateClone := *gtb.GroupTokenBucketState.clone()
	return &GroupTokenBucket{
		Settings:              settings,
		GroupTokenBucketState: stateClone,
	}
}

func (gtb *GroupTokenBucket) setState(state *GroupTokenBucketState) {
	gtb.Tokens = state.Tokens
	gtb.LastUpdate = state.LastUpdate
	gtb.Initialized = state.Initialized
}

// tokenSlot is used to split a token bucket into multiple slots to
// server different clients within the same resource group.
type tokenSlot struct {
	fillRate   uint64
	burstLimit int64
	// requireTokensSum is the number of tokens required.
	requireTokensSum float64
	// tokenCapacity is the number of tokens in the slot.
	tokenCapacity     float64
	lastTokenCapacity float64
	lastReqTime       time.Time
}

// GroupTokenBucketState is the running state of TokenBucket.
type GroupTokenBucketState struct {
	Tokens float64 `json:"tokens,omitempty"`
	// ClientUniqueID -> TokenSlot
	tokenSlots                 map[uint64]*tokenSlot
	clientConsumptionTokensSum float64
	// Used to store tokens in the token slot that exceed burst limits,
	// ensuring that these tokens are not lost but are reintroduced into
	// token calculation during the next update.
	lastBurstTokens float64
	// Used to store tokens that exceed the service limit,
	// ensuring that these tokens are not lost but are reintroduced into
	// token calculation during the next update.
	lastLimitedTokens float64

	LastUpdate  *time.Time `json:"last_update,omitempty"`
	Initialized bool       `json:"initialized"`
	// settingChanged is used to avoid that the number of tokens returned is jitter because of changing fill rate.
	settingChanged      bool
	lastCheckExpireSlot time.Time
}

func (gts *GroupTokenBucketState) clone() *GroupTokenBucketState {
	var tokenSlots map[uint64]*tokenSlot
	if gts.tokenSlots != nil {
		tokenSlots = make(map[uint64]*tokenSlot)
		for id, tokens := range gts.tokenSlots {
			tokenSlots[id] = tokens
		}
	}

	var lastUpdate *time.Time
	if gts.LastUpdate != nil {
		newLastUpdate := *gts.LastUpdate
		lastUpdate = &newLastUpdate
	}
	return &GroupTokenBucketState{
		Tokens:                     gts.Tokens,
		LastUpdate:                 lastUpdate,
		Initialized:                gts.Initialized,
		tokenSlots:                 tokenSlots,
		clientConsumptionTokensSum: gts.clientConsumptionTokensSum,
		lastCheckExpireSlot:        gts.lastCheckExpireSlot,
	}
}

func (gts *GroupTokenBucketState) resetLoan() {
	gts.settingChanged = false
	gts.Tokens = 0
	gts.clientConsumptionTokensSum = 0
	evenRatio := 1.0
	if l := len(gts.tokenSlots); l > 0 {
		evenRatio = 1 / float64(l)
	}

	evenTokens := gts.Tokens * evenRatio
	for _, slot := range gts.tokenSlots {
		slot.requireTokensSum = 0
		slot.tokenCapacity = evenTokens
		slot.lastTokenCapacity = evenTokens
	}
}

func (gtb *GroupTokenBucket) balanceSlotTokens(
	clientUniqueID uint64,
	requiredToken, elapseTokens float64,
) {
	now := time.Now()
	slot, exist := gtb.tokenSlots[clientUniqueID]
	if !exist {
		// Only slots that require a positive number will be considered alive,
		// but still need to allocate the elapsed tokens as well.
		if requiredToken != 0 {
			slot = &tokenSlot{lastReqTime: now}
			gtb.tokenSlots[clientUniqueID] = slot
			gtb.clientConsumptionTokensSum = 0
		}
	} else {
		slot.lastReqTime = now
		if gtb.clientConsumptionTokensSum >= maxAssignTokens {
			gtb.clientConsumptionTokensSum = 0
		}
		// Clean up slot that required 0.
		if requiredToken == 0 {
			delete(gtb.tokenSlots, clientUniqueID)
			gtb.clientConsumptionTokensSum = 0
		}
	}

	if time.Since(gtb.lastCheckExpireSlot) >= slotExpireTimeout {
		gtb.lastCheckExpireSlot = now
		for clientUniqueID, slot := range gtb.tokenSlots {
			if time.Since(slot.lastReqTime) >= slotExpireTimeout {
				delete(gtb.tokenSlots, clientUniqueID)
				log.Info("delete resource group slot because expire",
					zap.Time("last-req-time", slot.lastReqTime),
					zap.Duration("expire-timeout", slotExpireTimeout),
					zap.Uint64("del-client-id", clientUniqueID),
					zap.Int("len", len(gtb.tokenSlots)))
			}
		}
	}
	if len(gtb.tokenSlots) == 0 {
		return
	}
	evenRatio := 1 / float64(len(gtb.tokenSlots))
	if mode := getBurstableMode(gtb.Settings); mode == rateControlled || mode == unlimited {
		for _, slot := range gtb.tokenSlots {
			slot.fillRate = uint64(gtb.getFillRateSetting() * evenRatio)
			slot.burstLimit = gtb.getBurstLimitSetting()
		}
		return
	}

	for _, slot := range gtb.tokenSlots {
		if gtb.clientConsumptionTokensSum == 0 || len(gtb.tokenSlots) == 1 {
			// Need to make each slot even.
			slot.tokenCapacity = evenRatio * gtb.Tokens
			slot.lastTokenCapacity = evenRatio * gtb.Tokens
			slot.requireTokensSum = 0
			gtb.clientConsumptionTokensSum = 0

			slot.fillRate, slot.burstLimit = gtb.calcRateAndBurstLimit(evenRatio)
		} else {
			// In order to have fewer tokens available to clients that are currently consuming more.
			// We have the following formula:
			// 		client1: (1 - a/N + 1/N) * 1/N
			// 		client2: (1 - b/N + 1/N) * 1/N
			// 		...
			// 		clientN: (1 - n/N + 1/N) * 1/N
			// Sum is:
			// 		(N - (a+b+...+n)/N +1) * 1/N => (N - 1 + 1) * 1/N => 1
			ratio := (1 - slot.requireTokensSum/gtb.clientConsumptionTokensSum + evenRatio) * evenRatio

			assignToken := elapseTokens * ratio
			fillRate, burstLimit := gtb.calcRateAndBurstLimit(ratio)

			// Need to reserve burst limit to next balance.
			if burstLimit > 0 && slot.tokenCapacity > float64(burstLimit) {
				reservedTokens := slot.tokenCapacity - float64(burstLimit)
				gtb.lastBurstTokens += reservedTokens
				gtb.Tokens -= reservedTokens
				assignToken -= reservedTokens
			}

			slot.tokenCapacity += assignToken
			slot.lastTokenCapacity += assignToken
			slot.fillRate = fillRate
			slot.burstLimit = burstLimit
		}
	}
	if requiredToken != 0 {
		// Only slots that require a positive number will be considered alive.
		slot.requireTokensSum += requiredToken
		gtb.clientConsumptionTokensSum += requiredToken
	}
}

func (gtb *GroupTokenBucket) calcRateAndBurstLimit(ratio float64) (fillRate uint64, burstLimit int64) {
	if getBurstableMode(gtb.Settings) == moderated {
		fillRate = uint64(math.Min(gtb.getFillRateSetting()+defaultModeratedBurstRate, unlimitedRate) * ratio)
		burstLimit = int64(fillRate)
		return
	}
	fillRate = uint64(gtb.getFillRateSetting() * ratio)
	burstLimit = int64(float64(gtb.getBurstLimitSetting()) * ratio)
	return
}

// NewGroupTokenBucket returns a new GroupTokenBucket
func NewGroupTokenBucket(tokenBucket *rmpb.TokenBucket) *GroupTokenBucket {
	if tokenBucket == nil || tokenBucket.Settings == nil {
		return &GroupTokenBucket{}
	}
	return &GroupTokenBucket{
		Settings: tokenBucket.GetSettings(),
		GroupTokenBucketState: GroupTokenBucketState{
			Tokens:     tokenBucket.GetTokens(),
			tokenSlots: make(map[uint64]*tokenSlot),
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

	// The settings in token is delta of the last update and now.
	gtb.Tokens += tb.GetTokens()
}

// init initializes the group token bucket.
func (gtb *GroupTokenBucket) init(now time.Time, clientID uint64) {
	if gtb.getFillRateSetting() == 0 {
		gtb.setFillRateSetting(defaultRefillRate)
	}
	if gtb.Tokens < defaultInitialTokens && gtb.getBurstLimitSetting() > 0 {
		gtb.Tokens = defaultInitialTokens
	}
	// init slot
	gtb.tokenSlots[clientID] = &tokenSlot{
		// Copy settings to avoid modifying the original settings.
		fillRate:          uint64(gtb.getFillRateSetting()),
		burstLimit:        gtb.getBurstLimitSetting(),
		tokenCapacity:     gtb.Tokens,
		lastTokenCapacity: gtb.Tokens,
	}
	gtb.LastUpdate = &now
	gtb.lastCheckExpireSlot = now
	gtb.Initialized = true
}

// updateTokens updates the tokens and settings.
func (gtb *GroupTokenBucket) updateTokens(now time.Time, burstLimit int64, clientUniqueID uint64, requiredToken float64) {
	var elapseTokens float64
	if !gtb.Initialized {
		gtb.init(now, clientUniqueID)
	} else if burst := float64(burstLimit); burst > 0 {
		if delta := now.Sub(*gtb.LastUpdate); delta > 0 {
			elapseTokens = gtb.getFillRateSetting()*delta.Seconds() + gtb.lastBurstTokens + gtb.lastLimitedTokens
			gtb.lastBurstTokens = 0
			gtb.lastLimitedTokens = 0
			gtb.Tokens += elapseTokens
		}
		if gtb.Tokens > burst {
			elapseTokens -= gtb.Tokens - burst
			gtb.Tokens = burst
		}
	}
	gtb.LastUpdate = &now
	// Reloan when setting changed
	if gtb.settingChanged && gtb.Tokens <= 0 {
		elapseTokens = 0
		gtb.resetLoan()
	}
	// Balance each slots.
	gtb.balanceSlotTokens(clientUniqueID, requiredToken, elapseTokens)
}

// request requests tokens from the corresponding slot.
func (gtb *GroupTokenBucket) request(now time.Time,
	requiredToken float64,
	targetPeriodMs, clientUniqueID uint64,
) (*rmpb.TokenBucket, int64) {
	burstLimit := gtb.getBurstLimitSetting()
	gtb.updateTokens(now, burstLimit, clientUniqueID, requiredToken)
	slot, ok := gtb.tokenSlots[clientUniqueID]
	if !ok {
		return &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{BurstLimit: burstLimit}}, 0
	}
	res, trickleDuration := slot.assignSlotTokens(requiredToken, targetPeriodMs)
	// Update bucket to record all tokens.
	gtb.Tokens -= slot.lastTokenCapacity - slot.tokenCapacity
	slot.lastTokenCapacity = slot.tokenCapacity

	return res, trickleDuration
}

func (ts *tokenSlot) assignSlotTokens(requiredToken float64, targetPeriodMs uint64) (*rmpb.TokenBucket, int64) {
	var res rmpb.TokenBucket
	burstLimit := ts.burstLimit
	res.Settings = &rmpb.TokenLimitSettings{BurstLimit: burstLimit}
	if getBurstableMode(res.Settings) == unlimited {
		res.Tokens = requiredToken
		return &res, 0
	}
	// FillRate is used for the token server unavailable in abnormal situation.
	if requiredToken <= 0 {
		return &res, 0
	}
	// If the current tokens can directly meet the requirement, returns the need token.
	if ts.tokenCapacity >= requiredToken {
		ts.tokenCapacity -= requiredToken
		// granted the total request tokens
		res.Tokens = requiredToken
		return &res, 0
	}

	// Firstly allocate the remaining tokens
	var grantedTokens float64
	hasRemaining := false
	if ts.tokenCapacity > 0 {
		grantedTokens = ts.tokenCapacity
		requiredToken -= grantedTokens
		ts.tokenCapacity = 0
		hasRemaining = true
	}

	var (
		targetPeriodTime    = time.Duration(targetPeriodMs) * time.Millisecond
		targetPeriodTimeSec = targetPeriodTime.Seconds()
		trickleTime         = 0.
		fillRate            = ts.fillRate
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

	// loanCoefficient is relative to the capacity of load RUs.
	// It's like a buffer to slow down the client consumption. the buffer capacity is `(1 + 2 ... +loanCoefficient) * fillRate * targetPeriodTimeSec`.
	// Details see test case `TestGroupTokenBucketRequestLoop`.

	p := make([]float64, loanCoefficient)
	p[0] = float64(loanCoefficient) * float64(fillRate) * targetPeriodTimeSec
	for i := 1; i < loanCoefficient; i++ {
		p[i] = float64(loanCoefficient-i)*float64(fillRate)*targetPeriodTimeSec + p[i-1]
	}
	for i := 0; i < loanCoefficient && requiredToken > 0 && trickleTime < targetPeriodTimeSec; i++ {
		loan := -ts.tokenCapacity
		if loan >= p[i] {
			continue
		}
		roundReserveTokens := p[i] - loan
		fillRate := float64(loanCoefficient-i) * float64(fillRate)
		if roundReserveTokens > requiredToken {
			ts.tokenCapacity -= requiredToken
			grantedTokens += requiredToken
			trickleTime += grantedTokens / fillRate
			requiredToken = 0
		} else {
			roundReserveTime := roundReserveTokens / fillRate
			if roundReserveTime+trickleTime >= targetPeriodTimeSec {
				roundTokens := (targetPeriodTimeSec - trickleTime) * fillRate
				requiredToken -= roundTokens
				ts.tokenCapacity -= roundTokens
				grantedTokens += roundTokens
				trickleTime = targetPeriodTimeSec
			} else {
				grantedTokens += roundReserveTokens
				requiredToken -= roundReserveTokens
				ts.tokenCapacity -= roundReserveTokens
				trickleTime += roundReserveTime
			}
		}
	}
	if requiredToken > 0 && grantedTokens < defaultReserveRatio*float64(fillRate)*targetPeriodTimeSec {
		reservedTokens := math.Min(requiredToken+grantedTokens, defaultReserveRatio*float64(fillRate)*targetPeriodTimeSec)
		ts.tokenCapacity -= reservedTokens - grantedTokens
		grantedTokens = reservedTokens
	}
	res.Tokens = grantedTokens

	var trickleDuration time.Duration
	// Can't directly treat targetPeriodTime as trickleTime when there is a token remaining.
	// If treated, client consumption will be slowed down (actually could be increased).
	if hasRemaining {
		trickleDuration = time.Duration(math.Min(trickleTime, targetPeriodTime.Seconds()) * float64(time.Second))
	} else {
		trickleDuration = targetPeriodTime
	}
	return &res, trickleDuration.Milliseconds()
}
