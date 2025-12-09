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
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
<<<<<<< HEAD
	defaultRefillRate    = 10000
	defaultInitialTokens = 10 * 10000
=======
	defaultRefillRate         = 10000
	defaultModeratedBurstRate = 10000
	defaultInitialTokens      = 10 * 10000
	defaultReserveRatio       = 0.5
	defaultLoanCoefficient    = 2
	slotExpireTimeout         = 10 * time.Minute
>>>>>>> ff5f804f27 (feat(resourcemanager): refactor token bucket slot allocation (#9746))
)

const (
	defaultReserveRatio    = 0.5
	defaultLoanCoefficient = 2
	maxAssignTokens        = math.MaxFloat64 / 1024 // assume max client connect is 1024
	slotExpireTimeout      = 10 * time.Minute
)

// GroupTokenBucket is a token bucket for a resource group.
// Now we don't save consumption in `GroupTokenBucket`, only statistics it in prometheus.
type GroupTokenBucket struct {
	// Settings is the setting of TokenBucket.
	// BurstLimit is used as below:
	//   - If b == 0, that means the limiter is unlimited capacity. default use in resource controller (burst with a rate within an unlimited capacity).
	//   - If b < 0, that means the limiter is unlimited capacity and fillrate(r) is ignored, can be seen as r == Inf (burst within an unlimited capacity).
	//   - If b > 0, that means the limiter is limited capacity.
	// MaxTokens limits the number of tokens that can be accumulated
	Settings              *rmpb.TokenLimitSettings `json:"settings,omitempty"`
	GroupTokenBucketState `json:"state,omitempty"`
}

// Clone returns the deep copy of GroupTokenBucket
func (gtb *GroupTokenBucket) Clone() *GroupTokenBucket {
	if gtb == nil {
		return nil
	}
	var settings *rmpb.TokenLimitSettings
	if gtb.Settings != nil {
		settings = proto.Clone(gtb.Settings).(*rmpb.TokenLimitSettings)
	}
	stateClone := *gtb.GroupTokenBucketState.Clone()
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

// TokenSlot is used to split a token bucket into multiple slots to
// server different clients within the same resource group.
<<<<<<< HEAD
type TokenSlot struct {
	// settings is the token limit settings for the slot.
	settings *rmpb.TokenLimitSettings
	// requireTokensSum is the number of tokens required.
	requireTokensSum float64
	// tokenCapacity is the number of tokens in the slot.
	tokenCapacity     float64
=======
type tokenSlot struct {
	id                uint64
	fillRate          uint64
	burstLimit        int64
	curTokenCapacity  float64
>>>>>>> ff5f804f27 (feat(resourcemanager): refactor token bucket slot allocation (#9746))
	lastTokenCapacity float64
	lastReqTime       time.Time
}

<<<<<<< HEAD
func (ts *TokenSlot) logFields() []zap.Field {
	return []zap.Field{
		zap.Uint64("slot-fill-rate", ts.settings.GetFillRate()),
		zap.Int64("slot-burst-limit", ts.settings.GetBurstLimit()),
		zap.Float64("slot-require-tokens-sum", ts.requireTokensSum),
		zap.Float64("slot-token-capacity", ts.tokenCapacity),
=======
func newTokenSlot(clientUniqueID uint64, now time.Time) *tokenSlot {
	return &tokenSlot{
		id:          clientUniqueID,
		lastReqTime: now,
	}
}

func (ts *tokenSlot) logFields() []zap.Field {
	return []zap.Field{
		zap.Uint64("slot-id", ts.id),
		zap.Uint64("slot-fill-rate", ts.fillRate),
		zap.Int64("slot-burst-limit", ts.burstLimit),
		zap.Float64("slot-cur-token-capacity", ts.curTokenCapacity),
>>>>>>> ff5f804f27 (feat(resourcemanager): refactor token bucket slot allocation (#9746))
		zap.Float64("slot-last-token-capacity", ts.lastTokenCapacity),
		zap.Time("slot-last-req-time", ts.lastReqTime),
	}
}

// GroupTokenBucketState is the running state of TokenBucket.
type GroupTokenBucketState struct {
	Tokens      float64    `json:"tokens,omitempty"`
	LastUpdate  *time.Time `json:"last_update,omitempty"`
	Initialized bool       `json:"initialized"`

	resourceGroupName string
	// groupRUTracker is used to get the real-time RU/s of each client.
	grt *groupRUTracker
	// ClientUniqueID -> TokenSlot
<<<<<<< HEAD
	tokenSlots                 map[uint64]*TokenSlot
	clientConsumptionTokensSum float64
	lastBurstTokens            float64
=======
	tokenSlots map[uint64]*tokenSlot
	// Used to store tokens in the token slot that exceed burst limits,
	// ensuring that these tokens are not lost but are reintroduced into
	// token calculation during the next update.
	reservedBurstTokens float64
	// Used to store tokens that exceed the service limit,
	// ensuring that these tokens are not lost but are reintroduced into
	// token calculation during the next update.
	reservedServiceTokens float64
	// overrideFillRate is used to override the fill rate of the token bucket.
	// It's used to control the fill rate of the token bucket within the service
	// limit to ensure the priority of the resource group. Only non-negative value
	// means the fill rate is overridden.
	overrideFillRate float64
	// overrideBurstLimit is used to override the burst limit of the token bucket.
	// It's used to control the burst limit of the token bucket within the service
	// limit to ensure the priority of the resource group. Only non-negative value
	// means the burst limit is overridden.
	overrideBurstLimit int64
>>>>>>> ff5f804f27 (feat(resourcemanager): refactor token bucket slot allocation (#9746))

	// settingChanged is used to avoid that the number of tokens returned is jitter because of changing fill rate.
	settingChanged bool
}

// Clone returns the copy of GroupTokenBucketState
func (gts *GroupTokenBucketState) Clone() *GroupTokenBucketState {
	var tokenSlots map[uint64]*TokenSlot
	if gts.tokenSlots != nil {
		tokenSlots = make(map[uint64]*TokenSlot)
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
<<<<<<< HEAD
		Tokens:                     gts.Tokens,
		LastUpdate:                 lastUpdate,
		Initialized:                gts.Initialized,
		resourceGroupName:          gts.resourceGroupName,
		tokenSlots:                 tokenSlots,
		clientConsumptionTokensSum: gts.clientConsumptionTokensSum,
		lastCheckExpireSlot:        gts.lastCheckExpireSlot,
=======
		Tokens:             gts.Tokens,
		LastUpdate:         lastUpdate,
		Initialized:        gts.Initialized,
		resourceGroupName:  gts.resourceGroupName,
		tokenSlots:         tokenSlots,
		overrideFillRate:   gts.overrideFillRate,
		overrideBurstLimit: gts.overrideBurstLimit,
>>>>>>> ff5f804f27 (feat(resourcemanager): refactor token bucket slot allocation (#9746))
	}
}

func (gts *GroupTokenBucketState) resetLoan() {
	gts.settingChanged = false
	gts.Tokens = 0
	// Reset all slots.
	for _, slot := range gts.tokenSlots {
		slot.curTokenCapacity = 0
		slot.lastTokenCapacity = 0
	}
}

<<<<<<< HEAD
func (gts *GroupTokenBucketState) balanceSlotTokens(
	clientUniqueID uint64,
	settings *rmpb.TokenLimitSettings,
	requiredToken, elapseTokens float64) {
	now := time.Now()
	slot, exist := gts.tokenSlots[clientUniqueID]
	if !exist {
		// Only slots that require a positive number will be considered alive,
		// but still need to allocate the elapsed tokens as well.
		if requiredToken != 0 {
			slot = &TokenSlot{lastReqTime: now}
			gts.tokenSlots[clientUniqueID] = slot
			gts.clientConsumptionTokensSum = 0
		}
	} else {
		slot.lastReqTime = now
		if gts.clientConsumptionTokensSum >= maxAssignTokens {
			gts.clientConsumptionTokensSum = 0
		}
		// Clean up slot that required 0.
		if requiredToken == 0 {
			delete(gts.tokenSlots, clientUniqueID)
			gts.clientConsumptionTokensSum = 0
		}
	}

	if time.Since(gts.lastCheckExpireSlot) >= slotExpireTimeout {
		gts.lastCheckExpireSlot = now
		for clientUniqueID, slot := range gts.tokenSlots {
			if time.Since(slot.lastReqTime) >= slotExpireTimeout {
				delete(gts.tokenSlots, clientUniqueID)
				log.Info("delete resource group slot because expire", zap.Time("last-req-time", slot.lastReqTime),
					zap.Any("expire timeout", slotExpireTimeout), zap.Any("del client id", clientUniqueID), zap.Any("len", len(gts.tokenSlots)))
			}
		}
	}
	if len(gts.tokenSlots) == 0 {
		return
	}
	evenRatio := 1 / float64(len(gts.tokenSlots))
	if settings.GetBurstLimit() <= 0 {
		for _, slot := range gts.tokenSlots {
			slot.settings = &rmpb.TokenLimitSettings{
				FillRate:   uint64(float64(settings.GetFillRate()) * evenRatio),
				BurstLimit: settings.GetBurstLimit(),
			}
=======
func (gtb *GroupTokenBucket) balanceSlotTokens(
	now time.Time,
	clientUniqueID uint64,
	requiredToken, tokensForBalance float64,
) {
	slot, exist := gtb.tokenSlots[clientUniqueID]
	if !exist && requiredToken != 0 {
		// Create a new slot if the slot is not exist and the required token is not 0.
		slot = newTokenSlot(clientUniqueID, now)
		gtb.tokenSlots[clientUniqueID] = slot
	} else if exist && requiredToken != 0 {
		// Update the existing slot.
		slot.lastReqTime = now
	} else if requiredToken == 0 {
		// Clean up the slot that required 0.
		delete(gtb.tokenSlots, clientUniqueID)
	}
	// Clean up the expired slots.
	for clientUniqueID, slot := range gtb.tokenSlots {
		if time.Since(slot.lastReqTime) >= slotExpireTimeout {
			delete(gtb.tokenSlots, clientUniqueID)
			log.Info("delete resource group slot because expire",
				zap.Time("last-req-time", slot.lastReqTime),
				zap.Duration("expire-timeout", slotExpireTimeout),
				zap.Uint64("del-client-id", clientUniqueID),
				zap.Int("len", len(gtb.tokenSlots)))
			continue
		}
	}
	// Do nothing if there is no slot.
	slotNum := len(gtb.tokenSlots)
	if slotNum == 0 {
		return
	}
	// Balance the slots.
	// If the burstable mode is rateControlled or unlimited, just make each slot even and allow them to burst.
	evenRatio := 1 / float64(slotNum)
	if mode := gtb.getBurstableMode(); mode == rateControlled || mode == unlimited {
		for _, slot := range gtb.tokenSlots {
			slot.fillRate = uint64(gtb.getFillRate() * evenRatio)
			slot.burstLimit = gtb.getBurstLimit()
>>>>>>> ff5f804f27 (feat(resourcemanager): refactor token bucket slot allocation (#9746))
		}
		return
	}
	// If the slot number is 1, just treat it as the whole resource group.
	if slotNum == 1 {
		for _, slot := range gtb.tokenSlots {
			slot.curTokenCapacity = gtb.Tokens
			slot.lastTokenCapacity = gtb.Tokens
			slot.fillRate, slot.burstLimit = gtb.getFillRateAndBurstLimit()
		}
		return
	}

<<<<<<< HEAD
	for _, slot := range gts.tokenSlots {
		if gts.clientConsumptionTokensSum == 0 || len(gts.tokenSlots) == 1 {
			// Need to make each slot even.
			slot.tokenCapacity = evenRatio * gts.Tokens
			slot.lastTokenCapacity = evenRatio * gts.Tokens
			slot.requireTokensSum = 0
			gts.clientConsumptionTokensSum = 0

			var (
				fillRate   = float64(settings.GetFillRate()) * evenRatio
				burstLimit = float64(settings.GetBurstLimit()) * evenRatio
			)

			slot.settings = &rmpb.TokenLimitSettings{
				FillRate:   uint64(fillRate),
				BurstLimit: int64(burstLimit),
			}
		} else {
			// In order to have fewer tokens available to clients that are currently consuming more.
			// We have the following formula:
			// 		client1: (1 - a/N + 1/N) * 1/N
			// 		client2: (1 - b/N + 1/N) * 1/N
			// 		...
			// 		clientN: (1 - n/N + 1/N) * 1/N
			// Sum is:
			// 		(N - (a+b+...+n)/N +1) * 1/N => (N - 1 + 1) * 1/N => 1
			ratio := (1 - slot.requireTokensSum/gts.clientConsumptionTokensSum + evenRatio) * evenRatio

			var (
				fillRate    = float64(settings.GetFillRate()) * ratio
				burstLimit  = float64(settings.GetBurstLimit()) * ratio
				assignToken = elapseTokens * ratio
			)

			// Need to reserve burst limit to next balance.
			if burstLimit > 0 && slot.tokenCapacity > burstLimit {
				reservedTokens := slot.tokenCapacity - burstLimit
				gts.lastBurstTokens += reservedTokens
				gts.Tokens -= reservedTokens
				assignToken -= reservedTokens
			}

			slot.tokenCapacity += assignToken
			slot.lastTokenCapacity += assignToken
			slot.settings = &rmpb.TokenLimitSettings{
				FillRate:   uint64(fillRate),
				BurstLimit: int64(burstLimit),
			}
		}
	}
	if requiredToken != 0 {
		// Only slots that require a positive number will be considered alive.
		slot.requireTokensSum += requiredToken
		gts.clientConsumptionTokensSum += requiredToken
	}
}

=======
	var (
		totalFillRate, totalBurstLimit = gtb.getFillRateAndBurstLimit()
		basicFillRate                  = float64(totalFillRate) * evenRatio
		allocatedFillRate              = 0.0
		allocationMap                  = make(map[uint64]float64, len(gtb.tokenSlots))
		extraDemandSlots               = make(map[uint64]float64, len(gtb.tokenSlots))
		extraDemandSum                 = 0.0
	)
	for clientUniqueID := range gtb.tokenSlots {
		allocation := gtb.grt.getOrCreateRUTracker(clientUniqueID).getRUPerSec()
		// If the RU demand is greater than the basic fill rate, allocate the basic fill rate first.
		if allocation > basicFillRate {
			// Record the extra demand for the high demand slots.
			extraDemand := allocation - basicFillRate
			extraDemandSum += extraDemand
			extraDemandSlots[clientUniqueID] = extraDemand
			// Allocate the basic fill rate.
			allocation = basicFillRate
		}
		allocationMap[clientUniqueID] = allocation
		allocatedFillRate += allocation
	}
	remainingFillRate := float64(totalFillRate) - allocatedFillRate
	// For the remaining fill rate, allocate it proportionally to the high demand slots.
	if remainingFillRate > 0 && len(extraDemandSlots) > 0 {
		for clientUniqueID, extraDemand := range extraDemandSlots {
			allocationMap[clientUniqueID] += remainingFillRate * (extraDemand / extraDemandSum)
		}
	} else if remainingFillRate > 0 && len(extraDemandSlots) == 0 {
		// If there is no high demand slots, distribute the remaining fill rate to all slots evenly.
		avg := remainingFillRate / float64(slotNum)
		for clientUniqueID := range allocationMap {
			allocationMap[clientUniqueID] += avg
		}
	}
	// Finally, distribute the fill rate and burst limit to each slot based on the allocation.
	for clientUniqueID, slot := range gtb.tokenSlots {
		// Distribute the fill rate.
		fillRate := allocationMap[clientUniqueID]
		// Distribute the burst limit and assign tokens based on the allocation ratio.
		ratio := fillRate / float64(totalFillRate)
		burstLimit := float64(totalBurstLimit) * ratio
		assignTokens := tokensForBalance * ratio
		// Need to reserve burst limit to next balance.
		if burstLimit > 0 && slot.curTokenCapacity > burstLimit {
			reservedTokens := slot.curTokenCapacity - burstLimit
			gtb.reservedBurstTokens += reservedTokens
			gtb.Tokens -= reservedTokens
			assignTokens -= reservedTokens
		}
		// Update the slot token capacity.
		slot.curTokenCapacity += assignTokens
		slot.lastTokenCapacity += assignTokens
		// Update the slot fill rate and burst limit.
		slot.fillRate = uint64(fillRate)
		slot.burstLimit = int64(burstLimit)
	}
}

func (gtb *GroupTokenBucket) getFillRateAndBurstLimit() (fillRate uint64, burstLimit int64) {
	if gtb.getBurstableMode() == moderated {
		fillRate = uint64(math.Min(gtb.getFillRate()+defaultModeratedBurstRate, UnlimitedRate))
		burstLimit = int64(fillRate)
		return
	}
	fillRate = uint64(gtb.getFillRate())
	burstLimit = int64(float64(gtb.getBurstLimit()))
	return
}

>>>>>>> ff5f804f27 (feat(resourcemanager): refactor token bucket slot allocation (#9746))
// NewGroupTokenBucket returns a new GroupTokenBucket
func NewGroupTokenBucket(resourceGroupName string, tokenBucket *rmpb.TokenBucket) *GroupTokenBucket {
	if tokenBucket == nil || tokenBucket.Settings == nil {
		return &GroupTokenBucket{}
	}
	return &GroupTokenBucket{
		Settings: tokenBucket.GetSettings(),
		GroupTokenBucketState: GroupTokenBucketState{
			Tokens:            tokenBucket.GetTokens(),
			resourceGroupName: resourceGroupName,
			tokenSlots:        make(map[uint64]*TokenSlot),
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
<<<<<<< HEAD
func (gtb *GroupTokenBucket) init(now time.Time, clientID uint64) {
	if gtb.Settings.FillRate == 0 {
		gtb.Settings.FillRate = defaultRefillRate
=======
func (gtb *GroupTokenBucket) init(now time.Time) {
	if gtb.getFillRate() == 0 {
		gtb.setFillRateSetting(defaultRefillRate)
>>>>>>> ff5f804f27 (feat(resourcemanager): refactor token bucket slot allocation (#9746))
	}
	if gtb.Tokens < defaultInitialTokens && gtb.Settings.BurstLimit > 0 {
		gtb.Tokens = defaultInitialTokens
	}
<<<<<<< HEAD
	// init slot
	gtb.tokenSlots[clientID] = &TokenSlot{
		settings:          gtb.Settings,
		tokenCapacity:     gtb.Tokens,
		lastTokenCapacity: gtb.Tokens,
	}
=======
>>>>>>> ff5f804f27 (feat(resourcemanager): refactor token bucket slot allocation (#9746))
	gtb.LastUpdate = &now
	gtb.Initialized = true
}

// updateTokens updates the tokens and settings.
func (gtb *GroupTokenBucket) updateTokens(now time.Time, burstLimit int64, clientUniqueID uint64, requiredToken float64) {
	var elapseTokens float64
	if !gtb.Initialized {
		gtb.init(now)
	} else if burst := float64(burstLimit); burst > 0 {
		if delta := now.Sub(*gtb.LastUpdate); delta > 0 {
			elapseTokens = float64(gtb.Settings.GetFillRate())*delta.Seconds() + gtb.lastBurstTokens
			gtb.lastBurstTokens = 0
			gtb.Tokens += elapseTokens
		}
		if gtb.Tokens > burst {
			elapseTokens -= gtb.Tokens - burst
			gtb.Tokens = burst
		}
		gtb.LastUpdate = &now
	}
	// Reloan when setting changed
	if gtb.settingChanged && gtb.Tokens <= 0 {
		elapseTokens = 0
		gtb.resetLoan()
	}
	// Balance each slots.
<<<<<<< HEAD
	gtb.balanceSlotTokens(clientUniqueID, gtb.Settings, requiredToken, elapseTokens)
=======
	gtb.balanceSlotTokens(now, clientUniqueID, requiredToken, tokensForBalance)
>>>>>>> ff5f804f27 (feat(resourcemanager): refactor token bucket slot allocation (#9746))
}

func (gtb *GroupTokenBucket) inspectAnomalies(
	tb *rmpb.TokenBucket,
	slot *TokenSlot,
	logFields []zap.Field,
) bool {
	var errMsg string
	// Verify whether the allocated token is invalid, such as negative values, math.Inf, or math.NaN.
	if tb.Tokens < 0 || math.IsInf(tb.Tokens, 0) || math.IsNaN(tb.Tokens) {
		errMsg = "assigned token is invalid"
	}
	// Verify whether the state of the slot is abnormal.
	if math.IsInf(slot.curTokenCapacity, 0) || math.IsNaN(slot.curTokenCapacity) {
		errMsg = "slot token capacity is invalid"
	}
	// If there is any error, reset the group token bucket to avoid the group token bucket is in a bad state.
	isAnomaly := len(errMsg) > 0
	if isAnomaly {
		logFields = append(logFields,
			append(
				slot.logFields(),
				zap.String("resource-group-name", gtb.resourceGroupName),
				zap.String("settings", gtb.Settings.String()),
				zap.Float64("tokens", gtb.Tokens),
				zap.Int("slot-len", len(gtb.tokenSlots)),
			)...,
		)
		log.Error(errMsg, logFields...)
		// Reset after logging to keep the original context.
		gtb.resetLoan()
	}
	return isAnomaly
}

// request requests tokens from the corresponding slot.
func (gtb *GroupTokenBucket) request(
	now time.Time,
	requiredToken float64,
	targetPeriodMs, clientUniqueID uint64,
) (*rmpb.TokenBucket, int64) {
	burstLimit := gtb.Settings.GetBurstLimit()
	gtb.updateTokens(now, burstLimit, clientUniqueID, requiredToken)
	slot, ok := gtb.tokenSlots[clientUniqueID]
	if !ok {
		return &rmpb.TokenBucket{
			Settings: &rmpb.TokenLimitSettings{BurstLimit: burstLimit},
			Tokens:   0.0,
		}, 0
	}
	res, trickleDuration := slot.assignSlotTokens(requiredToken, targetPeriodMs)
	// Inspect the group token bucket and the assigned token result to catch any anomalies.
	if isAnomaly := gtb.inspectAnomalies(res, slot, []zap.Field{
		zap.Time("now", now),
		zap.Uint64("client-unique-id", clientUniqueID),
		zap.Uint64("target-period-ms", targetPeriodMs),
		zap.Float64("required-token", requiredToken),
		zap.Float64("assigned-tokens", res.Tokens),
	}); isAnomaly {
		// Return nil here to prevent sending any unexpected result to the client.
		// The client has to retry later to access the resource group whose state has been reset.
		return nil, 0
	}
	// Update bucket to record all tokens.
<<<<<<< HEAD
	gtb.Tokens -= slot.lastTokenCapacity - slot.tokenCapacity
	slot.lastTokenCapacity = slot.tokenCapacity

=======
	gtb.Tokens -= slot.lastTokenCapacity - slot.curTokenCapacity
	slot.lastTokenCapacity = slot.curTokenCapacity
>>>>>>> ff5f804f27 (feat(resourcemanager): refactor token bucket slot allocation (#9746))
	return res, trickleDuration
}

func (ts *TokenSlot) assignSlotTokens(requiredToken float64, targetPeriodMs uint64) (*rmpb.TokenBucket, int64) {
	burstLimit := ts.settings.GetBurstLimit()
	res := &rmpb.TokenBucket{
		Settings: &rmpb.TokenLimitSettings{BurstLimit: burstLimit},
		Tokens:   0.0,
	}
	// If BurstLimit < 0, just return.
	if burstLimit < 0 {
		res.Tokens = requiredToken
		return res, 0
	}
	// FillRate is used for the token server unavailable in abnormal situation.
	if requiredToken <= 0 {
		return res, 0
	}
	// If the current tokens can directly meet the requirement, returns the need token.
	if ts.curTokenCapacity >= requiredToken {
		ts.curTokenCapacity -= requiredToken
		// granted the total request tokens
		res.Tokens = requiredToken
		return res, 0
	}

	// Firstly allocate the remaining tokens
	var grantedTokens float64
<<<<<<< HEAD
	hasRemaining := false
	if ts.tokenCapacity > 0 {
		grantedTokens = ts.tokenCapacity
		requiredToken -= grantedTokens
		ts.tokenCapacity = 0
		hasRemaining = true
=======
	hasConsumedExistingTokens := false
	if ts.curTokenCapacity > 0 {
		grantedTokens = ts.curTokenCapacity
		requiredToken -= grantedTokens
		ts.curTokenCapacity = 0
		hasConsumedExistingTokens = true
>>>>>>> ff5f804f27 (feat(resourcemanager): refactor token bucket slot allocation (#9746))
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

	// loanCoefficient is relative to the capacity of load RUs.
	// It's like a buffer to slow down the client consumption. the buffer capacity is `(1 + 2 ... +loanCoefficient) * fillRate * targetPeriodTimeSec`.
	// Details see test case `TestGroupTokenBucketRequestLoop`.

	p := make([]float64, loanCoefficient)
	p[0] = float64(loanCoefficient) * float64(fillRate) * targetPeriodTimeSec
	for i := 1; i < loanCoefficient; i++ {
		p[i] = float64(loanCoefficient-i)*float64(fillRate)*targetPeriodTimeSec + p[i-1]
	}
	for i := 0; i < loanCoefficient && requiredToken > 0 && trickleTime < targetPeriodTimeSec; i++ {
		loan := -ts.curTokenCapacity
		if loan >= p[i] {
			continue
		}
		roundReserveTokens := p[i] - loan
		fillRate := float64(loanCoefficient-i) * float64(fillRate)
		if roundReserveTokens > requiredToken {
			ts.curTokenCapacity -= requiredToken
			grantedTokens += requiredToken
			trickleTime += grantedTokens / fillRate
			requiredToken = 0
		} else {
			roundReserveTime := roundReserveTokens / fillRate
			if roundReserveTime+trickleTime >= targetPeriodTimeSec {
				roundTokens := (targetPeriodTimeSec - trickleTime) * fillRate
				requiredToken -= roundTokens
				ts.curTokenCapacity -= roundTokens
				grantedTokens += roundTokens
				trickleTime = targetPeriodTimeSec
			} else {
				grantedTokens += roundReserveTokens
				requiredToken -= roundReserveTokens
				ts.curTokenCapacity -= roundReserveTokens
				trickleTime += roundReserveTime
			}
		}
	}
	if requiredToken > 0 && grantedTokens < defaultReserveRatio*float64(fillRate)*targetPeriodTimeSec {
		reservedTokens := math.Min(requiredToken+grantedTokens, defaultReserveRatio*float64(fillRate)*targetPeriodTimeSec)
		ts.curTokenCapacity -= reservedTokens - grantedTokens
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
	return res, trickleDuration.Milliseconds()
}
