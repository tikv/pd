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
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	defaultRefillRate    = 10000
	defaultInitialTokens = 10 * 10000
)

const (
	defaultReserveRatio    = 0.5
	defaultLoanCoefficient = 2
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

// tokenSlot is used to split a token bucket into multiple slots to
// serve different clients within the same resource group.
type tokenSlot struct {
	id                uint64
	fillRate          uint64
	burstLimit        int64
	curTokenCapacity  float64
	lastTokenCapacity float64
	lastReqTime       time.Time
}

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
	// ClientUniqueID -> tokenSlot
	tokenSlots      map[uint64]*tokenSlot
	lastBurstTokens float64

	// settingChanged is used to avoid that the number of tokens returned is jitter because of changing fill rate.
	settingChanged bool
}

// Clone returns the copy of GroupTokenBucketState
func (gts *GroupTokenBucketState) Clone() *GroupTokenBucketState {
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
		Tokens:            gts.Tokens,
		LastUpdate:        lastUpdate,
		Initialized:       gts.Initialized,
		resourceGroupName: gts.resourceGroupName,
		tokenSlots:        tokenSlots,
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

func (gtb *GroupTokenBucket) balanceSlotTokens(
	now time.Time,
	clientUniqueID uint64,
	requiredToken, tokensForBalance float64,
) {
	slot, exist := gtb.tokenSlots[clientUniqueID]
	if !exist && requiredToken != 0 {
		// Create a new slot if the slot does not exist and the required token is not 0.
		slot = newTokenSlot(clientUniqueID, now)
		gtb.tokenSlots[clientUniqueID] = slot
		log.Debug("create resource group slot",
			zap.String("resource-group-name", gtb.resourceGroupName),
			zap.Uint64("client-unique-id", clientUniqueID),
			zap.Float64("required-token", requiredToken),
			zap.Int("slot-len", len(gtb.tokenSlots)),
		)
	} else if exist && requiredToken != 0 {
		// Update the existing slot.
		slot.lastReqTime = now
	} else if requiredToken == 0 {
		// Clean up the slot that required 0.
		if exist {
			log.Debug("delete resource group slot because required token is 0",
				zap.String("resource-group-name", gtb.resourceGroupName),
				zap.Uint64("client-unique-id", clientUniqueID),
				zap.Int("slot-len", len(gtb.tokenSlots)),
			)
		}
		delete(gtb.tokenSlots, clientUniqueID)
	}
	// Clean up the expired slots.
	for cid, s := range gtb.tokenSlots {
		if time.Since(s.lastReqTime) >= slotExpireTimeout {
			delete(gtb.tokenSlots, cid)
			log.Info("delete resource group slot because expire",
				zap.Time("last-req-time", s.lastReqTime),
				zap.Duration("expire-timeout", slotExpireTimeout),
				zap.Uint64("client-unique-id", clientUniqueID),
				zap.Uint64("del-client-id", cid),
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
	// If BurstLimit <= 0, just make each slot even and allow them to burst.
	evenRatio := 1 / float64(slotNum)
	if gtb.Settings.GetBurstLimit() <= 0 {
		for _, s := range gtb.tokenSlots {
			s.fillRate = uint64(float64(gtb.Settings.GetFillRate()) * evenRatio)
			s.burstLimit = gtb.Settings.GetBurstLimit()
		}
		return
	}
	// If the slot number is 1, just treat it as the whole resource group.
	if slotNum == 1 {
		fillRate := gtb.Settings.GetFillRate()
		burstLimit := gtb.Settings.GetBurstLimit()
		for _, s := range gtb.tokenSlots {
			// If fill rate is 0, don't grant any existing tokens.
			if fillRate == 0 {
				s.curTokenCapacity = 0
				s.lastTokenCapacity = 0
			} else {
				s.curTokenCapacity = gtb.Tokens
				s.lastTokenCapacity = gtb.Tokens
			}
			s.fillRate = fillRate
			s.burstLimit = burstLimit
		}
		return
	}

	var (
		totalFillRate  = gtb.Settings.GetFillRate()
		totalBurst     = gtb.Settings.GetBurstLimit()
		basicFillRate  = float64(totalFillRate) * evenRatio
		allocatedFill  = 0.0
		allocationMap  = make(map[uint64]float64, slotNum)
		extraDemand    = make(map[uint64]float64, slotNum)
		extraDemandSum = 0.0
	)
	// Guard: if totalFillRate is 0, throttle all slots.
	if totalFillRate == 0 {
		log.Debug("resource group total fill rate is 0, throttle all slots",
			zap.String("resource-group-name", gtb.resourceGroupName),
			zap.Int("slot-len", slotNum),
		)
		gtb.Tokens = 0
		gtb.lastBurstTokens = 0
		for _, s := range gtb.tokenSlots {
			s.fillRate = 0
			s.burstLimit = 0
			s.curTokenCapacity = 0
			s.lastTokenCapacity = 0
		}
		return
	}
	if gtb.grt == nil {
		gtb.grt = newGroupRUTracker()
	}
	for cid := range gtb.tokenSlots {
		ruTracker := gtb.grt.getOrCreateRUTracker(cid)
		allocation := ruTracker.getRUPerSec()
		// If the RU demand is greater than the basic fill rate, allocate the basic fill rate first.
		if allocation > basicFillRate {
			// Record the extra demand for the high demand slots.
			extra := allocation - basicFillRate
			extraDemandSum += extra
			extraDemand[cid] = extra
			// Allocate the basic fill rate.
			allocation = basicFillRate
		} else if !ruTracker.isInitialized() {
			// If the RU tracker is not initialized, just allocate the basic fill rate.
			// This is to avoid that the new slot can't get any fill rate.
			allocation = basicFillRate
		}
		allocationMap[cid] = allocation
		allocatedFill += allocation
	}
	remainingFillRate := float64(totalFillRate) - allocatedFill
	// For the remaining fill rate, allocate it proportionally to the high demand slots.
	if remainingFillRate > 0 && len(extraDemand) > 0 {
		for cid, extra := range extraDemand {
			allocationMap[cid] += remainingFillRate * (extra / extraDemandSum)
		}
	} else if remainingFillRate > 0 && len(extraDemand) == 0 {
		// If there are no high demand slots, distribute the remaining fill rate to all slots evenly.
		avg := remainingFillRate / float64(slotNum)
		for cid := range allocationMap {
			allocationMap[cid] += avg
		}
	}
	// Finally, distribute the fill rate and burst limit to each slot based on the allocation.
	for cid, s := range gtb.tokenSlots {
		// Distribute the fill rate.
		fillRate := allocationMap[cid]
		// Distribute the burst limit and assign tokens based on the allocation ratio.
		ratio := fillRate / float64(totalFillRate)
		burstLimit := float64(totalBurst) * ratio
		assignTokens := tokensForBalance * ratio
		// Need to reserve burst limit to next balance.
		if burstLimit > 0 && s.curTokenCapacity > burstLimit {
			reservedTokens := s.curTokenCapacity - burstLimit
			gtb.lastBurstTokens += reservedTokens
			gtb.Tokens -= reservedTokens
			assignTokens -= reservedTokens
		}
		// Update the slot token capacity.
		s.curTokenCapacity += assignTokens
		s.lastTokenCapacity += assignTokens
		// Update the slot fill rate and burst limit.
		s.fillRate = uint64(fillRate)
		s.burstLimit = int64(burstLimit)
	}
}

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
			tokenSlots:        make(map[uint64]*tokenSlot),
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
func (gtb *GroupTokenBucket) init(now time.Time) {
	if gtb.Settings.FillRate == 0 {
		gtb.Settings.FillRate = defaultRefillRate
	}
	if gtb.Tokens < defaultInitialTokens && gtb.Settings.BurstLimit > 0 {
		gtb.Tokens = defaultInitialTokens
	}
	gtb.LastUpdate = &now
	gtb.Initialized = true
}

// updateTokens updates the tokens and settings.
func (gtb *GroupTokenBucket) updateTokens(now time.Time, burstLimit int64, clientUniqueID uint64, requiredToken float64) {
	var tokensForBalance float64
	if !gtb.Initialized {
		gtb.init(now)
	} else if burst := float64(burstLimit); burst > 0 {
		if delta := now.Sub(*gtb.LastUpdate); delta > 0 {
			tokensForBalance = float64(gtb.Settings.GetFillRate())*delta.Seconds() + gtb.lastBurstTokens
			gtb.lastBurstTokens = 0
			gtb.Tokens += tokensForBalance
		}
		if gtb.Tokens > burst {
			tokensForBalance -= gtb.Tokens - burst
			gtb.Tokens = burst
		}
		gtb.LastUpdate = &now
	}
	// Reloan when setting changed
	if gtb.settingChanged && gtb.Tokens <= 0 {
		tokensForBalance = 0
		gtb.resetLoan()
	}
	// Balance each slots.
	gtb.balanceSlotTokens(now, clientUniqueID, requiredToken, tokensForBalance)
}

func (gtb *GroupTokenBucket) inspectAnomalies(
	tb *rmpb.TokenBucket,
	slot *tokenSlot,
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
	if requiredToken > 0 && slot.fillRate == 0 {
		log.Info("resource group slot fill rate is 0, reject token request",
			zap.String("resource-group-name", gtb.resourceGroupName),
			zap.Uint64("client-unique-id", clientUniqueID),
			zap.Float64("required-token", requiredToken),
			zap.Uint64("slot-fill-rate", slot.fillRate),
			zap.Int64("slot-burst-limit", slot.burstLimit),
			zap.Float64("bucket-tokens", gtb.Tokens),
			zap.Int("slot-len", len(gtb.tokenSlots)),
		)
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
	gtb.Tokens -= slot.lastTokenCapacity - slot.curTokenCapacity
	slot.lastTokenCapacity = slot.curTokenCapacity

	return res, trickleDuration
}

func (ts *tokenSlot) assignSlotTokens(requiredToken float64, targetPeriodMs uint64) (*rmpb.TokenBucket, int64) {
	burstLimit := ts.burstLimit
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
	// If fill rate is 0 (throttled), avoid granting any tokens and keep client retrying.
	if ts.fillRate == 0 {
		ts.curTokenCapacity = 0
		ts.lastTokenCapacity = 0
		return res, int64(targetPeriodMs)
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
	hasRemaining := false
	if ts.curTokenCapacity > 0 {
		grantedTokens = ts.curTokenCapacity
		requiredToken -= grantedTokens
		ts.curTokenCapacity = 0
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
	log.Debug("assign slot tokens",
		zap.Uint64("client-unique-id", ts.id),
		zap.Float64("required-token", requiredToken),
		zap.Uint64("target-period-ms", targetPeriodMs),
		zap.Uint64("fill-rate", fillRate),
		zap.Int64("burst-limit", burstLimit),
		zap.Int("loan-coefficient", loanCoefficient),
		zap.Float64("current-token-capacity", ts.curTokenCapacity),
	)
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
