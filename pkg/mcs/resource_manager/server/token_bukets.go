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
	"context"
	"math"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

const (
	defaultRequestChanSize = 1000
	defaultRefillRate      = 10000
	defaultInitialTokens   = 10 * 10000
	defaultReserveRatio    = 0.05
	defaultLoanCoefficient = 2
)

type requestTyp uint8

const (
	normal requestTyp = iota
	// loan is used to indicate the request should be served by the loan.
	loan
	// arrogate is used to indicate the request should be served by
	// other clients' token slots.
	arrogate
)

// TokenRequest wraps the necessary info for a token request.
type TokenRequest struct {
	// Request parameters.
	now            time.Time
	neededTokens   float64
	targetPeriodMs uint64
	clientUniqueID uint64
	// Internal states.
	grantedTokens float64
	typ           requestTyp
	// respChan is used to send the response back to the gRPC stream.
	respChan chan<- struct {
		resp          *rmpb.TokenBucket
		trickleTimeMs int64
	}
}

// NewTokenRequest creates a new token request.
func NewTokenRequest(
	now time.Time,
	neededTokens float64,
	targetPeriodMs, clientUniqueID uint64,
	respChan chan<- struct {
		resp          *rmpb.TokenBucket
		trickleTimeMs int64
	},
) *TokenRequest {
	return &TokenRequest{
		now:            now,
		neededTokens:   neededTokens,
		targetPeriodMs: targetPeriodMs,
		clientUniqueID: clientUniqueID,
		typ:            normal,
		respChan:       respChan,
	}
}

// Finish the request by sending the response back to the gRPC stream.
func (tr *TokenRequest) finish(resp *rmpb.TokenBucket, trickleTimeMs int64) {
	if resp == nil || tr.respChan == nil {
		return
	}
	tr.respChan <- struct {
		resp          *rmpb.TokenBucket
		trickleTimeMs int64
	}{
		resp,
		trickleTimeMs,
	}
}

// TokenSlot is used to split a token bucket into multiple slots to
// server different clients within the same resource group.
type TokenSlot struct {
	// settings is the token limit settings for the slot.
	settings *rmpb.TokenLimitSettings
	// tokens is the number of tokens in the slot.
	tokens float64
	// requestSlotChan is used to receive token requests from the group token bucket
	// state main loop.
	requestSlotChan chan *TokenRequest
	// requestChan is used to send the token request back to the main loop of the
	// group token bucket state.
	requestChan chan<- *TokenRequest
}

func newTokenSlot(
	ctx context.Context,
	requestChan chan<- *TokenRequest,
) *TokenSlot {
	slot := &TokenSlot{
		tokens:          0,
		requestSlotChan: make(chan *TokenRequest, defaultRequestChanSize),
		requestChan:     requestChan,
	}
	go slot.slotHandler(ctx)
	return slot
}

// slotHandler will start a goroutine to handle token requests for the slot.
// There are two possible ways for a token request to go:
//  1. If the token in the slot is enough, the request will be served immediately
//     and send back to the gRPC stream.
//  2. If the token in the slot is not enough, the request will be put into the
//     main loop of the group token bucket state again and try to get rest tokens
//     from other slots.
func (ts *TokenSlot) slotHandler(ctx context.Context) {
	slotCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		select {
		case <-slotCtx.Done():
			return
		case req := <-ts.requestSlotChan:
			ts.serveRequest(req)
		}
	}
}

func (ts *TokenSlot) serveRequest(req *TokenRequest) {
	burstLimit := ts.settings.GetBurstLimit()
	res := &rmpb.TokenBucket{
		Settings: &rmpb.TokenLimitSettings{
			BurstLimit: burstLimit,
		},
	}
	// If the current tokens can directly meet the requirement,
	// returns the needed tokens.
	if ts.tokens >= req.neededTokens {
		ts.tokens -= req.neededTokens
		req.grantedTokens = req.neededTokens
		// granted the total request tokens
		res.Tokens = req.grantedTokens
		req.finish(res, 0)
		return
	}
	// Set it to the proper type and send it back to the main loop if needed.
	switch req.typ {
	case normal:
		// If the request is normal, we need to arrogate the tokens from other slots.
		req.typ = arrogate
		ts.requestChan <- req
		return
	case arrogate:
		// TODO: maybe should grant the remained tokens to the request here.
		// If the request is arrogate, we need to loan the tokens from its own slots.
		req.typ = loan
		ts.requestChan <- req
		return
	case loan:
	}
	// First allocate the remaining tokens.
	hasRemaining := false
	if ts.tokens > 0 {
		req.grantedTokens = ts.tokens
		req.neededTokens -= req.grantedTokens
		ts.tokens = 0
		hasRemaining = true
	}
	var (
		targetPeriodTime    = time.Duration(req.targetPeriodMs) * time.Millisecond
		targetPeriodTimeSec = targetPeriodTime.Seconds()
		trickleTime         = 0.
		fillRate            = ts.settings.GetFillRate()
	)
	loanCoefficient := defaultLoanCoefficient
	// when BurstLimit less or equal FillRate, the server does not accumulate a significant number of tokens.
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
	for i := 0; i < loanCoefficient && req.neededTokens > 0 && trickleTime < targetPeriodTimeSec; i++ {
		loan := -ts.tokens
		if loan > p[i] {
			continue
		}
		roundReserveTokens := p[i] - loan
		fillRate := float64(loanCoefficient-i) * float64(fillRate)
		if roundReserveTokens > req.neededTokens {
			ts.tokens -= req.neededTokens
			req.grantedTokens += req.neededTokens
			trickleTime += req.grantedTokens / fillRate
			req.neededTokens = 0
		} else {
			roundReserveTime := roundReserveTokens / fillRate
			if roundReserveTime+trickleTime >= targetPeriodTimeSec {
				roundTokens := (targetPeriodTimeSec - trickleTime) * fillRate
				req.neededTokens -= roundTokens
				ts.tokens -= roundTokens
				req.grantedTokens += roundTokens
				trickleTime = targetPeriodTimeSec
			} else {
				req.grantedTokens += roundReserveTokens
				req.neededTokens -= roundReserveTokens
				ts.tokens -= roundReserveTokens
				trickleTime += roundReserveTime
			}
		}
	}
	if req.grantedTokens < defaultReserveRatio*float64(fillRate)*targetPeriodTimeSec {
		ts.tokens -= defaultReserveRatio*float64(fillRate)*targetPeriodTimeSec - req.grantedTokens
		req.grantedTokens = defaultReserveRatio * float64(fillRate) * targetPeriodTimeSec
	}
	res.Tokens = req.grantedTokens
	var trickleDuration time.Duration
	// can't directly treat targetPeriodTime as trickleTime when there is a token remaining.
	// If treat, client consumption will be slowed down (actually cloud be increased).
	if hasRemaining {
		trickleDuration = time.Duration(math.Min(trickleTime, targetPeriodTimeSec) * float64(time.Second))
	} else {
		trickleDuration = targetPeriodTime
	}
	req.finish(res, trickleDuration.Milliseconds())
}

// GroupTokenBucketState is the running state of TokenBucket.
type GroupTokenBucketState struct {
	/* Internal States */
	slots []uint64
	// ClientUniqueID -> TokenSlot
	tokenSlots sync.Map
	// requestChan is used to receive token requests from gRPC stream.
	requestChan chan *TokenRequest
	// patchChan is used to receive the token bucket settings patch.
	patchChan chan *rmpb.TokenBucket

	/* Visible States */
	Tokens      float64    `json:"tokens,omitempty"`
	LastUpdate  *time.Time `json:"last_update,omitempty"`
	Initialized bool       `json:"initialized"`
}

func newGroupTokenBucketState(tokens float64) *GroupTokenBucketState {
	return &GroupTokenBucketState{
		tokenSlots:  sync.Map{},
		requestChan: make(chan *TokenRequest, defaultRequestChanSize),
		patchChan:   make(chan *rmpb.TokenBucket, 1),
		Tokens:      tokens,
	}
}

func (gts *GroupTokenBucketState) clone() *GroupTokenBucketState {
	return &GroupTokenBucketState{
		Tokens:      gts.Tokens,
		LastUpdate:  gts.LastUpdate,
		Initialized: gts.Initialized,
	}
}

func (gts *GroupTokenBucketState) getOrCreateTokenSlot(
	ctx context.Context,
	clientUniqueID uint64,
	settings *rmpb.TokenLimitSettings,
	requestChan chan<- *TokenRequest,
) *TokenSlot {
	slot, ok := gts.tokenSlots.Load(clientUniqueID)
	if !ok {
		slot, ok = gts.tokenSlots.LoadOrStore(clientUniqueID, newTokenSlot(ctx, requestChan))
		if !ok {
			gts.slots = append(gts.slots, clientUniqueID)
			// The new slot is inserted, need to balance the tokens and settings.
			gts.balanceSlotTokens(settings)
		}
	}
	return slot.(*TokenSlot)
}

func (gts *GroupTokenBucketState) getMostTokensSlot(exclude uint64) *TokenSlot {
	var mostTokensSlot *TokenSlot
	gts.tokenSlots.Range(func(key, value interface{}) bool {
		if key.(uint64) == exclude {
			return true
		}
		slot := value.(*TokenSlot)
		if mostTokensSlot == nil || slot.tokens > mostTokensSlot.tokens {
			mostTokensSlot = slot
		}
		return true
	})
	return mostTokensSlot
}

func (gts *GroupTokenBucketState) balanceSlotTokens(settings *rmpb.TokenLimitSettings) {
	slotNum := len(gts.slots)
	// If there is only one slot, set all tokens to it.
	if slotNum == 1 {
		gts.tokenSlots.Range(func(key, value interface{}) bool {
			slot := value.(*TokenSlot)
			slot.settings = settings
			slot.tokens = gts.Tokens
			return true
		})
		return
	}
	// Re-calculate the average tokens for each slot.
	var (
		subTokens  = gts.getTokens() / float64(slotNum)
		fillRate   = settings.GetFillRate() / uint64(slotNum)
		burstLimit = settings.GetBurstLimit() / int64(slotNum)
	)
	gts.tokenSlots.Range(func(key, value interface{}) bool {
		slot := value.(*TokenSlot)
		slot.settings.FillRate = fillRate
		slot.settings.BurstLimit = burstLimit
		slot.tokens = subTokens
		return true
	})
}

func (gts *GroupTokenBucketState) getTokens() (sum float64) {
	gts.tokenSlots.Range(func(key, value interface{}) bool {
		sum += value.(*TokenSlot).tokens
		return true
	})
	return
}

// ingestTokens is used to increase or decrease the total and slot tokens.
func (gts *GroupTokenBucketState) ingestTokens(tokens float64) {
	subTokens := tokens / float64(len(gts.slots))
	gts.tokenSlots.Range(func(key, value interface{}) bool {
		slot := value.(*TokenSlot)
		slot.tokens += subTokens
		return true
	})
}

func (gts *GroupTokenBucketState) setTokens(tokens float64) {
	gts.Tokens = tokens
	subTokens := tokens / float64(len(gts.slots))
	gts.tokenSlots.Range(func(key, value interface{}) bool {
		slot := value.(*TokenSlot)
		slot.tokens = subTokens
		return true
	})
}

// GroupTokenBucket is a token bucket for a resource group.
type GroupTokenBucket struct {
	// Settings is the setting of TokenBucket.
	// BurstLimit is used as below:
	//   - If b == 0, that means the limiter is unlimited capacity. default use in resource controller (burst with a rate within an unlimited capacity).
	//   - If b < 0, that means the limiter is unlimited capacity and fillrate(r) is ignored, can be seen as r == Inf (burst within a unlimited capacity).
	//   - If b > 0, that means the limiter is limited capacity.
	// MaxTokens limits the number of tokens that can be accumulated
	Settings               *rmpb.TokenLimitSettings `json:"settings,omitempty"`
	*GroupTokenBucketState `json:"state,omitempty"`
}

// NewGroupTokenBucket returns a new GroupTokenBucket and runs the main loop.
func NewGroupTokenBucket(ctx context.Context, tokenBucket *rmpb.TokenBucket) *GroupTokenBucket {
	gtb := &GroupTokenBucket{
		Settings:              tokenBucket.GetSettings(),
		GroupTokenBucketState: newGroupTokenBucketState(tokenBucket.GetTokens()),
	}
	go gtb.mainLoop(ctx)
	return gtb
}

// GetTokenBucket returns the grpc protoc struct of GroupTokenBucket.
func (gtb *GroupTokenBucket) GetTokenBucket() *rmpb.TokenBucket {
	if gtb.Settings == nil {
		return nil
	}
	return &rmpb.TokenBucket{
		Settings: gtb.Settings,
		Tokens:   gtb.getTokens(),
	}
}

// mainLoop will start a goroutine to receive token requests from gRPC stream
// and dispatch them to different token slots. A token request could be re-sent
// to this loop multiple times if the token needed could not be satisfied. By
// this mechanism, we can implement a priority-based token consuming strategy.
func (gtb *GroupTokenBucket) mainLoop(ctx context.Context) {
	tokenCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		select {
		case <-tokenCtx.Done():
			return
		case req := <-gtb.requestChan:
			burstLimit := gtb.Settings.GetBurstLimit()
			gtb.updateTokens(req.now, burstLimit)
			res := &rmpb.TokenBucket{}
			// The fill rate will be used when the token server is in abnormal situation.
			if req.neededTokens <= 0 {
				req.finish(res, 0)
				continue
			}
			// If burst limit is -1, just return the needed tokens.
			if burstLimit < 0 {
				res.Tokens = req.neededTokens
				req.finish(res, 0)
				continue
			}
			switch req.typ {
			case arrogate:
				if slot := gtb.getMostTokensSlot(req.clientUniqueID); slot != nil {
					slot.requestSlotChan <- req
					continue
				}
				fallthrough
			case normal, loan:
				// Dispatch the request to the corresponding slot.
				gtb.getOrCreateTokenSlot(
					tokenCtx,
					req.clientUniqueID,
					gtb.Settings,
					gtb.requestChan,
				).requestSlotChan <- req
			}
		case tokenBucket := <-gtb.patchChan:
			settings := proto.Clone(tokenBucket.GetSettings()).(*rmpb.TokenLimitSettings)
			if settings == nil {
				continue
			}
			gtb.Settings = settings
			// Add delta between the last update and now.
			gtb.ingestTokens(tokenBucket.GetTokens())
			slotNum := len(gtb.slots)
			if slotNum < 1 {
				continue
			}
			var (
				fillRate   = settings.GetFillRate() / uint64(slotNum)
				burstLimit = settings.GetBurstLimit() / int64(slotNum)
			)
			gtb.tokenSlots.Range(func(key, value interface{}) bool {
				slot := value.(*TokenSlot)
				slot.settings.FillRate = fillRate
				slot.settings.BurstLimit = burstLimit
				if slot.tokens < 0 {
					slot.tokens = 0
				}
				return true
			})
		}
	}
}

// init initializes the group token bucket.
func (gtb *GroupTokenBucket) init(now time.Time) {
	if gtb.Settings != nil && gtb.Settings.FillRate == 0 {
		gtb.Settings.FillRate = defaultRefillRate
	}
	if gtb.Tokens < defaultInitialTokens {
		gtb.Tokens = defaultInitialTokens
	}
	gtb.LastUpdate = &now
	gtb.Initialized = true
}

func (gtb *GroupTokenBucket) updateTokens(now time.Time, burstLimit int64) {
	if !gtb.Initialized {
		gtb.init(now)
	} else if delta := now.Sub(*gtb.LastUpdate); delta > 0 {
		delta := float64(gtb.Settings.GetFillRate()) * delta.Seconds()
		gtb.ingestTokens(delta)
		gtb.LastUpdate = &now
	}
	if burst := float64(burstLimit); burst != 0 && gtb.getTokens() > burst {
		gtb.setTokens(burst)
	}
}

// patch patches the token bucket settings.
func (gtb *GroupTokenBucket) patch(tb *rmpb.TokenBucket) {
	if gtb == nil {
		return
	}
	gtb.patchChan <- tb
}

func (gtb *GroupTokenBucket) request(
	now time.Time,
	neededTokens float64,
	targetPeriodMs, clientUniqueID uint64,
) (*rmpb.TokenBucket, int64) {
	respChan := make(chan struct {
		resp          *rmpb.TokenBucket
		trickleTimeMs int64
	}, 1)
	gtb.requestChan <- NewTokenRequest(now, neededTokens, targetPeriodMs, clientUniqueID, respChan)
	resp := <-respChan
	return resp.resp, resp.trickleTimeMs
}
