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
	"math/rand"
	"time"

	"github.com/gogo/protobuf/proto"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

const (
	defaultRefillRate      = 10000
	defaultInitialTokens   = 10 * 10000
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
	slots []uint64
	// ClientUniqueID -> Tokens
	tokenSlots map[uint64]float64
	// settingChanged is used to avoid that the number of tokens returned is jitter because of changing fill rate.
	settingChanged bool
	// The total tokens in the bucket should be equal to the sum of all tokens in the slots.
	Tokens      float64    `json:"tokens,omitempty"`
	LastUpdate  *time.Time `json:"last_update,omitempty"`
	Initialized bool       `json:"initialized"`
}

// newGroupTokenBucketState returns a new GroupTokenBucketState.
func newGroupTokenBucketState(tokens float64) GroupTokenBucketState {
	return GroupTokenBucketState{
		slots:      make([]uint64, 0),
		tokenSlots: make(map[uint64]float64),
		Tokens:     tokens,
	}
}

// Clone returns the copy of GroupTokenBucketState
func (s *GroupTokenBucketState) Clone() *GroupTokenBucketState {
	slots := make([]uint64, len(s.slots))
	copy(slots, s.slots)
	tokenSlots := make(map[uint64]float64)
	for id, tokens := range s.tokenSlots {
		tokenSlots[id] = tokens
	}
	var lastUpdate *time.Time
	if s.LastUpdate != nil {
		newLastUpdate := *s.LastUpdate
		lastUpdate = &newLastUpdate
	}
	return &GroupTokenBucketState{
		slots:       slots,
		tokenSlots:  tokenSlots,
		Tokens:      s.Tokens,
		LastUpdate:  lastUpdate,
		Initialized: s.Initialized,
	}
}

// NOTICE: all the slot-related operations SHOULD NOT change the total tokens in the bucket
// and it SHOULD make sure that the total tokens in the bucket is always equal to the sum of
// all tokens in the slots.
func (s *GroupTokenBucketState) balanceSlotTokens() {
	slots := len(s.tokenSlots)
	if slots == 0 {
		return
	}
	subTokens := s.Tokens / float64(slots)
	for id := range s.tokenSlots {
		s.tokenSlots[id] = subTokens
	}
}

func (s *GroupTokenBucketState) insertSlot(clientUniqueID uint64) {
	if _, ok := s.tokenSlots[clientUniqueID]; ok {
		return
	}
	s.slots = append(s.slots, clientUniqueID)
	if len(s.tokenSlots) == 0 {
		s.tokenSlots[clientUniqueID] = s.Tokens
		return
	}
	s.tokenSlots[clientUniqueID] = 0
	// Re-balance the tokens in the slots.
	s.balanceSlotTokens()
}

func (s *GroupTokenBucketState) removeSlot(clientUniqueIDs uint64) {
	if _, ok := s.tokenSlots[clientUniqueIDs]; !ok {
		return
	}
	for i, id := range s.slots {
		if id == clientUniqueIDs {
			s.slots = append(s.slots[:i], s.slots[i+1:]...)
			break
		}
	}
	delete(s.tokenSlots, clientUniqueIDs)
	// Re-balance the tokens in the slots.
	s.balanceSlotTokens()
}

// Ingest the tokens into the bucket and update the slot tokens as well.
// TODO: if the token given is negative, will it be a problem?
func (s *GroupTokenBucketState) ingestTokens(tokens float64) {
	s.Tokens += tokens
	slots := len(s.tokenSlots)
	if slots == 0 {
		return
	}
	subTokens := tokens / float64(slots)
	for id := range s.tokenSlots {
		s.tokenSlots[id] += subTokens
	}
}

func (s *GroupTokenBucketState) setTokens(tokens float64) {
	s.Tokens = tokens
	s.balanceSlotTokens()
}

// consumeTokens will first consume the tokens in its own slot, if the slot could not
// meet the demand, it will consume the tokens in other slots randomly. Please make
// sure the total tokens in the bucket is enough before calling this function.
func (s *GroupTokenBucketState) consumeTokens(clientUniqueID uint64, tokens float64) {
	s.insertSlot(clientUniqueID)
	s.Tokens -= tokens
	subTokens := s.getTokens(clientUniqueID)
	if subTokens >= tokens {
		s.tokenSlots[clientUniqueID] -= tokens
		return
	}
	// All tokens in the slot are consumed.
	s.tokenSlots[clientUniqueID] = 0
	// We still need the remaining tokens.
	neededTokens := tokens - subTokens
	// Randomly consume the tokens in other slots.
	slotsLen := len(s.slots)
	for neededTokens > 0 {
		id := s.slots[rand.Intn(slotsLen)]
		if id == clientUniqueID {
			continue
		}
		grantedTokens := math.Min(s.tokenSlots[id], neededTokens)
		neededTokens -= grantedTokens
		s.tokenSlots[id] -= grantedTokens
	}
}

func (s *GroupTokenBucketState) getTokens(clientUniqueIDs ...uint64) (tokens float64) {
	if len(clientUniqueIDs) == 0 {
		return s.Tokens
	}
	for _, id := range clientUniqueIDs {
		tokens += s.tokenSlots[id]
	}
	return tokens
}

// NewGroupTokenBucket returns a new GroupTokenBucket
func NewGroupTokenBucket(tokenBucket *rmpb.TokenBucket) GroupTokenBucket {
	if tokenBucket == nil || tokenBucket.Settings == nil {
		return GroupTokenBucket{
			GroupTokenBucketState: newGroupTokenBucketState(0),
		}
	}
	return GroupTokenBucket{
		Settings:              tokenBucket.Settings,
		GroupTokenBucketState: newGroupTokenBucketState(tokenBucket.Tokens),
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
	t.ingestTokens(tb.GetTokens())
}

// init initializes the group token bucket.
func (t *GroupTokenBucket) init(now time.Time) {
	if t.Settings.FillRate == 0 {
		t.Settings.FillRate = defaultRefillRate
	}
	if t.getTokens() < defaultInitialTokens {
		t.setTokens(defaultInitialTokens)
	}
	t.LastUpdate = &now
	t.Initialized = true
}

// request requests tokens from the group token bucket.
func (t *GroupTokenBucket) request(
	now time.Time,
	neededTokens float64,
	targetPeriodMs, clientUniqueID uint64,
) (*rmpb.TokenBucket, int64) {
	if !t.Initialized {
		t.init(now)
	} else {
		delta := now.Sub(*t.LastUpdate)
		if delta > 0 {
			t.ingestTokens(float64(t.Settings.FillRate) * delta.Seconds())
			t.LastUpdate = &now
		}
	}
	// reloan when setting changed
	if t.settingChanged && t.getTokens() <= 0 {
		t.setTokens(0)
	}
	t.settingChanged = false
	if t.Settings.BurstLimit != 0 {
		if burst := float64(t.Settings.BurstLimit); t.getTokens() > burst {
			t.setTokens(burst)
		}
	}

	var res rmpb.TokenBucket
	res.Settings = &rmpb.TokenLimitSettings{BurstLimit: t.Settings.GetBurstLimit()}
	// If BurstLimit is -1, just return.
	if res.Settings.BurstLimit < 0 {
		res.Tokens = neededTokens
		return &res, 0
	}
	// If the needed token number is less than 0, just return.
	if neededTokens <= 0 {
		return &res, 0
	}
	// Check whether the total tokens in the bucket could meet the requirement.
	totalTokens := t.getTokens()
	if totalTokens >= neededTokens {
		t.consumeTokens(clientUniqueID, neededTokens)
		res.Tokens = neededTokens
		return &res, 0
	}
	// Grant the remaining tokens and start to calculate the loan.
	var grantedTokens float64
	hasRemaining := false
	if totalTokens > 0 {
		grantedTokens = totalTokens
		neededTokens -= grantedTokens
		t.setTokens(0)
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
		loan := -t.getTokens()
		if loan > p[i] {
			continue
		}
		roundReserveTokens := p[i] - loan
		fillRate := float64(LoanCoefficient-i) * float64(t.Settings.FillRate)
		if roundReserveTokens > neededTokens {
			t.ingestTokens(-neededTokens)
			grantedTokens += neededTokens
			trickleTime += grantedTokens / fillRate
			neededTokens = 0
		} else {
			roundReserveTime := roundReserveTokens / fillRate
			if roundReserveTime+trickleTime >= targetPeriodTime.Seconds() {
				roundTokens := (targetPeriodTime.Seconds() - trickleTime) * fillRate
				neededTokens -= roundTokens
				t.ingestTokens(-roundTokens)
				grantedTokens += roundTokens
				trickleTime = targetPeriodTime.Seconds()
			} else {
				grantedTokens += roundReserveTokens
				neededTokens -= roundReserveTokens
				t.ingestTokens(-roundReserveTokens)
				trickleTime += roundReserveTime
			}
		}
	}
	if grantedTokens < defaultReserveRatio*float64(t.Settings.FillRate)*targetPeriodTime.Seconds() {
		t.ingestTokens(-(defaultReserveRatio*float64(t.Settings.FillRate)*targetPeriodTime.Seconds() - grantedTokens))
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
