// Copyright 2020 TiKV Project Authors.
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

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

const defaultRefillRate = 10000

const defaultInitialTokens = 10 * 10000

// GroupTokenBucket is a token bucket for a resource group.
type GroupTokenBucket struct {
	TokenBucketState TokenBucket               `json:"token_bucket"`
	Consumption      *rmpb.TokenBucketsRequest `json:"consumption"`
	LastUpdate       time.Time                 `json:"last_update"`
	Initialized      bool                      `json:"initialized"`
}

// Update updates the token bucket.
func (t *GroupTokenBucket) Update(now time.Time) {
	if !t.Initialized {
		t.TokenBucketState.Settings.Fillrate = defaultRefillRate
		t.TokenBucketState.Tokens = defaultInitialTokens
		t.LastUpdate = now
		t.Initialized = true
		return
	}

	delta := now.Sub(t.LastUpdate)
	if delta > 0 {
		t.TokenBucketState.Update(delta)
		t.LastUpdate = now
	}
}

// GetTokenBucket returns the token bucket.
func (t *GroupTokenBucket) GetTokenBucket() *rmpb.TokenBucket {
	return t.TokenBucketState.TokenBucket
}

// TokenBucket is a token bucket.
type TokenBucket struct {
	*rmpb.TokenBucket
}

// Update updates the token bucket.
func (s *TokenBucket) Update(sinceDuration time.Duration) {
	if sinceDuration > 0 {
		s.Tokens += float64(s.Settings.Fillrate) * sinceDuration.Seconds()
	}
}

// Request requests tokens from the token bucket.
func (s *TokenBucket) Request(
	neededTokens float64, targetPeriodMs uint64,
) *rmpb.TokenBucket {
	var res rmpb.TokenBucket
	// TODO: consider the shares for dispatch the fill rate
	res.Settings.Fillrate = s.Settings.Fillrate

	if neededTokens <= 0 {
		return &res
	}

	if s.Tokens >= neededTokens {
		s.Tokens -= neededTokens
		// granted the total request tokens
		res.Tokens = neededTokens
		return &res
	}

	var grantedTokens float64
	if s.Tokens > 0 {
		grantedTokens = s.Tokens
		neededTokens -= grantedTokens
	}

	availableRate := float64(s.Settings.Fillrate)
	if debt := -s.Tokens; debt > 0 {
		debt -= float64(s.Settings.Fillrate) * float64(targetPeriodMs) / 1000
		if debt > 0 {
			debtRate := debt / float64(targetPeriodMs/1000)
			availableRate -= debtRate
			availableRate = math.Max(availableRate, 0.05*s.Tokens)
		}
	}

	consumptionDuration := time.Duration(float64(time.Second) * (neededTokens / availableRate))
	targetDuration := time.Duration(targetPeriodMs/1000) * time.Second
	if consumptionDuration <= targetDuration {
		grantedTokens += neededTokens
	} else {
		grantedTokens += availableRate * targetDuration.Seconds()
	}
	s.Tokens -= grantedTokens
	res.Settings.Fillrate = uint64(availableRate)
	res.Tokens = grantedTokens
	return &res
}
