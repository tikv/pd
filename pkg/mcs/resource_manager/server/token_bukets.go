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
	"fmt"
	"math"
	"time"

	"github.com/gogo/protobuf/proto"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

const defaultRefillRate = 10000

const defaultInitialTokens = 10 * 10000

const defaultMaxTokens = 1e7

var reserveRatio float64 = 0.05

// GroupTokenBucket is a token bucket for a resource group.
// TODO: statistics Consumption
type GroupTokenBucket struct {
	*rmpb.TokenBucket `json:"token_bucket,omitempty"`
	// MaxTokens limits the number of tokens that can be accumulated
	MaxTokens float64 `json:"max_tokens,omitempty"`

	Consumption *rmpb.TokenBucketsRequest `json:"consumption,omitempty"`
	LastUpdate  *time.Time                `json:"last_update,omitempty"`
	Initialized bool                      `json:"initialized"`
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

// update updates the token bucket.
func (t *GroupTokenBucket) update(now time.Time) {
	if !t.Initialized {
		if t.Settings.FillRate == 0 {
			t.Settings.FillRate = defaultRefillRate
		}
		if t.Tokens < defaultInitialTokens {
			t.Tokens = defaultInitialTokens
		}
		t.LastUpdate = &now
		t.Initialized = true
		return
	}

	delta := now.Sub(*t.LastUpdate)
	if delta > 0 {
		t.Tokens += float64(t.Settings.FillRate) * delta.Seconds()
		t.LastUpdate = &now
	}
	if t.Tokens > t.MaxTokens {
		t.Tokens = t.MaxTokens
	}
}

// request requests tokens from the token bucket.
func (t *GroupTokenBucket) request(
	neededTokens float64, targetPeriodMs uint64,
) (*rmpb.TokenBucket, int64) {
	var res rmpb.TokenBucket
	res.Settings = &rmpb.TokenLimitSettings{}
	// FillRate is used for the token server unavailable in abnormal situation.
	res.Settings.FillRate = 0
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
	if t.Tokens > 0 {
		grantedTokens = t.Tokens
		neededTokens -= grantedTokens
	}

	var trickleTime = time.Duration(targetPeriodMs) * time.Millisecond
	availableRate := float64(t.Settings.FillRate)
	if debt := -t.Tokens; debt > 0 {
		fmt.Println(debt)
		debt -= float64(t.Settings.FillRate) * trickleTime.Seconds()
		if debt > 0 {
			debtRate := debt / float64(targetPeriodMs/1000)
			availableRate -= debtRate
			availableRate = math.Max(availableRate, reserveRatio*float64(t.Settings.FillRate))
		}
	}

	consumptionDuration := time.Duration(float64(time.Second) * (neededTokens / availableRate))
	if consumptionDuration <= trickleTime {
		grantedTokens += neededTokens
	} else {
		grantedTokens += availableRate * trickleTime.Seconds()
	}
	t.Tokens -= grantedTokens
	res.Tokens = grantedTokens
	return &res, trickleTime.Milliseconds()
}
