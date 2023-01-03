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
	"time"

	"github.com/gogo/protobuf/proto"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

const defaultInitialTokens = 10 * 10000

const defaultMaxTokens = 1e7

const defaultLoanMaxPeriod = 10 * time.Second

var loanReserveRatio float64 = 0.05

// GroupTokenBucket is a token bucket for a resource group.
// TODO: statistics Consumption
type GroupTokenBucket struct {
	*rmpb.TokenBucket `json:"token_bucket,omitempty"`
	Consumption       *rmpb.TokenBucketsRequest `json:"consumption,omitempty"`
	LastUpdate        *time.Time                `json:"last_update,omitempty"`
	Initialized       bool                      `json:"initialized"`
	LoanExpireTime    *time.Time                `json:"loan_time,omitempty"`
	LoanMaxPeriod     time.Duration             `json:"loan_max_perio,omitempty"`
	MaxTokens         float64                   `json:"max_tokens,omitempty"`
}

func NewGroupTokenBucket(tokenBucket *rmpb.TokenBucket) GroupTokenBucket {
	return GroupTokenBucket{
		TokenBucket:   tokenBucket,
		MaxTokens:     defaultMaxTokens,
		LoanMaxPeriod: defaultLoanMaxPeriod,
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
		if t.Tokens < defaultInitialTokens {
			t.Tokens = defaultInitialTokens
		}
		t.LastUpdate = &now
		t.Initialized = true
		return
	}

	delta := now.Sub(*t.LastUpdate)
	if delta > 0 {
		t.Tokens += float64(t.Settings.Fillrate) * delta.Seconds()
		t.LastUpdate = &now
	}
	if t.Tokens >= 0 {
		t.LoanExpireTime = nil
	}
	if t.Tokens > defaultMaxTokens {
		t.Tokens = defaultMaxTokens
	}
}

// request requests tokens from the token bucket.
func (t *GroupTokenBucket) request(
	neededTokens float64, targetPeriodMs uint64,
) (*rmpb.TokenBucket, int64) {
	var res rmpb.TokenBucket
	res.Settings = &rmpb.TokenLimitSettings{}
	// TODO: consider the shares for dispatch the fill rate
	res.Settings.Fillrate = 0
	if neededTokens <= 0 {
		return &res, 0
	}

	if t.Tokens >= neededTokens {
		t.Tokens -= neededTokens
		// granted the total request tokens
		res.Tokens = neededTokens
		return &res, 0
	}

	var grantedTokens float64
	if t.Tokens > 0 {
		grantedTokens = t.Tokens
		t.Tokens = 0
		neededTokens -= grantedTokens
	}

	var periodFilled float64
	var trickleTime = int64(targetPeriodMs)
	if t.LoanExpireTime != nil && t.LoanExpireTime.After(*t.LastUpdate) {
		duration := t.LoanExpireTime.Sub(*t.LastUpdate)
		periodFilled = float64(t.Settings.Fillrate) * (1 - loanReserveRatio) * duration.Seconds()
		trickleTime = duration.Milliseconds()
	} else {
		et := t.LastUpdate.Add(t.LoanMaxPeriod)
		t.LoanExpireTime = &et
		periodFilled = float64(t.Settings.Fillrate) * (1 - loanReserveRatio) * t.LoanMaxPeriod.Seconds()
	}
	periodFilled += t.Tokens
	if periodFilled <= float64(t.Settings.Fillrate)*loanReserveRatio {
		periodFilled = float64(t.Settings.Fillrate) * loanReserveRatio
	}
	if periodFilled >= neededTokens {
		grantedTokens += neededTokens
		t.Tokens -= neededTokens
	} else {
		grantedTokens += periodFilled
		t.Tokens -= periodFilled
	}
	res.Tokens = grantedTokens
	return &res, trickleTime
}
