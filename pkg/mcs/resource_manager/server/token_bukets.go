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

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

const defaultRefillRate = 10000

const defaultInitialTokens = 10 * 10000

// GroupTokenBucket is a token bucket for a resource group.
type GroupTokenBucket struct {
	*rmpb.TokenBucket `json:"token_bucket,omitempty"`
	Consumption       *rmpb.TokenBucketsRequest `json:"consumption,omitempty"`
	LastUpdate        *time.Time                `json:"last_update,omitempty"`
	Initialized       bool                      `json:"initialized"`
}

// Update updates the token bucket.
func (t *GroupTokenBucket) Update(now time.Time) {
	if !t.Initialized {
		t.Settings.Fillrate = defaultRefillRate
		t.Tokens = defaultInitialTokens
		t.LastUpdate = &now
		t.Initialized = true
		return
	}

	delta := now.Sub(*t.LastUpdate)
	if delta > 0 {
		t.Tokens += float64(t.Settings.Fillrate) * delta.Seconds()
		t.LastUpdate = &now
	}
}

// GetTokenBucket returns the token bucket.
func (t *GroupTokenBucket) GetTokenBucket() *rmpb.TokenBucket {
	return t.TokenBucket
}

// Request requests tokens from the token bucket.
func (t *GroupTokenBucket) Request(
	neededTokens float64, targetPeriodMs uint64,
) *rmpb.TokenBucket {
	// TODO: Implement the token bucket algorithm.
	return nil
}
