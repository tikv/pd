// Copyright 2025 TiKV Project Authors.
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

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/utils/syncutil"
)

// serviceLimiterBurstFactor defines how many seconds worth of tokens can be accumulated.
// This allows the limiter to handle batch requests from clients that request tokens periodically.
// Since the client will request tokens with a 5-second period by default, the burst factor is 5.0 here.
const serviceLimiterBurstFactor = 5.0

// TODO: persist the service limit to the storage and reload it when the service starts.
type serviceLimiter struct {
	syncutil.RWMutex
	// ServiceLimit is the configured service limit for this limiter.
	// It's an unburstable RU per second rate limit.
	ServiceLimit float64 `json:"service_limit"`
	// AvailableTokens tracks the available RU tokens for this limiter.
	AvailableTokens float64 `json:"available_tokens"`
	// LastUpdate records the last time the limiter was updated.
	LastUpdate time.Time `json:"last_update"`
	// KeyspaceID is the keyspace ID of the keyspace that this limiter belongs to.
	keyspaceID uint32
}

func newServiceLimiter(keyspaceID uint32, serviceLimit float64) *serviceLimiter {
	// The service limit should be non-negative.
	serviceLimit = math.Max(0, serviceLimit)
	return &serviceLimiter{
		ServiceLimit: serviceLimit,
		LastUpdate:   time.Now(),
		keyspaceID:   keyspaceID,
	}
}

func (krl *serviceLimiter) setServiceLimit(newServiceLimit float64) {
	// The service limit should be non-negative.
	newServiceLimit = math.Max(0, newServiceLimit)
	krl.Lock()
	defer krl.Unlock()
	if newServiceLimit == krl.ServiceLimit {
		return
	}
	oldServiceLimit := krl.ServiceLimit
	krl.ServiceLimit = newServiceLimit
	now := time.Now()
	// If the old service limit was 0 (no limit) or the new service limit is 0,
	// initialize the available tokens or clear the available tokens.
	if oldServiceLimit == 0 || newServiceLimit == 0 {
		krl.AvailableTokens = 0
		krl.LastUpdate = now
	} else {
		// Trigger the refill of the available tokens to ensure that the previous
		// service limit setting was not too high, so that many available tokens
		// are not left unused, causing the new service limit to become invalid.
		krl.refillTokensLocked(now)
	}
}

func (krl *serviceLimiter) refillTokensLocked(now time.Time) {
	// No limit configured, do nothing.
	if krl.ServiceLimit <= 0 {
		return
	}
	// Calculate the elapsed time since the last update.
	elapsed := now.Sub(krl.LastUpdate).Seconds()
	if elapsed < 0 {
		log.Warn("refill service limit tokens with negative elapsed time",
			zap.Uint32("keyspace-id", krl.keyspaceID),
			zap.Float64("service_limit", krl.ServiceLimit),
			zap.Float64("available_tokens", krl.AvailableTokens),
			zap.Float64("elapsed", elapsed),
		)
		return
	}
	// Add tokens based on the configured rate and the burst limit.
	krl.AvailableTokens = math.Min(
		krl.AvailableTokens+krl.ServiceLimit*elapsed,
		krl.ServiceLimit*serviceLimiterBurstFactor,
	)
	// Update the last update time.
	krl.LastUpdate = now
}

// applyServiceLimit applies the service limit to the requested tokens and returns the limited tokens.
func (krl *serviceLimiter) applyServiceLimit(
	now time.Time,
	requestedTokens float64,
) (limitedTokens float64) {
	if krl == nil {
		return requestedTokens
	}
	krl.Lock()
	defer krl.Unlock()

	// No limit configured, allow all tokens.
	if krl.ServiceLimit <= 0 {
		return requestedTokens
	}

	// Refill first to ensure the available tokens is up to date.
	krl.refillTokensLocked(now)

	// If the requested tokens is less than the available tokens, grant all tokens.
	if requestedTokens <= krl.AvailableTokens {
		krl.AvailableTokens -= requestedTokens
		return requestedTokens
	}

	// If the requested tokens is greater than the available tokens, grant all available tokens.
	if krl.AvailableTokens > 0 && requestedTokens > krl.AvailableTokens {
		limitedTokens = krl.AvailableTokens
		// TODO: allow the loan to decrease the allocation at a smooth rate.
		krl.AvailableTokens = 0
	}

	return limitedTokens
}

// Clone returns a copy of the service limiter.
func (krl *serviceLimiter) Clone() *serviceLimiter {
	krl.RLock()
	defer krl.RUnlock()
	return &serviceLimiter{
		ServiceLimit:    krl.ServiceLimit,
		AvailableTokens: krl.AvailableTokens,
		LastUpdate:      krl.LastUpdate,
		keyspaceID:      krl.keyspaceID,
	}
}
