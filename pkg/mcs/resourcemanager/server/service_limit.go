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

	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

// serviceLimiterBurstFactor defines how many seconds worth of tokens can be accumulated.
// This allows the limiter to handle batch requests from clients that request tokens periodically.
// Since the client will request tokens with a 5-second period by default, the burst factor is 5.0 here.
const serviceLimiterBurstFactor = 5.0

type serviceLimiter struct {
	syncutil.RWMutex
	// ServiceLimit is the configured service limit for this limiter.
	// It's an unburstable RU per second rate limit.
	ServiceLimit float64 `json:"service_limit"`
	// LastUpdate records the last time the limiter was updated.
	LastUpdate time.Time `json:"last_update"`
	// Theoretical arrival time of the next point where the service limit tokens can be granted.
	TAT time.Time `json:"tat"`
	// BurstWindow is the window of time that the service limit tokens can be granted.
	BurstWindow time.Duration `json:"burst_window"`
	// KeyspaceID is the keyspace ID of the keyspace that this limiter belongs to.
	keyspaceID uint32
	// storage is used to persist the service limit.
	storage endpoint.ResourceGroupStorage
}

func newServiceLimiter(keyspaceID uint32, serviceLimit float64, storage endpoint.ResourceGroupStorage) *serviceLimiter {
	// The service limit should be non-negative.
	serviceLimit = math.Max(0, serviceLimit)
	now := time.Now()
	return &serviceLimiter{
		ServiceLimit: serviceLimit,
		LastUpdate:   now,
		TAT:          now,
		BurstWindow:  serviceLimiterBurstFactor * time.Second,
		keyspaceID:   keyspaceID,
		storage:      storage,
	}
}

func (krl *serviceLimiter) setServiceLimit(now time.Time, newServiceLimit float64) {
	// The service limit should be non-negative.
	newServiceLimit = math.Max(0, newServiceLimit)
	krl.Lock()
	defer krl.Unlock()
	if newServiceLimit == krl.ServiceLimit {
		return
	}
	// Scale the TAT according to the new service limit, this helps to keep the TAT consistent with the service limit.
	if krl.TAT.After(now) && krl.ServiceLimit > 0 && newServiceLimit > 0 {
		krl.TAT = now.Add(
			time.Duration(
				// NewDebt = RemainingDebt * ScaleRatio
				float64(krl.TAT.Sub(now)) * krl.ServiceLimit / newServiceLimit,
			),
		)
	} else {
		krl.TAT = now
	}
	// Update the service limit and last update time.
	krl.ServiceLimit = newServiceLimit
	krl.LastUpdate = now

	// Persist the service limit to storage
	if krl.storage != nil {
		if err := krl.storage.SaveServiceLimit(krl.keyspaceID, newServiceLimit); err != nil {
			log.Error("failed to persist service limit",
				zap.Time("last-update", krl.LastUpdate),
				zap.Time("tat", krl.TAT),
				zap.Uint32("keyspace-id", krl.keyspaceID),
				zap.Float64("service-limit", newServiceLimit),
				zap.Error(err))
		}
	}
}

func (krl *serviceLimiter) getServiceLimit() float64 {
	if krl == nil {
		return 0.0
	}
	krl.RLock()
	defer krl.RUnlock()
	return krl.ServiceLimit
}

func (krl *serviceLimiter) perTokenIntervalInNanosLocked() float64 {
	if krl.ServiceLimit <= 0 {
		return 0.0
	}
	return 1e9 / krl.ServiceLimit
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
	// If the service limit is less than or equal to 0, it means no limit.
	if krl.ServiceLimit <= 0 {
		return requestedTokens
	}
	// Use GCRA(Generic Cell Rate Algorithm) to grant the tokens.
	base := krl.TAT
	if now.After(krl.TAT) {
		base = now
	}
	slack := now.Add(krl.BurstWindow).Sub(base)
	if slack <= 0 {
		return 0
	}
	perTokenInterval := krl.perTokenIntervalInNanosLocked()
	maxGrant := float64(slack) / perTokenInterval
	grant := math.Min(requestedTokens, maxGrant)
	if grant <= 0 {
		return 0
	}
	krl.TAT = base.Add(time.Duration(grant * perTokenInterval))

	return grant
}

// Clone returns a copy of the service limiter.
func (krl *serviceLimiter) Clone() *serviceLimiter {
	krl.RLock()
	defer krl.RUnlock()
	return &serviceLimiter{
		ServiceLimit: krl.ServiceLimit,
		LastUpdate:   krl.LastUpdate,
		TAT:          krl.TAT,
		BurstWindow:  krl.BurstWindow,
		keyspaceID:   krl.keyspaceID,
	}
}
