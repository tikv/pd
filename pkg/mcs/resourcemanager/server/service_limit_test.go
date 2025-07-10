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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage"
)

func TestNewServiceLimiter(t *testing.T) {
	re := require.New(t)

	// Test creating a service limiter with positive limit
	limiter := newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	re.NotNil(limiter)
	re.Equal(100.0, limiter.ServiceLimit)
	re.Equal(0.0, limiter.AvailableTokens)

	// Test creating a service limiter with zero limit
	limiter = newServiceLimiter(constant.NullKeyspaceID, 0.0, nil)
	re.NotNil(limiter)
	re.Equal(0.0, limiter.ServiceLimit)
	re.Equal(0.0, limiter.AvailableTokens)

	// Test creating a service limiter with negative limit
	limiter = newServiceLimiter(constant.NullKeyspaceID, -10.0, nil)
	re.NotNil(limiter)
	re.Equal(0.0, limiter.ServiceLimit)
	re.Equal(0.0, limiter.AvailableTokens)
}

func TestServiceLimiterPersistence(t *testing.T) {
	re := require.New(t)

	// Create a storage backend for testing
	storage := storage.NewStorageWithMemoryBackend()

	// Test persisting service limit
	limiter := newServiceLimiter(1, 0.0, storage)
	limiter.setServiceLimit(100.5)

	// Verify the service limit was persisted
	loadedLimit, err := storage.LoadServiceLimit(1)
	re.NoError(err)
	re.Equal(100.5, loadedLimit)

	// Test updating the service limit
	limiter.setServiceLimit(200.5)
	loadedLimit, err = storage.LoadServiceLimit(1)
	re.NoError(err)
	re.Equal(200.5, loadedLimit)

	// Test loading non-existent service limit
	loadedLimit, err = storage.LoadServiceLimit(999)
	re.NoError(err)            // No error should be returned for non-existent limit
	re.Equal(0.0, loadedLimit) // Should return 0 for non-existent limit

	// Test loading service limits from storage
	for _, keyspaceID := range []uint32{1, 2, 3} {
		err = storage.SaveServiceLimit(keyspaceID, float64(keyspaceID)*100.0)
		re.NoError(err)
	}
	err = storage.LoadServiceLimits(func(keyspaceID uint32, serviceLimit float64) {
		re.Equal(float64(keyspaceID)*100.0, serviceLimit)
	})
	re.NoError(err)
}

func TestRefillTokensLocked(t *testing.T) {
	re := require.New(t)

	// Test refill with positive service limit
	limiter := newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	baseTime := time.Now()
	limiter.LastUpdate = baseTime
	limiter.AvailableTokens = 50.0

	// Refill after 1 second should add 100 tokens, but cap at burst limit (100 * serviceLimiterBurstFactor)
	futureTime := baseTime.Add(time.Second)
	limiter.refillTokensLocked(futureTime)
	re.Equal(150.0, limiter.AvailableTokens) // 50 + 100 = 150 (within burst limit)
	re.Equal(futureTime, limiter.LastUpdate)

	// Test refill when reach the burst limit
	limiter.AvailableTokens = 475.0
	limiter.LastUpdate = baseTime
	limiter.refillTokensLocked(futureTime)
	re.Equal(100*serviceLimiterBurstFactor, limiter.AvailableTokens) // Should remain at burst limit
	re.Equal(futureTime, limiter.LastUpdate)                         // Should update time even when no tokens added

	// Test partial refill
	limiter.AvailableTokens = 20.0
	limiter.LastUpdate = baseTime
	halfSecondLater := baseTime.Add(500 * time.Millisecond)
	limiter.refillTokensLocked(halfSecondLater)
	re.InDelta(70.0, limiter.AvailableTokens, 0.1) // 20 + 100*0.5 = 70
	re.Equal(halfSecondLater, limiter.LastUpdate)

	// Test with zero service limit
	limiter.ServiceLimit = 0.0
	limiter.AvailableTokens = 0.0
	limiter.LastUpdate = baseTime
	limiter.refillTokensLocked(futureTime)
	re.Equal(0.0, limiter.AvailableTokens) // Should remain 0
	re.Equal(baseTime, limiter.LastUpdate) // Should not update time

	// Test with elapsed time <= 0 (time going backwards)
	limiter.ServiceLimit = 100.0
	limiter.AvailableTokens = 50.0
	limiter.LastUpdate = futureTime
	limiter.refillTokensLocked(baseTime)     // Earlier time
	re.Equal(50.0, limiter.AvailableTokens)  // Should remain unchanged
	re.Equal(futureTime, limiter.LastUpdate) // Should not update time
}

func TestApplyServiceLimit(t *testing.T) {
	re := require.New(t)

	// Test with nil limiter
	var limiter *serviceLimiter
	tokens := limiter.applyServiceLimit(time.Now(), 50.0)
	re.Equal(50.0, tokens)

	// Test with zero service limit (no limit)
	limiter = newServiceLimiter(constant.NullKeyspaceID, 0.0, nil)
	now := time.Now()
	tokens = limiter.applyServiceLimit(now, 50.0)
	re.Equal(50.0, tokens)

	// Test request within available tokens (need to set available tokens first)
	limiter = newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	limiter.AvailableTokens = 100.0 // Manually set available tokens
	limiter.LastUpdate = now
	tokens = limiter.applyServiceLimit(now, 50.0)
	re.Equal(50.0, tokens)
	re.Equal(50.0, limiter.AvailableTokens) // 100 - 50 = 50

	// Test request exactly equal to available tokens
	limiter = newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	limiter.AvailableTokens = 100.0 // Manually set available tokens
	limiter.LastUpdate = now
	tokens = limiter.applyServiceLimit(now, 100.0)
	re.Equal(100.0, tokens)
	re.Equal(0.0, limiter.AvailableTokens)

	// Test request exceeding available tokens
	limiter = newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	limiter.LastUpdate = now
	limiter.AvailableTokens = 30.0
	tokens = limiter.applyServiceLimit(now, 80.0)
	re.Equal(30.0, tokens) // Only available tokens granted
	re.Equal(0.0, limiter.AvailableTokens)
}

func TestApplyServiceLimitWithRefill(t *testing.T) {
	re := require.New(t)

	// Test that refill happens before applying limit
	limiter := newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	baseTime := time.Now()
	limiter.LastUpdate = baseTime
	limiter.AvailableTokens = 20.0

	// Request after 1 second should trigger refill first
	futureTime := baseTime.Add(time.Second)
	tokens := limiter.applyServiceLimit(futureTime, 50.0)
	re.Equal(50.0, tokens)
	re.Equal(70.0, limiter.AvailableTokens) // 20 + 100 - 50 = 70
	re.Equal(futureTime, limiter.LastUpdate)

	// Test partial refill scenario
	limiter = newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	limiter.LastUpdate = baseTime
	limiter.AvailableTokens = 10.0

	halfSecondLater := baseTime.Add(500 * time.Millisecond)
	tokens = limiter.applyServiceLimit(halfSecondLater, 80.0)
	// After refill: 10 + 100*0.5 = 60 tokens available
	re.Equal(60.0, tokens)
	re.Equal(0.0, limiter.AvailableTokens)
}

func TestServiceLimiterEdgeCases(t *testing.T) {
	re := require.New(t)

	// Test with very small service limit
	limiter := newServiceLimiter(constant.NullKeyspaceID, 0.1, nil)
	limiter.AvailableTokens = 0.1   // Manually set available tokens
	limiter.LastUpdate = time.Now() // Set LastUpdate to current time to avoid refill
	now := time.Now()
	tokens := limiter.applyServiceLimit(now, 1.0)
	re.InDelta(0.1, tokens, 0.001) // Use InDelta to handle floating point precision

	// Test with very large service limit
	limiter = newServiceLimiter(constant.NullKeyspaceID, 1000000.0, nil)
	limiter.AvailableTokens = 1000000.0 // Manually set available tokens
	tokens = limiter.applyServiceLimit(now, 500000.0)
	re.Equal(500000.0, tokens)

	// Test with zero requested tokens
	limiter = newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	limiter.AvailableTokens = 100.0 // Manually set available tokens
	tokens = limiter.applyServiceLimit(now, 0.0)
	re.Equal(0.0, tokens)
	re.Equal(100.0, limiter.AvailableTokens) // Should remain unchanged

	// Test with fractional tokens
	limiter = newServiceLimiter(constant.NullKeyspaceID, 10.5, nil)
	limiter.LastUpdate = now
	limiter.AvailableTokens = 5.25
	tokens = limiter.applyServiceLimit(now, 7.75)
	re.Equal(5.25, tokens)
}

func TestSetServiceLimit(t *testing.T) {
	re := require.New(t)

	// Test setting the same service limit (should be no-op)
	limiter := newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	originalTokens := limiter.AvailableTokens
	originalUpdate := limiter.LastUpdate

	limiter.setServiceLimit(100.0) // Same limit
	re.Equal(100.0, limiter.ServiceLimit)
	re.Equal(originalTokens, limiter.AvailableTokens) // Should remain unchanged
	re.Equal(originalUpdate, limiter.LastUpdate)      // Should remain unchanged

	// Test increasing service limit
	limiter = newServiceLimiter(constant.NullKeyspaceID, 50.0, nil)
	baseTime := time.Now()
	limiter.LastUpdate = baseTime
	limiter.AvailableTokens = 30.0

	limiter.setServiceLimit(100.0)
	re.Equal(100.0, limiter.ServiceLimit)
	re.InDelta(30.0, limiter.AvailableTokens, 0.1) // Should remain the same since no time elapsed (allow for floating point precision)
	re.True(limiter.LastUpdate.After(baseTime))    // Should update time

	// Test decreasing service limit with available tokens exceeding new limit
	limiter = newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	baseTime = time.Now()
	limiter.LastUpdate = baseTime
	limiter.AvailableTokens = 80.0

	// Sleep a bit to ensure some time passes
	time.Sleep(1 * time.Millisecond)
	limiter.setServiceLimit(50.0)
	re.Equal(50.0, limiter.ServiceLimit)
	// After refill, tokens might exceed 80, but should be capped at burst limit (50 * serviceLimiterBurstFactor)
	re.Greater(limiter.AvailableTokens, 80.0)                               // Should increase the available tokens
	re.LessOrEqual(limiter.AvailableTokens, 50.0*serviceLimiterBurstFactor) // Should not exceed burst limit
	re.True(limiter.LastUpdate.After(baseTime))                             // Should update time

	// Test decreasing service limit with available tokens below new limit
	limiter = newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	baseTime = time.Now()
	limiter.LastUpdate = baseTime
	limiter.AvailableTokens = 30.0

	limiter.setServiceLimit(50.0)
	re.Equal(50.0, limiter.ServiceLimit)
	re.InDelta(30.0, limiter.AvailableTokens, 0.1) // Should remain unchanged since below new limit (allow for floating point precision)
	re.True(limiter.LastUpdate.After(baseTime))    // Should update time

	// Test setting service limit to zero
	limiter = newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	baseTime = time.Now()
	limiter.LastUpdate = baseTime
	limiter.AvailableTokens = 50.0

	limiter.setServiceLimit(0.0)
	re.Equal(0.0, limiter.ServiceLimit)
	re.Equal(0.0, limiter.AvailableTokens)      // Should be cleared
	re.True(limiter.LastUpdate.After(baseTime)) // Should update time

	// Test setting service limit from zero to positive (should NOT initialize available tokens)
	limiter = newServiceLimiter(constant.NullKeyspaceID, 0.0, nil)
	baseTime = time.Now()
	limiter.LastUpdate = baseTime
	limiter.AvailableTokens = 0.0

	limiter.setServiceLimit(50.0)
	re.Equal(50.0, limiter.ServiceLimit)
	re.Equal(0.0, limiter.AvailableTokens)      // Should remain 0 (not initialized to service limit)
	re.True(limiter.LastUpdate.After(baseTime)) // Should update time

	// Test setting negative service limit (should be treated as zero)
	limiter = newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	baseTime = time.Now()
	limiter.LastUpdate = baseTime
	limiter.AvailableTokens = 50.0

	limiter.setServiceLimit(-10.0)
	re.Equal(0.0, limiter.ServiceLimit)         // Should be treated as zero
	re.Equal(0.0, limiter.AvailableTokens)      // Should be cleared
	re.True(limiter.LastUpdate.After(baseTime)) // Should update time

	// Test setting service limit with time elapsed (should trigger refill)
	limiter = newServiceLimiter(constant.NullKeyspaceID, 50.0, nil)
	baseTime = time.Now()
	limiter.LastUpdate = baseTime
	limiter.AvailableTokens = 20.0

	// Simulate time passing before setting new limit
	time.Sleep(10 * time.Millisecond)
	limiter.setServiceLimit(100.0)
	re.Equal(100.0, limiter.ServiceLimit)
	// Available tokens should be refilled based on elapsed time, but capped at new service limit
	re.GreaterOrEqual(limiter.AvailableTokens, 20.0) // Should be at least the original amount
	re.LessOrEqual(limiter.AvailableTokens, 100.0)   // Should not exceed new service limit
	re.True(limiter.LastUpdate.After(baseTime))      // Should update time

	// Test setting a smaller service limit
	limiter = newServiceLimiter(constant.NullKeyspaceID, 1000.0, nil)
	baseTime = time.Now()
	limiter.LastUpdate = baseTime
	limiter.AvailableTokens = 500.0

	limiter.setServiceLimit(10.0)
	re.Equal(10.0, limiter.ServiceLimit)
	// After refill with new limit, tokens should be capped at burst limit (10 * serviceLimiterBurstFactor)
	re.Equal(10.0*serviceLimiterBurstFactor, limiter.AvailableTokens) // Should be capped at burst limit, not service limit
	re.True(limiter.LastUpdate.After(baseTime))                       // Should update time

	// Test setting a larger service limit
	limiter = newServiceLimiter(constant.NullKeyspaceID, 10.0, nil)
	baseTime = time.Now()
	limiter.LastUpdate = baseTime
	limiter.AvailableTokens = 5.0

	limiter.setServiceLimit(1000000.0)
	re.Equal(1000000.0, limiter.ServiceLimit)
	re.Greater(limiter.AvailableTokens, 5.0)
	re.True(limiter.LastUpdate.After(baseTime))
}
