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

	for _, serviceLimit := range []float64{100.0, 0.0, -10.0} {
		now := time.Now()
		limiter := newServiceLimiter(constant.NullKeyspaceID, serviceLimit, nil)
		re.NotNil(limiter)
		if serviceLimit > 0 {
			re.Equal(serviceLimit, limiter.ServiceLimit)
		} else {
			re.Equal(0.0, limiter.ServiceLimit)
		}
		re.Equal(constant.NullKeyspaceID, limiter.keyspaceID)
		re.Equal(serviceLimiterBurstFactor*time.Second, limiter.BurstWindow.Duration)
		// TAT should be initialized to now
		re.GreaterOrEqual(limiter.TAT.Sub(now), time.Duration(0))
		re.Less(limiter.TAT.Sub(now), time.Second) // Should be very close to now
	}
}

func TestServiceLimiterPersistence(t *testing.T) {
	re := require.New(t)

	// Create a storage backend for testing
	storage := storage.NewStorageWithMemoryBackend()

	// Test persisting service limit
	limiter := newServiceLimiter(1, 0.0, storage)
	limiter.setServiceLimit(time.Now(), 100.5)

	// Verify the service limit was persisted
	loadedLimit, err := storage.LoadServiceLimit(1)
	re.NoError(err)
	re.Equal(100.5, loadedLimit)

	// Test updating the service limit
	limiter.setServiceLimit(time.Now(), 200.5)
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

func TestGetServiceLimit(t *testing.T) {
	re := require.New(t)

	// Test with nil limiter
	var limiter *serviceLimiter
	limit, tat, slack := limiter.getStates()
	re.Zero(limit)
	re.Zero(tat)
	re.Zero(slack)

	// Test with valid limiter
	limiter = newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	now := time.Now()
	limit, tat, slack = limiter.getStates()
	re.Equal(100.0, limit)
	re.GreaterOrEqual(now.Sub(tat), time.Duration(0))
	re.Less(now.Sub(tat), time.Second)
	re.Zero(slack)

	// Test after updating service limit
	now = time.Now()
	limiter.setServiceLimit(now, 200.0)
	limit, tat, slack = limiter.getStates()
	re.Equal(200.0, limit)
	re.GreaterOrEqual(now.Sub(tat), time.Duration(0))
	re.Less(now.Sub(tat), time.Second)
	re.Zero(slack)
}

func TestPerTokenIntervalLocked(t *testing.T) {
	re := require.New(t)

	// Test with positive service limit
	limiter := newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	interval := limiter.perTokenIntervalInNanosLocked()
	re.Equal(1e9/100.0, interval)

	// Test with zero service limit
	limiter.ServiceLimit = 0.0
	interval = limiter.perTokenIntervalInNanosLocked()
	re.Zero(interval)

	// Test with fractional service limit
	limiter.ServiceLimit = 2.5
	interval = limiter.perTokenIntervalInNanosLocked()
	re.Equal(1e9/2.5, interval)
}

func TestApplyServiceLimit(t *testing.T) {
	re := require.New(t)

	now := time.Now()
	// Test with nil limiter
	var limiter *serviceLimiter
	tokens := limiter.applyServiceLimit(now, 50.0)
	re.Equal(50.0, tokens) // nil limiter should return as much as requested

	// Test with zero service limit (no limit)
	limiter = newServiceLimiter(constant.NullKeyspaceID, 0.0, nil)
	tokens = limiter.applyServiceLimit(now, 50.0)
	re.Equal(50.0, tokens) // No limit means full grant

	// Test with zero requested tokens
	limiter = newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	tokens = limiter.applyServiceLimit(now, 0.0)
	re.Equal(0.0, tokens) // Zero request should return zero

	// Test with negative requested tokens
	tokens = limiter.applyServiceLimit(now, -10.0)
	re.Equal(0.0, tokens) // Negative request should return zero

	// Test initial burst capacity - should be able to grant up to burst window worth of tokens
	limiter = newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	now = limiter.TAT
	// Initially TAT equals now, so we should have full burst capacity
	burstCapacity := 100.0 * serviceLimiterBurstFactor // 500 tokens
	tokens = limiter.applyServiceLimit(now, burstCapacity)
	re.InDelta(burstCapacity, tokens, 0.1)

	// After granting burst capacity, TAT should be advancedg
	expectedTAT := now.Add(time.Duration(burstCapacity * limiter.perTokenIntervalInNanosLocked()))
	re.Equal(expectedTAT, limiter.TAT)

	// Test request exceeding burst capacity
	limiter = newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	excessiveRequest := burstCapacity + 100.0
	tokens = limiter.applyServiceLimit(now, excessiveRequest)
	re.InDelta(burstCapacity, tokens, 0.1) // Should be capped at burst capacity
}

func TestApplyServiceLimitWithTimeProgression(t *testing.T) {
	re := require.New(t)

	// Test GCRA behavior with time progression
	limiter := newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)

	now := time.Now()
	// First request - should exhaust burst capacity
	burstCapacity := 100.0 * serviceLimiterBurstFactor // 500 tokens
	tokens := limiter.applyServiceLimit(now, burstCapacity)
	re.InDelta(burstCapacity, tokens, 0.1)

	// Immediate second request should be denied (no slack available)
	tokens = limiter.applyServiceLimit(now, 50.0)
	re.Equal(0.0, tokens)

	// After 1 second, should be able to get 100 more tokens (rate limit)
	oneSecondLater := now.Add(time.Second)
	tokens = limiter.applyServiceLimit(oneSecondLater, 150.0) // Request more than available
	re.Equal(100.0, tokens)                                   // Should only get what's available (100 tokens per second)

	// After another second, should get another 100 tokens
	twoSecondsLater := oneSecondLater.Add(time.Second)
	tokens = limiter.applyServiceLimit(twoSecondsLater, 100.0)
	re.Equal(100.0, tokens)

	// Test partial time progression
	limiter = newServiceLimiter(constant.NullKeyspaceID, 200.0, nil) // 200 tokens/second
	// Exhaust initial burst
	burstCapacity = 200.0 * serviceLimiterBurstFactor // 1000 tokens
	now = time.Now()
	tokens = limiter.applyServiceLimit(now, burstCapacity)
	re.InDelta(burstCapacity, tokens, 0.1)

	// After 0.5 seconds, should get 100 tokens (200 * 0.5)
	halfSecondLater := now.Add(500 * time.Millisecond)
	tokens = limiter.applyServiceLimit(halfSecondLater, 150.0)
	re.Equal(100.0, tokens) // Should get 100 tokens (0.5s * 200 tokens/s)
}

func TestServiceLimiterEdgeCases(t *testing.T) {
	re := require.New(t)

	// Test with very small service limit
	limiter := newServiceLimiter(constant.NullKeyspaceID, 0.1, nil)
	// With 0.1 tokens/second, burst capacity is 0.1 * 5 = 0.5 tokens
	burstCapacity := 0.1 * serviceLimiterBurstFactor
	now := time.Now()
	tokens := limiter.applyServiceLimit(now, 1.0) // Request more than burst capacity
	re.InDelta(burstCapacity, tokens, 0.1)        // Should get burst capacity

	// Test with very large service limit
	limiter = newServiceLimiter(constant.NullKeyspaceID, 1000000.0, nil)
	now = time.Now()
	tokens = limiter.applyServiceLimit(now, 500000.0)
	re.Equal(500000.0, tokens) // Request is within burst capacity

	// Test with fractional service limit and fractional requests
	limiter = newServiceLimiter(constant.NullKeyspaceID, 10.5, nil)
	now = time.Now()
	tokens = limiter.applyServiceLimit(now, 7.75)
	re.Equal(7.75, tokens) // Should get exactly what was requested

	// Test consecutive small requests
	tokens = limiter.applyServiceLimit(now, 5.25)
	re.Equal(5.25, tokens)

	// Now should have used 13 tokens total, leaving 39.5 tokens in burst capacity
	tokens = limiter.applyServiceLimit(now, 40.0)
	re.InDelta(39.5, tokens, 0.1) // Should get remaining burst capacity

	// Test time going backwards (should not cause issues)
	limiter = newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	futureTime := now.Add(time.Second)
	pastTime := now.Add(-time.Second)

	// First request at future time
	tokens = limiter.applyServiceLimit(futureTime, 100.0)
	re.Equal(100.0, tokens)

	// Request at past time should still work (uses current TAT)
	tokens = limiter.applyServiceLimit(pastTime, 50.0)
	re.Equal(50.0, tokens) // Should still be able to grant based on current TAT
}

func TestSetServiceLimit(t *testing.T) {
	re := require.New(t)

	// Test setting the same service limit (should be no-op)
	limiter := newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	originalTAT := limiter.TAT
	originalLastUpdate := limiter.LastUpdate

	limiter.setServiceLimit(time.Now(), 100.0) // Same limit
	re.Equal(100.0, limiter.ServiceLimit)
	re.Equal(originalTAT, limiter.TAT)               // TAT should remain unchanged
	re.Equal(originalLastUpdate, limiter.LastUpdate) // LastUpdate should remain unchanged

	// Test setting service limit to zeroa
	limiter = newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	originalLastUpdate = limiter.LastUpdate

	limiter.setServiceLimit(time.Now(), 0.0)
	re.Equal(0.0, limiter.ServiceLimit)
	re.GreaterOrEqual(limiter.LastUpdate.Sub(originalLastUpdate), time.Duration(0)) // Should update time (allow equal time)
	// TAT should be reset to now when setting to zero
	re.GreaterOrEqual(limiter.TAT.Sub(limiter.LastUpdate), time.Duration(0))
	re.Less(limiter.TAT.Sub(limiter.LastUpdate), time.Millisecond)

	// Test setting service limit from zero to positive
	limiter = newServiceLimiter(constant.NullKeyspaceID, 0.0, nil)
	originalLastUpdate = limiter.LastUpdate

	limiter.setServiceLimit(time.Now(), 50.0)
	re.Equal(50.0, limiter.ServiceLimit)
	re.GreaterOrEqual(limiter.LastUpdate.Sub(originalLastUpdate), time.Duration(0)) // Should update time (allow equal time)
	// TAT should be reset to now
	re.GreaterOrEqual(limiter.TAT.Sub(limiter.LastUpdate), time.Duration(0))
	re.Less(limiter.TAT.Sub(limiter.LastUpdate), time.Millisecond)

	// Test setting negative service limit (should be treated as zero)
	limiter = newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	originalLastUpdate = limiter.LastUpdate

	limiter.setServiceLimit(time.Now(), -10.0)
	re.Equal(0.0, limiter.ServiceLimit)                                             // Should be treated as zero
	re.GreaterOrEqual(limiter.LastUpdate.Sub(originalLastUpdate), time.Duration(0)) // Should update time (allow equal time)
}

func TestSetServiceLimitTATScaling(t *testing.T) {
	re := require.New(t)

	// Test TAT scaling when increasing service limit
	limiter := newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	now := limiter.TAT

	// First, consume some tokens to advance TAT
	tokens := limiter.applyServiceLimit(now, 200.0) // Consume 200 tokens
	re.Equal(200.0, tokens)

	// TAT should now be 2 seconds in the future (200 tokens / 100 tokens per second)
	expectedTAT := now.Add(2 * time.Second)
	re.Equal(expectedTAT, limiter.TAT)

	// Now double the service limit to 200 tokens/second
	limiter.setServiceLimit(now, 200.0)

	// TAT should be scaled down proportionally: 2s * (100/200) = 1s in the future
	expectedScaledTAT := now.Add(1 * time.Second)
	re.Equal(expectedScaledTAT, limiter.TAT)

	// Test TAT scaling when decreasing service limit
	limiter = newServiceLimiter(constant.NullKeyspaceID, 200.0, nil)
	now = limiter.TAT

	// Consume tokens to advance TAT by 1 second
	tokens = limiter.applyServiceLimit(now, 200.0) // 200 tokens at 200/s = 1 second
	re.Equal(200.0, tokens)

	// Halve the service limit to 100 tokens/second
	limiter.setServiceLimit(now, 100.0)

	// TAT should remain the same since the service limit is not increased
	expectedScaledTAT = now.Add(1 * time.Second)
	re.Equal(expectedScaledTAT, limiter.TAT)

	// Test TAT scaling with zero old service limit (should reset TAT)
	limiter = newServiceLimiter(constant.NullKeyspaceID, 0.0, nil)
	now = limiter.TAT
	// Advance TAT artificially
	limiter.TAT = now.Add(5 * time.Second)

	limiter.setServiceLimit(now, 100.0)
	// TAT should be reset to current time since old service limit was 0
	re.Equal(now, limiter.TAT)

	// Test TAT scaling when setting to zero service limit
	limiter = newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	now = limiter.TAT
	// Advance TAT
	limiter.applyServiceLimit(now, 100.0)

	limiter.setServiceLimit(now, 0.0)
	// TAT should be reset to current time
	re.Equal(now, limiter.TAT)

	// Test TAT scaling when TAT is in the past
	limiter = newServiceLimiter(constant.NullKeyspaceID, 100.0, nil)
	now = limiter.TAT
	pastTime := now.Add(-5 * time.Second)
	limiter.TAT = pastTime // Set TAT to past

	limiter.setServiceLimit(now, 200.0)
	// TAT should remain the same since scaling only happens when the service limit is increased and the TAT is in the future.
	re.Equal(pastTime, limiter.TAT)
}

func TestServiceLimiterClone(t *testing.T) {
	re := require.New(t)
	now := time.Now()

	// Test cloning a limiter
	original := newServiceLimiter(1, 100.0, nil)
	// Modify the original state
	original.applyServiceLimit(now, 50.0)

	clone := original.clone()

	// Verify all fields are copied correctly
	re.Equal(original.ServiceLimit, clone.ServiceLimit)
	re.Equal(original.LastUpdate, clone.LastUpdate)
	re.Equal(original.TAT, clone.TAT)
	re.Equal(original.BurstWindow, clone.BurstWindow)
	re.Equal(original.keyspaceID, clone.keyspaceID)

	// Verify clone is independent (no shared storage reference)
	re.Nil(clone.storage) // Storage should not be copied

	// Verify modifying clone doesn't affect original
	clone.setServiceLimit(now, 200.0)
	re.Equal(100.0, original.ServiceLimit) // Original should be unchanged
	re.Equal(200.0, clone.ServiceLimit)    // Clone should be changed
}
