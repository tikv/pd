// Copyright 2024 TiKV Project Authors.
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

package circuitbreaker

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/client/errs"
)

// advance emulate the state machine clock moves forward by the given duration
func (cb *CircuitBreaker) advance(duration time.Duration) {
	cb.state.end = cb.state.end.Add(-duration - 1)
}

var settings = Settings{
	ErrorRateThresholdPct: 50,
	MinQPSForOpen:         10,
	ErrorRateWindow:       30 * time.Second,
	CoolDownInterval:      10 * time.Second,
	HalfOpenSuccessCount:  2,
}

var minCountToOpen = int(settings.MinQPSForOpen * uint32(settings.ErrorRateWindow.Seconds()))

func TestCircuitBreakerExecuteWrapperReturnValues(t *testing.T) {
	re := require.New(t)
	cb := NewCircuitBreaker("test_cb", settings)
	originalError := errors.New("circuit breaker is open")

	err := cb.Execute(func() (Overloading, error) {
		return No, originalError
	})
	re.Equal(err, originalError)

	// same by interpret the result as overloading error
	err = cb.Execute(func() (Overloading, error) {
		return Yes, originalError
	})
	re.Equal(err, originalError)
}

func TestCircuitBreakerOpenState(t *testing.T) {
	re := require.New(t)
	cb := NewCircuitBreaker("test_cb", settings)
	driveQPS(cb, minCountToOpen, Yes, re)
	re.Equal(StateClosed, cb.state.stateType)
	assertSucceeds(cb, re) // no error till ErrorRateWindow is finished
	cb.advance(settings.ErrorRateWindow)
	assertFastFail(cb, re)
	re.Equal(StateOpen, cb.state.stateType)
}

func TestCircuitBreakerCloseStateNotEnoughQPS(t *testing.T) {
	re := require.New(t)
	cb := NewCircuitBreaker("test_cb", settings)
	re.Equal(StateClosed, cb.state.stateType)
	driveQPS(cb, minCountToOpen/2, Yes, re)
	cb.advance(settings.ErrorRateWindow)
	assertSucceeds(cb, re)
	re.Equal(StateClosed, cb.state.stateType)
}

func TestCircuitBreakerCloseStateNotEnoughErrorRate(t *testing.T) {
	re := require.New(t)
	cb := NewCircuitBreaker("test_cb", settings)
	re.Equal(StateClosed, cb.state.stateType)
	driveQPS(cb, minCountToOpen/4, Yes, re)
	driveQPS(cb, minCountToOpen, No, re)
	cb.advance(settings.ErrorRateWindow)
	assertSucceeds(cb, re)
	re.Equal(StateClosed, cb.state.stateType)
}

func TestCircuitBreakerHalfOpenToClosed(t *testing.T) {
	re := require.New(t)
	cb := NewCircuitBreaker("test_cb", settings)
	re.Equal(StateClosed, cb.state.stateType)
	driveQPS(cb, minCountToOpen, Yes, re)
	cb.advance(settings.ErrorRateWindow)
	assertFastFail(cb, re)
	re.Equal(StateOpen, cb.state.stateType)
	cb.advance(settings.CoolDownInterval)
	assertSucceeds(cb, re)
	re.Equal(StateHalfOpen, cb.state.stateType)
	assertSucceeds(cb, re)
	re.Equal(StateHalfOpen, cb.state.stateType)
	// state always transferred on the incoming request
	assertSucceeds(cb, re)
	re.Equal(StateClosed, cb.state.stateType)
}

func TestCircuitBreakerHalfOpenToOpen(t *testing.T) {
	re := require.New(t)
	cb := NewCircuitBreaker("test_cb", settings)
	re.Equal(StateClosed, cb.state.stateType)
	driveQPS(cb, minCountToOpen, Yes, re)
	cb.advance(settings.ErrorRateWindow)
	assertFastFail(cb, re)
	re.Equal(StateOpen, cb.state.stateType)
	cb.advance(settings.CoolDownInterval)
	assertSucceeds(cb, re)
	re.Equal(StateHalfOpen, cb.state.stateType)
	err := cb.Execute(func() (Overloading, error) {
		return Yes, nil // this trip circuit breaker again
	})
	re.NoError(err)
	re.Equal(StateHalfOpen, cb.state.stateType)
	// state always transferred on the incoming request
	assertFastFail(cb, re)
	re.Equal(StateOpen, cb.state.stateType)
}

// in half open state, circuit breaker will allow only HalfOpenSuccessCount pending and should fast fail all other request till HalfOpenSuccessCount requests is completed
// this test moves circuit breaker to the half open state and verifies that requests above HalfOpenSuccessCount are failing
func TestCircuitBreakerHalfOpenFailOverPendingCount(t *testing.T) {
	re := require.New(t)
	cb := newCircuitBreakerMovedToHalfOpenState(re)

	// the next request will move circuit breaker into the half open state
	var started []chan bool
	var waited []chan bool
	var ended []chan bool
	for range settings.HalfOpenSuccessCount {
		start := make(chan bool)
		wait := make(chan bool)
		end := make(chan bool)
		started = append(started, start)
		waited = append(waited, wait)
		ended = append(ended, end)
		go func() {
			defer func() {
				end <- true
			}()
			err := cb.Execute(func() (Overloading, error) {
				start <- true
				<-wait
				return No, nil
			})
			re.NoError(err)
		}()
	}
	// make sure all requests are started
	for i := range started {
		<-started[i]
	}
	// validate that requests beyond HalfOpenSuccessCount are failing
	assertFastFail(cb, re)
	re.Equal(StateHalfOpen, cb.state.stateType)
	// unblock pending requests and wait till they are completed
	for i := range ended {
		waited[i] <- true
		<-ended[i]
	}
	// validate that circuit breaker moves to closed state
	assertSucceeds(cb, re)
	re.Equal(StateClosed, cb.state.stateType)
	// make sure that after moving to open state all counters are reset
	re.Equal(uint32(1), cb.state.successCount)
}

func TestCircuitBreakerCountOnlyRequestsInSameWindow(t *testing.T) {
	re := require.New(t)
	cb := NewCircuitBreaker("test_cb", settings)
	re.Equal(StateClosed, cb.state.stateType)

	start := make(chan bool)
	wait := make(chan bool)
	end := make(chan bool)
	go func() {
		defer func() {
			end <- true
		}()
		err := cb.Execute(func() (Overloading, error) {
			start <- true
			<-wait
			return No, nil
		})
		re.NoError(err)
	}()
	<-start // make sure the request is started
	// assert running request is not counted
	re.Equal(uint32(0), cb.state.successCount)

	// advance request to the next window
	cb.advance(settings.ErrorRateWindow)
	assertSucceeds(cb, re)
	re.Equal(uint32(1), cb.state.successCount)

	// complete the request from the previous window
	wait <- true // resume
	<-end        // wait for the request to complete
	// assert request from last window is not counted
	re.Equal(uint32(1), cb.state.successCount)
}

func TestCircuitBreakerChangeSettings(t *testing.T) {
	re := require.New(t)

	cb := NewCircuitBreaker("test_cb", AlwaysClosedSettings)
	driveQPS(cb, int(AlwaysClosedSettings.MinQPSForOpen*uint32(AlwaysClosedSettings.ErrorRateWindow.Seconds())), Yes, re)
	cb.advance(AlwaysClosedSettings.ErrorRateWindow)
	assertSucceeds(cb, re)
	re.Equal(StateClosed, cb.state.stateType)

	cb.ChangeSettings(func(config *Settings) {
		config.ErrorRateThresholdPct = settings.ErrorRateThresholdPct
	})
	re.Equal(settings.ErrorRateThresholdPct, cb.config.ErrorRateThresholdPct)

	driveQPS(cb, minCountToOpen, Yes, re)
	cb.advance(settings.ErrorRateWindow)
	assertFastFail(cb, re)
	re.Equal(StateOpen, cb.state.stateType)
}

func TestCircuitBreakerEnabled(t *testing.T) {
	re := require.New(t)
	cb := NewCircuitBreaker("test_cb", AlwaysClosedSettings)
	re.False(cb.IsEnabled())
	cb.ChangeSettings(func(config *Settings) {
		config.ErrorRateThresholdPct = settings.ErrorRateThresholdPct
	})
	re.True(cb.IsEnabled())
}

func newCircuitBreakerMovedToHalfOpenState(re *require.Assertions) *CircuitBreaker {
	cb := NewCircuitBreaker("test_cb", settings)
	re.Equal(StateClosed, cb.state.stateType)
	driveQPS(cb, minCountToOpen, Yes, re)
	cb.advance(settings.ErrorRateWindow)
	assertFastFail(cb, re)
	re.Equal(StateOpen, cb.state.stateType)
	cb.advance(settings.CoolDownInterval)
	return cb
}

func driveQPS(cb *CircuitBreaker, count int, overload Overloading, re *require.Assertions) {
	for range count {
		err := cb.Execute(func() (Overloading, error) {
			return overload, nil
		})
		re.NoError(err)
	}
}

func assertFastFail(cb *CircuitBreaker, re *require.Assertions) {
	var executed = false
	err := cb.Execute(func() (Overloading, error) {
		executed = true
		return No, nil
	})
	re.Equal(err, errs.ErrCircuitBreakerOpen)
	re.False(executed)
}

func assertSucceeds(cb *CircuitBreaker, re *require.Assertions) {
	err := cb.Execute(func() (Overloading, error) {
		return No, nil
	})
	re.NoError(err)
}
