// Copyright 2024 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package circuitbreaker

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/client/errs"
	m "github.com/tikv/pd/client/metrics"
)

// Overloading is a type describing service return value
type Overloading bool

const (
	// No means the service is not overloaded
	No = false
	// Yes means the service is overloaded
	Yes = true
)

// Settings describes configuration for Circuit Breaker
type Settings struct {
	// Defines the error rate threshold to trip the circuit breaker.
	ErrorRateThresholdPct uint32
	// Defines the average qps over the `error_rate_window` that must be met before evaluating the error rate threshold.
	MinQPSForOpen uint32
	// Defines how long to track errors before evaluating error_rate_threshold.
	ErrorRateWindow time.Duration
	// Defines how long to wait after circuit breaker is open before go to half-open state to send a probe request.
	CoolDownInterval time.Duration
	// Defines how many subsequent requests to test after cooldown period before fully close the circuit.
	HalfOpenSuccessCount uint32
}

// AlwaysClosedSettings is a configuration that never trips the circuit breaker.
var AlwaysClosedSettings = Settings{
	ErrorRateThresholdPct: 0,                // never trips
	ErrorRateWindow:       10 * time.Second, // effectively results in testing for new settings every 10 seconds
	MinQPSForOpen:         10,
	CoolDownInterval:      10 * time.Second,
	HalfOpenSuccessCount:  1,
}

// CircuitBreaker is a state machine to prevent sending requests that are likely to fail.
type CircuitBreaker struct {
	config *Settings
	name   string

	mutex sync.Mutex
	state *State

	successCounter  prometheus.Counter
	errorCounter    prometheus.Counter
	overloadCounter prometheus.Counter
	fastFailCounter prometheus.Counter
}

// StateType is a type that represents a state of CircuitBreaker.
type StateType int

// States of CircuitBreaker.
const (
	StateClosed StateType = iota
	StateOpen
	StateHalfOpen
)

// String implements stringer interface.
func (s StateType) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return fmt.Sprintf("unknown state: %d", s)
	}
}

var replacer = strings.NewReplacer(" ", "_", "-", "_")

// NewCircuitBreaker returns a new CircuitBreaker configured with the given Settings.
func NewCircuitBreaker(name string, st Settings) *CircuitBreaker {
	cb := new(CircuitBreaker)
	cb.name = name
	cb.config = &st
	cb.state = cb.newState(time.Now(), StateClosed)

	metricName := replacer.Replace(name)
	cb.successCounter = m.CircuitBreakerCounters.WithLabelValues(metricName, "success")
	cb.errorCounter = m.CircuitBreakerCounters.WithLabelValues(metricName, "error")
	cb.overloadCounter = m.CircuitBreakerCounters.WithLabelValues(metricName, "overload")
	cb.fastFailCounter = m.CircuitBreakerCounters.WithLabelValues(metricName, "fast_fail")
	return cb
}

// ChangeSettings changes the CircuitBreaker settings.
// The changes will be reflected only in the next evaluation window.
func (cb *CircuitBreaker) ChangeSettings(apply func(config *Settings)) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	apply(cb.config)
	log.Info("circuit breaker settings changed", zap.Any("config", cb.config))
}

// Execute calls the given function if the CircuitBreaker is closed and returns the result of execution.
// Execute returns an error instantly if the CircuitBreaker is open.
// https://github.com/tikv/rfcs/blob/master/text/0115-circuit-breaker.md
func (cb *CircuitBreaker) Execute(call func() (Overloading, error)) error {
	state, err := cb.onRequest()
	if err != nil {
		cb.fastFailCounter.Inc()
		return err
	}

	defer func() {
		e := recover()
		if e != nil {
			cb.emitMetric(Yes, err)
			cb.onResult(state, Yes)
			panic(e)
		}
	}()

	overloaded, err := call()
	cb.emitMetric(overloaded, err)
	cb.onResult(state, overloaded)
	return err
}

func (cb *CircuitBreaker) onRequest() (*State, error) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	state, err := cb.state.onRequest(cb)
	cb.state = state
	return state, err
}

func (cb *CircuitBreaker) onResult(state *State, overloaded Overloading) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	// even if the circuit breaker already moved to a new state while the request was in progress,
	// it is still ok to update the old state, but it is not relevant anymore
	state.onResult(overloaded)
}

func (cb *CircuitBreaker) emitMetric(overloaded Overloading, err error) {
	switch overloaded {
	case No:
		cb.successCounter.Inc()
	case Yes:
		cb.overloadCounter.Inc()
	default:
		panic("unknown state")
	}
	if err != nil {
		cb.errorCounter.Inc()
	}
}

// State represents the state of CircuitBreaker.
type State struct {
	stateType StateType
	cb        *CircuitBreaker
	end       time.Time

	pendingCount uint32
	successCount uint32
	failureCount uint32
}

// newState creates a new State with the given configuration and reset all success/failure counters.
func (cb *CircuitBreaker) newState(now time.Time, stateType StateType) *State {
	var end time.Time
	var pendingCount uint32
	switch stateType {
	case StateClosed:
		end = now.Add(cb.config.ErrorRateWindow)
	case StateOpen:
		end = now.Add(cb.config.CoolDownInterval)
	case StateHalfOpen:
		// we transition to HalfOpen state on the first request after the cooldown period,
		// so we start with 1 pending request
		pendingCount = 1
	default:
		panic("unknown state")
	}
	return &State{
		cb:           cb,
		stateType:    stateType,
		pendingCount: pendingCount,
		end:          end,
	}
}

// onRequest transitions the state to the next state based on the current state and the previous requests results
// The implementation represents a state machine for CircuitBreaker
// All state transitions happens at the request evaluation time only
// Circuit breaker start with a closed state, allows all requests to pass through and always lasts for a fixed duration of `Settings.ErrorRateWindow`.
// If `Settings.ErrorRateThresholdPct` is breached at the end of the window, then it moves to Open state, otherwise it moves to a new Closed state with a new window.
// Open state fails all request, it has a fixed duration of `Settings.CoolDownInterval` and always moves to HalfOpen state at the end of the interval.
// HalfOpen state does not have a fixed duration and lasts till `Settings.HalfOpenSuccessCount` are evaluated.
// If any of `Settings.HalfOpenSuccessCount` fails then it moves back to Open state, otherwise it moves to Closed state.
func (s *State) onRequest(cb *CircuitBreaker) (*State, error) {
	var now = time.Now()
	switch s.stateType {
	case StateClosed:
		if now.After(s.end) {
			// ErrorRateWindow is over, let's evaluate the error rate
			if s.cb.config.ErrorRateThresholdPct > 0 { // otherwise circuit breaker is disabled
				total := s.failureCount + s.successCount
				if total > 0 {
					observedErrorRatePct := s.failureCount * 100 / total
					if total >= uint32(s.cb.config.ErrorRateWindow.Seconds())*s.cb.config.MinQPSForOpen && observedErrorRatePct >= s.cb.config.ErrorRateThresholdPct {
						// the error threshold is breached, let's move to open state and start failing all requests
						log.Error("circuit breaker tripped and starting to fail all requests",
							zap.String("name", cb.name),
							zap.Uint32("observed-err-rate-pct", observedErrorRatePct),
							zap.Any("config", cb.config))
						return cb.newState(now, StateOpen), errs.ErrCircuitBreakerOpen
					}
				}
			}
			// the error threshold is not breached or there were not enough requests to evaluate it,
			// continue in the closed state and allow all requests
			return cb.newState(now, StateClosed), nil
		}
		// continue in closed state till ErrorRateWindow is over
		return s, nil
	case StateOpen:
		if s.cb.config.ErrorRateThresholdPct == 0 {
			return cb.newState(now, StateClosed), nil
		}

		if now.After(s.end) {
			// CoolDownInterval is over, it is time to transition to half-open state
			log.Info("circuit breaker cooldown period is over. Transitioning to half-open state to test the service",
				zap.String("name", cb.name),
				zap.Any("config", cb.config))
			return cb.newState(now, StateHalfOpen), nil
		} else {
			// continue in the open state till CoolDownInterval is over
			return s, errs.ErrCircuitBreakerOpen
		}
	case StateHalfOpen:
		if s.cb.config.ErrorRateThresholdPct == 0 {
			return cb.newState(now, StateClosed), nil
		}

		// do we need some expire time here in case of one of pending requests is stuck forever?
		if s.failureCount > 0 {
			// there were some failures during half-open state, let's go back to open state to wait a bit longer
			log.Error("circuit breaker goes from half-open to open again as errors persist and continue to fail all requests",
				zap.String("name", cb.name),
				zap.Any("config", cb.config))
			return cb.newState(now, StateOpen), errs.ErrCircuitBreakerOpen
		} else if s.successCount == s.cb.config.HalfOpenSuccessCount {
			// all probe requests are succeeded, we can move to closed state and allow all requests
			log.Info("circuit breaker is closed and start allowing all requests",
				zap.String("name", cb.name),
				zap.Any("config", cb.config))
			return cb.newState(now, StateClosed), nil
		} else if s.pendingCount < s.cb.config.HalfOpenSuccessCount {
			// allow more probe requests and continue in half-open state
			s.pendingCount++
			return s, nil
		} else {
			// continue in half-open state till all probe requests are done and fail all other requests for now
			return s, errs.ErrCircuitBreakerOpen
		}
	default:
		panic("unknown state")
	}
}

func (s *State) onResult(overloaded Overloading) {
	switch overloaded {
	case No:
		s.successCount++
	case Yes:
		s.failureCount++
	default:
		panic("unknown state")
	}
}

// Define context key type
type cbCtxKey struct{}

// Key used to store circuit breaker
var CircuitBreakerKey = cbCtxKey{}

// FromContext retrieves the circuit breaker from the context
func FromContext(ctx context.Context) *CircuitBreaker {
	if ctx == nil {
		return nil
	}
	if cb, ok := ctx.Value(CircuitBreakerKey).(*CircuitBreaker); ok {
		return cb
	}
	return nil
}

// WithCircuitBreaker stores the circuit breaker into a new context
func WithCircuitBreaker(ctx context.Context, cb *CircuitBreaker) context.Context {
	return context.WithValue(ctx, CircuitBreakerKey, cb)
}
