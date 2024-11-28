package circuitbreaker

import (
	"errors"
	"testing"
	"time"

	"github.com/tikv/pd/client/errs"

	"github.com/stretchr/testify/require"
)

// advance emulate the state machine clock moves forward by the given duration
func (cb *CircuitBreaker[T]) advance(duration time.Duration) {
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

func TestCircuitBreaker_Execute_Wrapper_Return_Values(t *testing.T) {
	re := require.New(t)
	cb := NewCircuitBreaker[int]("test_cb", settings)
	originalError := errors.New("circuit breaker is open")

	result, err := cb.Execute(func() (int, Overloading, error) {
		return 42, No, originalError
	})
	re.Equal(err, originalError)
	re.Equal(42, result)

	// same by interpret the result as overloading error
	result, err = cb.Execute(func() (int, Overloading, error) {
		return 42, Yes, originalError
	})
	re.Equal(err, originalError)
	re.Equal(42, result)
}

func TestCircuitBreaker_OpenState(t *testing.T) {
	re := require.New(t)
	cb := NewCircuitBreaker[int]("test_cb", settings)
	driveQPS(cb, minCountToOpen, Yes, re)
	re.Equal(StateClosed, cb.state.stateType)
	assertSucceeds(cb, re) // no error till ErrorRateWindow is finished
	cb.advance(settings.ErrorRateWindow)
	assertFastFail(cb, re)
	re.Equal(StateOpen, cb.state.stateType)
}

func TestCircuitBreaker_OpenState_Not_Enough_QPS(t *testing.T) {
	re := require.New(t)
	cb := NewCircuitBreaker[int]("test_cb", settings)
	re.Equal(StateClosed, cb.state.stateType)
	driveQPS(cb, minCountToOpen/2, Yes, re)
	cb.advance(settings.ErrorRateWindow)
	assertSucceeds(cb, re)
	re.Equal(StateClosed, cb.state.stateType)
}

func TestCircuitBreaker_OpenState_Not_Enough_Error_Rate(t *testing.T) {
	re := require.New(t)
	cb := NewCircuitBreaker[int]("test_cb", settings)
	re.Equal(StateClosed, cb.state.stateType)
	driveQPS(cb, minCountToOpen/4, Yes, re)
	driveQPS(cb, minCountToOpen, No, re)
	cb.advance(settings.ErrorRateWindow)
	assertSucceeds(cb, re)
	re.Equal(StateClosed, cb.state.stateType)
}

func TestCircuitBreaker_Half_Open_To_Closed(t *testing.T) {
	re := require.New(t)
	cb := NewCircuitBreaker[int]("test_cb", settings)
	re.Equal(StateClosed, cb.state.stateType)
	driveQPS(cb, minCountToOpen, Yes, re)
	cb.advance(settings.ErrorRateWindow)
	assertFastFail(cb, re)
	cb.advance(settings.CoolDownInterval)
	assertSucceeds(cb, re)
	assertSucceeds(cb, re)
	re.Equal(StateHalfOpen, cb.state.stateType)
	// state always transferred on the incoming request
	assertSucceeds(cb, re)
	re.Equal(StateClosed, cb.state.stateType)
}

func TestCircuitBreaker_Half_Open_To_Open(t *testing.T) {
	re := require.New(t)
	cb := NewCircuitBreaker[int]("test_cb", settings)
	re.Equal(StateClosed, cb.state.stateType)
	driveQPS(cb, minCountToOpen, Yes, re)
	cb.advance(settings.ErrorRateWindow)
	assertFastFail(cb, re)
	cb.advance(settings.CoolDownInterval)
	assertSucceeds(cb, re)
	re.Equal(StateHalfOpen, cb.state.stateType)
	_, err := cb.Execute(func() (int, Overloading, error) {
		return 42, Yes, nil // this trip circuit breaker again
	})
	re.NoError(err)
	re.Equal(StateHalfOpen, cb.state.stateType)
	// state always transferred on the incoming request
	assertFastFail(cb, re)
	re.Equal(StateOpen, cb.state.stateType)
}

func TestCircuitBreaker_Half_Open_Fail_Over_Pending_Count(t *testing.T) {
	re := require.New(t)
	cb := NewCircuitBreaker[int]("test_cb", settings)
	re.Equal(StateClosed, cb.state.stateType)
	driveQPS(cb, minCountToOpen, Yes, re)
	cb.advance(settings.ErrorRateWindow)
	assertFastFail(cb, re)
	re.Equal(StateOpen, cb.state.stateType)
	cb.advance(settings.CoolDownInterval)

	var started []chan bool
	var waited []chan bool
	var ended []chan bool
	for range int(settings.HalfOpenSuccessCount) {
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
			_, err := cb.Execute(func() (int, Overloading, error) {
				start <- true
				<-wait
				return 42, No, nil
			})
			re.NoError(err)
		}()
	}
	for i := range started {
		<-started[i]
	}
	assertFastFail(cb, re)
	re.Equal(StateHalfOpen, cb.state.stateType)
	for i := range ended {
		waited[i] <- true
		<-ended[i]
	}
	assertSucceeds(cb, re)
	re.Equal(StateClosed, cb.state.stateType)
}

func TestCircuitBreaker_ChangeSettings(t *testing.T) {
	re := require.New(t)

	cb := NewCircuitBreaker[int]("test_cb", AlwaysOpenSettings)
	driveQPS(cb, int(AlwaysOpenSettings.MinQPSForOpen*uint32(AlwaysOpenSettings.ErrorRateWindow.Seconds())), Yes, re)
	cb.advance(AlwaysOpenSettings.ErrorRateWindow)
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

func driveQPS(cb *CircuitBreaker[int], count int, overload Overloading, re *require.Assertions) {
	for range count {
		_, err := cb.Execute(func() (int, Overloading, error) {
			return 42, overload, nil
		})
		re.NoError(err)
	}
}

func assertFastFail(cb *CircuitBreaker[int], re *require.Assertions) {
	var executed = false
	_, err := cb.Execute(func() (int, Overloading, error) {
		executed = true
		return 42, No, nil
	})
	re.Equal(err, errs.ErrCircuitBreakerOpen)
	re.False(executed)
}

func assertSucceeds(cb *CircuitBreaker[int], re *require.Assertions) {
	result, err := cb.Execute(func() (int, Overloading, error) {
		return 42, No, nil
	})
	re.NoError(err)
	re.Equal(42, result)
}
