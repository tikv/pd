// Copyright 2026 TiKV Project Authors.
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

package servicediscovery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"

	pingcaplog "github.com/pingcap/log"

	clienterrs "github.com/tikv/pd/client/errs"
)

func TestMemberRefreshControllerEntersDegradedModeStrictly(t *testing.T) {
	t.Parallel()

	transportFailure := memberUpdateFailure{transport: true}
	nonTransportFailure := memberUpdateFailure{}

	testCases := []struct {
		name   string
		result memberUpdateResult
		states []memberConnectionState
		enter  bool
	}{
		{
			name:   "all current urls have transport failures and inactive connections",
			result: newFailedMemberUpdateResult(transportFailure, transportFailure, transportFailure),
			states: observedMemberConnectionStates(connectivity.Idle, connectivity.Connecting, connectivity.TransientFailure),
			enter:  true,
		},
		{
			name:   "empty url set",
			result: memberUpdateResult{},
			states: nil,
		},
		{
			name:   "not every url was attempted",
			result: newFailedMemberUpdateResult(transportFailure),
			states: observedMemberConnectionStates(connectivity.TransientFailure, connectivity.TransientFailure),
		},
		{
			name:   "non-transport failure",
			result: newFailedMemberUpdateResult(transportFailure, nonTransportFailure),
			states: observedMemberConnectionStates(connectivity.TransientFailure, connectivity.TransientFailure),
		},
		{
			name:   "missing connection",
			result: newFailedMemberUpdateResult(transportFailure),
			states: []memberConnectionState{{}},
		},
		{
			name:   "ready connection",
			result: newFailedMemberUpdateResult(transportFailure),
			states: observedMemberConnectionStates(connectivity.Ready),
		},
		{
			name:   "shutdown connection",
			result: newFailedMemberUpdateResult(transportFailure),
			states: observedMemberConnectionStates(connectivity.Shutdown),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			controller := memberRefreshController{}
			require.Equal(t, testCase.enter, controller.enterDegraded(
				testCase.result,
				memberTestURLs(len(testCase.states)),
				testCase.states,
			))
			require.Equal(t, testCase.enter, controller.isDegraded())
		})
	}
}

func TestMemberRefreshControllerInspectsConnectionStates(t *testing.T) {
	t.Parallel()

	transportFailure := memberUpdateFailure{transport: true}
	testCases := []struct {
		name   string
		states []memberConnectionState
		action memberRefreshAction
	}{
		{
			name:   "idle connections wait without a member refresh",
			states: observedMemberConnectionStates(connectivity.Idle, connectivity.Connecting, connectivity.Idle),
			action: memberRefreshWait,
		},
		{
			name:   "ready connection refreshes immediately",
			states: observedMemberConnectionStates(connectivity.TransientFailure, connectivity.Ready),
			action: memberRefreshRetryBatch,
		},
		{
			name:   "missing connection restores normal behavior",
			states: []memberConnectionState{{observed: true, state: connectivity.TransientFailure}, {}},
			action: memberRefreshRetryBatch,
		},
		{
			name:   "shutdown connection restores normal behavior",
			states: observedMemberConnectionStates(connectivity.TransientFailure, connectivity.Shutdown),
			action: memberRefreshRetryBatch,
		},
		{
			name:   "url replacement restores normal behavior",
			states: observedMemberConnectionStates(connectivity.TransientFailure, connectivity.TransientFailure),
			action: memberRefreshRetryBatch,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			controller := memberRefreshController{}
			initialURLs := memberTestURLs(len(testCase.states))
			failures := make([]memberUpdateFailure, len(testCase.states))
			for i := range failures {
				failures[i] = transportFailure
			}
			require.True(t, controller.enterDegraded(
				newFailedMemberUpdateResult(failures...),
				initialURLs,
				observedMemberConnectionStates(repeatedConnectivityState(connectivity.TransientFailure, len(testCase.states))...),
			))
			currentURLs := initialURLs
			if testCase.name == "url replacement restores normal behavior" {
				currentURLs = []string{"url-0", "replacement-url"}
			}
			decision := controller.inspect(currentURLs, testCase.states)
			require.Equal(t, testCase.action, decision.action)
			require.Equal(t, testCase.action == memberRefreshWait, controller.isDegraded())
		})
	}
}

func TestMemberRefreshControllerInspectDoesNotAllocate(t *testing.T) {
	transportFailure := memberUpdateFailure{transport: true}
	result := newFailedMemberUpdateResult(transportFailure, transportFailure, transportFailure)
	urls := memberTestURLs(3)
	states := observedMemberConnectionStates(connectivity.Idle, connectivity.Connecting, connectivity.TransientFailure)
	controller := memberRefreshController{}
	require.True(t, controller.enterDegraded(result, urls, states))
	allocations := testing.AllocsPerRun(1000, func() {
		memberRefreshDecisionSink = controller.inspect(urls, states)
	})
	require.Zero(t, allocations)
}

var memberRefreshDecisionSink memberRefreshDecision

func TestMemberTransportFailureTrackerEpisodes(t *testing.T) {
	t.Parallel()

	tracker := memberTransportFailureTracker{}
	start := time.Unix(100, 0)

	require.True(t, tracker.record(start, "http://pd-1:2379"))
	require.False(t, tracker.record(start.Add(time.Second), "http://pd-1:2379"))
	require.False(t, tracker.record(start.Add(2*time.Second), "http://pd-1:2379"))
	require.True(t, tracker.record(start.Add(3*time.Second), "http://pd-2:2379"))

	summary, ok := tracker.summary(start.Add(4 * time.Second))
	require.True(t, ok)
	require.Equal(t, []string{"http://pd-1:2379", "http://pd-2:2379"}, summary.failedURLs)
	require.Equal(t, uint64(4), summary.failedAttempts)
	require.Equal(t, uint64(2), summary.suppressedErrors)
	require.Equal(t, 4*time.Second, summary.failureDuration)

	recovery, ok := tracker.recover(start.Add(5*time.Second), "http://pd-2:2379")
	require.True(t, ok)
	require.Equal(t, "http://pd-2:2379", recovery.url)
	require.Equal(t, uint64(1), recovery.failedAttempts)
	require.Zero(t, recovery.suppressedErrors)

	// A success from pd-2 must not recover pd-1.
	summary, ok = tracker.summary(start.Add(6 * time.Second))
	require.True(t, ok)
	require.Equal(t, []string{"http://pd-1:2379"}, summary.failedURLs)

	tracker.cleanup([]string{"http://pd-3:2379"})
	_, ok = tracker.summary(start.Add(7 * time.Second))
	require.False(t, ok)
}

func TestClassifyMemberTransportFailure(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		failure   memberUpdateFailure
		transport bool
	}{
		{
			name:      "connection refused rpc",
			failure:   classifyMemberRPCFailure(status.Error(codes.Unavailable, "dial tcp 192.0.2.1:2379: connect: connection refused")),
			transport: true,
		},
		{
			name:      "tls rpc",
			failure:   classifyMemberRPCFailure(status.Error(codes.Unavailable, "transport: authentication handshake failed: tls certificate expired")),
			transport: true,
		},
		{
			name:      "dns rpc",
			failure:   classifyMemberRPCFailure(status.Error(codes.Unavailable, "lookup pd.invalid: no such host")),
			transport: true,
		},
		{
			name:      "deadline rpc",
			failure:   classifyMemberRPCFailure(status.Error(codes.DeadlineExceeded, "context deadline exceeded")),
			transport: true,
		},
		{
			name:      "reset rpc",
			failure:   classifyMemberRPCFailure(status.Error(codes.Unavailable, "read: connection reset by peer")),
			transport: true,
		},
		{
			name:      "non-network grpc status",
			failure:   classifyMemberRPCFailure(status.Error(codes.PermissionDenied, "permission denied")),
			transport: false,
		},
		{
			name:      "blocking dial timeout",
			failure:   classifyMemberDialFailure(clienterrs.ErrGRPCDial.Wrap(context.DeadlineExceeded).GenWithStackByCause()),
			transport: true,
		},
		{
			name:      "uncertain dial error",
			failure:   classifyMemberDialFailure(errors.New("invalid client configuration")),
			transport: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.transport, testCase.failure.transport)
		})
	}
}

func TestMemberTransportFailureSummaryAndRecoveryLogs(t *testing.T) {
	core, observedLogs := observer.New(zap.InfoLevel)
	restoreLogger := pingcaplog.ReplaceGlobals(zap.New(core), nil)
	t.Cleanup(restoreLogger)

	client := &serviceDiscovery{}
	start := time.Unix(100, 0)
	require.True(t, client.memberTransportFailures.record(start, "http://pd-1:2379"))
	require.False(t, client.memberTransportFailures.record(start.Add(time.Second), "http://pd-1:2379"))

	client.logMemberTransportFailureSummary(start.Add(2 * time.Second))
	summaryLogs := observedLogs.FilterMessage("[pd] member transport failures are being suppressed").All()
	require.Len(t, summaryLogs, 1)
	summaryFields := summaryLogs[0].ContextMap()
	require.Contains(t, summaryFields, "failed-urls")
	require.Equal(t, uint64(2), summaryFields["failed-attempts"])
	require.Equal(t, uint64(1), summaryFields["suppressed-errors"])
	require.NotContains(t, summaryFields, "error-classes")

	client.logMemberTransportFailureRecovery(start.Add(3*time.Second), "http://pd-1:2379")
	recoveryLogs := observedLogs.FilterMessage("[pd] member transport failure recovered").All()
	require.Len(t, recoveryLogs, 1)
	recoveryFields := recoveryLogs[0].ContextMap()
	require.Equal(t, "http://pd-1:2379", recoveryFields["url"])
	require.Equal(t, uint64(2), recoveryFields["failed-attempts"])
	require.Equal(t, uint64(1), recoveryFields["suppressed-errors"])
}

func TestMemberTransportFailureTrackerConcurrentAccess(_ *testing.T) {
	tracker := memberTransportFailureTracker{}
	now := time.Unix(100, 0)

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			url := fmt.Sprintf("http://pd-%d:2379", index%3)
			for range 100 {
				tracker.record(now, url)
				tracker.summary(now.Add(time.Second))
				tracker.recover(now.Add(2*time.Second), url)
				tracker.cleanup([]string{url})
			}
		}(i)
	}
	wg.Wait()
}

func TestMemberTransportFailureTrackerHealthyRecoveryDoesNotAllocate(t *testing.T) {
	tracker := memberTransportFailureTracker{}
	now := time.Unix(100, 0)
	allocations := testing.AllocsPerRun(1000, func() {
		_, _ = tracker.recover(now, "http://pd-1:2379")
	})
	require.Zero(t, allocations)
}

func BenchmarkMemberRefreshControllerInspect(b *testing.B) {
	transportFailure := memberUpdateFailure{transport: true}
	result := newFailedMemberUpdateResult(transportFailure, transportFailure, transportFailure)
	urls := memberTestURLs(3)
	states := observedMemberConnectionStates(connectivity.TransientFailure, connectivity.Connecting, connectivity.Idle)
	controller := memberRefreshController{}
	controller.enterDegraded(result, urls, states)
	b.ReportAllocs()
	for b.Loop() {
		controller.inspect(urls, states)
	}
}

func BenchmarkMemberTransportFailureTrackerSuppression(b *testing.B) {
	tracker := memberTransportFailureTracker{}
	now := time.Unix(100, 0)
	tracker.record(now, "http://pd-1:2379")
	b.ReportAllocs()
	for b.Loop() {
		tracker.record(now, "http://pd-1:2379")
	}
}

func newFailedMemberUpdateResult(failures ...memberUpdateFailure) memberUpdateResult {
	result := memberUpdateResult{}
	for i, failure := range failures {
		result.recordFailure(fmt.Sprintf("url-%d", i), failure)
	}
	return result
}

func memberTestURLs(count int) []string {
	urls := make([]string, 0, count)
	for i := range count {
		urls = append(urls, fmt.Sprintf("url-%d", i))
	}
	return urls
}

func repeatedConnectivityState(state connectivity.State, count int) []connectivity.State {
	states := make([]connectivity.State, count)
	for i := range states {
		states[i] = state
	}
	return states
}

func observedMemberConnectionStates(states ...connectivity.State) []memberConnectionState {
	observed := make([]memberConnectionState, 0, len(states))
	for _, state := range states {
		observed = append(observed, memberConnectionState{observed: true, state: state})
	}
	return observed
}
