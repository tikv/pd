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

	availabilityFailure := true
	otherFailure := false

	testCases := []struct {
		name        string
		result      memberUpdateResult
		connections []memberConnection
		currentURLs []string
		enter       bool
	}{
		{
			name:        "all current urls have availability failures and degraded-mode connections",
			result:      newFailedMemberUpdateResult(availabilityFailure, availabilityFailure, availabilityFailure),
			connections: observedMemberConnections(connectivity.Idle, connectivity.Connecting, connectivity.TransientFailure),
			enter:       true,
		},
		{
			name:        "empty url set",
			result:      memberUpdateResult{},
			connections: nil,
		},
		{
			name:        "not every url was attempted",
			result:      newFailedMemberUpdateResult(availabilityFailure),
			connections: observedMemberConnections(connectivity.TransientFailure, connectivity.TransientFailure),
		},
		{
			name:        "url set changed while failures were collected",
			result:      newFailedMemberUpdateResult(availabilityFailure, availabilityFailure),
			connections: observedMemberConnections(connectivity.TransientFailure, connectivity.TransientFailure),
			currentURLs: []string{"url-0", "replacement-url"},
		},
		{
			name:        "other failure",
			result:      newFailedMemberUpdateResult(availabilityFailure, otherFailure),
			connections: observedMemberConnections(connectivity.TransientFailure, connectivity.TransientFailure),
		},
		{
			name:        "missing connection",
			result:      newFailedMemberUpdateResult(availabilityFailure),
			connections: []memberConnection{{}},
		},
		{
			name:        "ready connection",
			result:      newFailedMemberUpdateResult(availabilityFailure),
			connections: observedMemberConnections(connectivity.Ready),
		},
		{
			name:        "shutdown connection",
			result:      newFailedMemberUpdateResult(availabilityFailure),
			connections: observedMemberConnections(connectivity.Shutdown),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			controller := memberRefreshController{}
			currentURLs := testCase.currentURLs
			if currentURLs == nil {
				currentURLs = memberTestURLs(len(testCase.connections))
			}
			require.Equal(t, testCase.enter, controller.tryEnterDegraded(
				testCase.result,
				currentURLs,
				testCase.connections,
			))
			require.Equal(t, testCase.enter, controller.isDegraded())
		})
	}
}

func TestMemberRefreshControllerCanRemainDegraded(t *testing.T) {
	t.Parallel()

	availabilityFailure := true
	testCases := []struct {
		name        string
		connections []memberConnection
		currentURLs []string
		remain      bool
	}{
		{
			name:        "idle connections wait without a member refresh",
			connections: observedMemberConnections(connectivity.Idle, connectivity.Connecting, connectivity.Idle),
			remain:      true,
		},
		{
			name:        "ready connection refreshes immediately",
			connections: observedMemberConnections(connectivity.TransientFailure, connectivity.Ready),
		},
		{
			name:        "missing connection restores normal behavior",
			connections: []memberConnection{{observed: true, state: connectivity.TransientFailure}, {}},
		},
		{
			name:        "shutdown connection restores normal behavior",
			connections: observedMemberConnections(connectivity.TransientFailure, connectivity.Shutdown),
		},
		{
			name:        "url replacement restores normal behavior",
			connections: observedMemberConnections(connectivity.TransientFailure, connectivity.TransientFailure),
			currentURLs: []string{"url-0", "replacement-url"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			controller := memberRefreshController{}
			initialURLs := memberTestURLs(len(testCase.connections))
			failures := make([]bool, len(testCase.connections))
			for i := range failures {
				failures[i] = availabilityFailure
			}
			require.True(t, controller.tryEnterDegraded(
				newFailedMemberUpdateResult(failures...),
				initialURLs,
				observedMemberConnections(repeatedConnectivityState(connectivity.TransientFailure, len(testCase.connections))...),
			))
			currentURLs := testCase.currentURLs
			if currentURLs == nil {
				currentURLs = initialURLs
			}
			remain := controller.canRemainDegraded(currentURLs, testCase.connections)
			require.Equal(t, testCase.remain, remain)
			require.True(t, controller.isDegraded())
		})
	}
}

func TestMemberRefreshControllerCanRemainDegradedDoesNotAllocate(t *testing.T) {
	availabilityFailure := true
	result := newFailedMemberUpdateResult(availabilityFailure, availabilityFailure, availabilityFailure)
	urls := memberTestURLs(3)
	connections := observedMemberConnections(connectivity.Idle, connectivity.Connecting, connectivity.TransientFailure)
	controller := memberRefreshController{}
	require.True(t, controller.tryEnterDegraded(result, urls, connections))
	allocations := testing.AllocsPerRun(1000, func() {
		memberRefreshRemainDegradedSink = controller.canRemainDegraded(urls, connections)
	})
	require.Zero(t, allocations)
}

var memberRefreshRemainDegradedSink bool

func TestMemberAvailabilityFailureTrackerEpisodes(t *testing.T) {
	t.Parallel()

	tracker := memberAvailabilityFailureTracker{}
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
	require.Equal(t, 4*time.Second, summary.longestFailureDuration)

	recovery, ok := tracker.recover(start.Add(5*time.Second), "http://pd-2:2379")
	require.True(t, ok)
	require.Equal(t, uint64(1), recovery.failedAttempts)
	require.Zero(t, recovery.suppressedErrors)

	// A success from pd-2 must not recover pd-1.
	summary, ok = tracker.summary(start.Add(6 * time.Second))
	require.True(t, ok)
	require.Equal(t, []string{"http://pd-1:2379"}, summary.failedURLs)

	tracker.retainCurrentFailures([]string{"http://pd-3:2379"}, []string{"http://pd-1:2379"})
	_, ok = tracker.summary(start.Add(7 * time.Second))
	require.False(t, ok)
}

func TestIsMemberAvailabilityFailure(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		got          bool
		availability bool
	}{
		{
			name:         "connection refused rpc",
			got:          isMemberRPCAvailabilityFailure(status.Error(codes.Unavailable, "dial tcp 192.0.2.1:2379: connect: connection refused")),
			availability: true,
		},
		{
			name:         "tls rpc",
			got:          isMemberRPCAvailabilityFailure(status.Error(codes.Unavailable, "transport: authentication handshake failed: tls certificate expired")),
			availability: true,
		},
		{
			name:         "dns rpc",
			got:          isMemberRPCAvailabilityFailure(status.Error(codes.Unavailable, "lookup pd.invalid: no such host")),
			availability: true,
		},
		{
			name:         "deadline rpc",
			got:          isMemberRPCAvailabilityFailure(status.Error(codes.DeadlineExceeded, "context deadline exceeded")),
			availability: true,
		},
		{
			name:         "local deadline",
			got:          isMemberRPCAvailabilityFailure(context.DeadlineExceeded),
			availability: true,
		},
		{
			name: "local cancellation",
			got:  isMemberRPCAvailabilityFailure(context.Canceled),
		},
		{
			name:         "reset rpc",
			got:          isMemberRPCAvailabilityFailure(status.Error(codes.Unavailable, "read: connection reset by peer")),
			availability: true,
		},
		{
			name:         "non-network grpc status",
			got:          isMemberRPCAvailabilityFailure(status.Error(codes.PermissionDenied, "permission denied")),
			availability: false,
		},
		{
			name:         "blocking dial timeout",
			got:          isMemberDialAvailabilityFailure(clienterrs.ErrGRPCDial.Wrap(context.DeadlineExceeded).GenWithStackByCause()),
			availability: true,
		},
		{
			name:         "uncertain dial error",
			got:          isMemberDialAvailabilityFailure(errors.New("invalid client configuration")),
			availability: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.availability, testCase.got)
		})
	}
}

func TestMemberAvailabilityFailureSummaryAndRecoveryLogs(t *testing.T) {
	core, observedLogs := observer.New(zap.InfoLevel)
	restoreLogger := pingcaplog.ReplaceGlobals(zap.New(core), nil)
	t.Cleanup(restoreLogger)

	client := &serviceDiscovery{}
	start := time.Unix(100, 0)
	require.True(t, client.memberAvailabilityFailures.record(start, "http://pd-1:2379"))
	require.False(t, client.memberAvailabilityFailures.record(start.Add(time.Second), "http://pd-1:2379"))

	client.logMemberAvailabilityFailureSummary(start.Add(2 * time.Second))
	summaryLogs := observedLogs.FilterMessage("[pd] member availability failures are being suppressed").All()
	require.Len(t, summaryLogs, 1)
	summaryFields := summaryLogs[0].ContextMap()
	require.Contains(t, summaryFields, "failed-urls")
	require.Contains(t, summaryFields, "longest-failure-duration")
	require.Equal(t, uint64(2), summaryFields["failed-attempts"])
	require.Equal(t, uint64(1), summaryFields["suppressed-errors"])
	require.NotContains(t, summaryFields, "error-classes")

	client.logMemberAvailabilityFailureRecovery(start.Add(3*time.Second), "http://pd-1:2379")
	recoveryLogs := observedLogs.FilterMessage("[pd] member availability failure recovered").All()
	require.Len(t, recoveryLogs, 1)
	recoveryFields := recoveryLogs[0].ContextMap()
	require.Equal(t, "http://pd-1:2379", recoveryFields["url"])
	require.Equal(t, uint64(2), recoveryFields["failed-attempts"])
	require.Equal(t, uint64(1), recoveryFields["suppressed-errors"])
}

func TestMemberAvailabilityFailureTrackerConcurrentAccess(_ *testing.T) {
	tracker := memberAvailabilityFailureTracker{}
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
				tracker.retainCurrentFailures([]string{url}, []string{url})
			}
		}(i)
	}
	wg.Wait()
}

func TestMemberAvailabilityFailureTrackerHealthyRecoveryDoesNotAllocate(t *testing.T) {
	tracker := memberAvailabilityFailureTracker{}
	now := time.Unix(100, 0)
	allocations := testing.AllocsPerRun(1000, func() {
		_, _ = tracker.recover(now, "http://pd-1:2379")
	})
	require.Zero(t, allocations)
}

func BenchmarkMemberRefreshControllerCanRemainDegraded(b *testing.B) {
	availabilityFailure := true
	result := newFailedMemberUpdateResult(availabilityFailure, availabilityFailure, availabilityFailure)
	urls := memberTestURLs(3)
	connections := observedMemberConnections(connectivity.TransientFailure, connectivity.Connecting, connectivity.Idle)
	controller := memberRefreshController{}
	controller.tryEnterDegraded(result, urls, connections)
	b.ReportAllocs()
	for b.Loop() {
		controller.canRemainDegraded(urls, connections)
	}
}

func BenchmarkMemberAvailabilityFailureTrackerSuppression(b *testing.B) {
	tracker := memberAvailabilityFailureTracker{}
	now := time.Unix(100, 0)
	tracker.record(now, "http://pd-1:2379")
	b.ReportAllocs()
	for b.Loop() {
		tracker.record(now, "http://pd-1:2379")
	}
}

func newFailedMemberUpdateResult(failures ...bool) memberUpdateResult {
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

func observedMemberConnections(states ...connectivity.State) []memberConnection {
	observed := make([]memberConnection, 0, len(states))
	for _, state := range states {
		observed = append(observed, memberConnection{observed: true, state: state})
	}
	return observed
}
