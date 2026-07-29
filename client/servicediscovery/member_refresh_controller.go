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
	"slices"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"

	clienterrs "github.com/tikv/pd/client/errs"
)

// isMemberDialTransportFailure recognizes the explicit gRPC dial sentinel.
func isMemberDialTransportFailure(err error) bool {
	return errors.Is(err, clienterrs.ErrGRPCDial)
}

// isMemberRPCTransportFailure recognizes Unavailable and DeadlineExceeded,
// including a local context deadline. These classifications only make an
// error eligible for transport-outage suppression; entering degraded mode also
// requires every corresponding gRPC connection to be in a degraded-mode state.
func isMemberRPCTransportFailure(err error) bool {
	code := status.Code(err)
	if errors.Is(err, context.DeadlineExceeded) {
		code = codes.DeadlineExceeded
	}
	return clienterrs.IsNetworkError(code)
}

type memberUpdateResult struct {
	// failedURLs is the ordered prefix attempted before the first success.
	failedURLs            []string
	transportFailureCount int
}

func (r *memberUpdateResult) recordFailure(url string, isTransportFailure bool) {
	r.failedURLs = append(r.failedURLs, url)
	if isTransportFailure {
		r.transportFailureCount++
	}
}

func (r *memberUpdateResult) allCurrentURLsFailedByTransport(urls []string) bool {
	if len(urls) == 0 || len(r.failedURLs) != len(urls) || r.transportFailureCount != len(urls) {
		return false
	}
	return slices.Equal(r.failedURLs, urls)
}

type memberConnection struct {
	observed bool
	state    connectivity.State
	conn     *grpc.ClientConn
}

type memberRefreshController struct {
	degradedURLs []string
}

func (c *memberRefreshController) isDegraded() bool {
	return len(c.degradedURLs) > 0
}

func (c *memberRefreshController) tryEnterDegraded(
	result memberUpdateResult,
	urls []string,
	connections []memberConnection,
) bool {
	if len(urls) != len(connections) || !result.allCurrentURLsFailedByTransport(urls) {
		return false
	}
	for _, connection := range connections {
		if !connection.observed || !isDegradedModeConnectionState(connection.state) {
			return false
		}
	}
	c.degradedURLs = append(c.degradedURLs[:0], urls...)
	return true
}

func (c *memberRefreshController) canRemainDegraded(urls []string, connections []memberConnection) bool {
	if !c.isDegraded() || len(urls) != len(connections) || !slices.Equal(c.degradedURLs, urls) {
		return false
	}
	for _, connection := range connections {
		if !connection.observed || !isDegradedModeConnectionState(connection.state) {
			return false
		}
	}
	return true
}

func (c *memberRefreshController) leaveDegraded() {
	c.degradedURLs = nil
}

// isDegradedModeConnectionState accepts only non-ready states that can recover
// on the existing connection. Missing and shut-down connections require an
// immediate member refresh instead.
func isDegradedModeConnectionState(state connectivity.State) bool {
	return state == connectivity.Idle ||
		state == connectivity.Connecting ||
		state == connectivity.TransientFailure
}

// A transport-failure episode starts with the first classified transport
// failure for a URL. It ends when that URL succeeds, returns a non-transport
// failure, leaves the current member set, or is not reached because an earlier
// URL completed the refresh. Only a direct success emits a recovery log.
type memberTransportFailureEpisode struct {
	firstFailure   time.Time
	failedAttempts uint64
}

type memberTransportFailureRecovery struct {
	failureDuration  time.Duration
	failedAttempts   uint64
	suppressedErrors uint64
}

type memberTransportFailureSummary struct {
	failedURLs             []string
	longestFailureDuration time.Duration
	failedAttempts         uint64
	suppressedErrors       uint64
}

type memberTransportFailureTracker struct {
	mu       sync.Mutex
	episodes map[string]*memberTransportFailureEpisode
}

// record returns true when the caller should emit the detailed failure log.
func (t *memberTransportFailureTracker) record(now time.Time, url string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.episodes == nil {
		t.episodes = make(map[string]*memberTransportFailureEpisode)
	}
	episode, ok := t.episodes[url]
	if !ok {
		t.episodes[url] = &memberTransportFailureEpisode{
			firstFailure:   now,
			failedAttempts: 1,
		}
		return true
	}

	episode.failedAttempts++
	return false
}

func (t *memberTransportFailureTracker) recover(now time.Time, url string) (memberTransportFailureRecovery, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	episode, ok := t.episodes[url]
	if !ok {
		return memberTransportFailureRecovery{}, false
	}
	delete(t.episodes, url)
	return memberTransportFailureRecovery{
		failureDuration:  now.Sub(episode.firstFailure),
		failedAttempts:   episode.failedAttempts,
		suppressedErrors: episode.failedAttempts - 1,
	}, true
}

func (t *memberTransportFailureTracker) discard(url string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.episodes, url)
}

// retainCurrentFailures drops stale episodes after a successful refresh.
func (t *memberTransportFailureTracker) retainCurrentFailures(currentURLs, failedURLs []string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for url := range t.episodes {
		if !slices.Contains(currentURLs, url) || !slices.Contains(failedURLs, url) {
			delete(t.episodes, url)
		}
	}
}

func (t *memberTransportFailureTracker) summary(now time.Time) (memberTransportFailureSummary, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.episodes) == 0 {
		return memberTransportFailureSummary{}, false
	}
	urls := make([]string, 0, len(t.episodes))
	for url := range t.episodes {
		urls = append(urls, url)
	}
	sort.Strings(urls)

	summary := memberTransportFailureSummary{
		failedURLs: urls,
	}
	earliest := now
	for _, url := range urls {
		episode := t.episodes[url]
		if episode.firstFailure.Before(earliest) {
			earliest = episode.firstFailure
		}
		summary.failedAttempts += episode.failedAttempts
		summary.suppressedErrors += episode.failedAttempts - 1
	}
	summary.longestFailureDuration = now.Sub(earliest)
	return summary, true
}
