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

func isMemberDialTransportFailure(err error) bool {
	return errors.Is(err, clienterrs.ErrGRPCDial)
}

func isMemberRPCTransportFailure(err error) bool {
	code := status.Code(err)
	if errors.Is(err, context.DeadlineExceeded) {
		code = codes.DeadlineExceeded
	}
	return clienterrs.IsNetworkError(code)
}

type memberUpdateResult struct {
	failedURLs        []string
	transportFailures int
}

func (r *memberUpdateResult) recordFailure(url string, transportFailure bool) {
	r.failedURLs = append(r.failedURLs, url)
	if transportFailure {
		r.transportFailures++
	}
}

func (r *memberUpdateResult) allFailedByTransport(urls []string) bool {
	if len(urls) == 0 || len(r.failedURLs) != len(urls) || r.transportFailures != len(urls) {
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

func (c *memberRefreshController) enterDegraded(
	result memberUpdateResult,
	urls []string,
	connections []memberConnection,
) bool {
	if len(urls) != len(connections) || !result.allFailedByTransport(urls) {
		return false
	}
	for _, connection := range connections {
		if !connection.observed || !isInactiveMemberConnectionState(connection.state) {
			return false
		}
	}
	c.degradedURLs = append(c.degradedURLs[:0], urls...)
	return true
}

func (c *memberRefreshController) shouldWait(urls []string, connections []memberConnection) bool {
	if !c.isDegraded() || len(urls) != len(connections) || !slices.Equal(c.degradedURLs, urls) {
		c.leaveDegraded()
		return false
	}
	for _, connection := range connections {
		if !connection.observed || !isInactiveMemberConnectionState(connection.state) {
			c.leaveDegraded()
			return false
		}
	}
	return true
}

func (c *memberRefreshController) leaveDegraded() {
	c.degradedURLs = nil
}

func isInactiveMemberConnectionState(state connectivity.State) bool {
	return state == connectivity.Idle ||
		state == connectivity.Connecting ||
		state == connectivity.TransientFailure
}

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
	failedURLs       []string
	failureDuration  time.Duration
	failedAttempts   uint64
	suppressedErrors uint64
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

func (t *memberTransportFailureTracker) retain(currentURLs, failedURLs []string) {
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
	summary.failureDuration = now.Sub(earliest)
	return summary, true
}
