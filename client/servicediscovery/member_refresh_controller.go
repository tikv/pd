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
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"

	clienterrs "github.com/tikv/pd/client/errs"
)

type memberUpdateFailure struct {
	transport bool
}

func classifyMemberDialFailure(err error) memberUpdateFailure {
	return memberUpdateFailure{
		transport: errors.Is(err, clienterrs.ErrGRPCDial),
	}
}

func classifyMemberRPCFailure(err error) memberUpdateFailure {
	code := status.Code(err)
	if errors.Is(err, context.DeadlineExceeded) {
		code = codes.DeadlineExceeded
	}
	return memberUpdateFailure{
		transport: clienterrs.IsNetworkError(code),
	}
}

type memberUpdateResult struct {
	attemptedURLs     []string
	transportFailures int
}

func (r *memberUpdateResult) recordFailure(url string, failure memberUpdateFailure) {
	r.attemptedURLs = append(r.attemptedURLs, url)
	if failure.transport {
		r.transportFailures++
	}
}

func (r *memberUpdateResult) allFailedByTransport(urls []string) bool {
	if len(urls) == 0 || len(r.attemptedURLs) != len(urls) || r.transportFailures != len(urls) {
		return false
	}
	return equalMemberURLs(r.attemptedURLs, urls)
}

type memberConnectionState struct {
	observed bool
	state    connectivity.State
}

type memberRefreshAction uint8

const (
	memberRefreshWait memberRefreshAction = iota
	memberRefreshRetryBatch
)

type memberRefreshDecision struct {
	action memberRefreshAction
}

type memberRefreshController struct {
	degraded     bool
	degradedURLs []string
}

func (c *memberRefreshController) isDegraded() bool {
	return c.degraded
}

func (c *memberRefreshController) enterDegraded(
	result memberUpdateResult,
	urls []string,
	states []memberConnectionState,
) bool {
	if len(urls) != len(states) || !result.allFailedByTransport(urls) {
		return false
	}
	for _, state := range states {
		if !state.observed || !isInactiveMemberConnectionState(state.state) {
			return false
		}
	}
	c.degraded = true
	c.degradedURLs = append(c.degradedURLs[:0], urls...)
	return true
}

func (c *memberRefreshController) inspect(urls []string, states []memberConnectionState) memberRefreshDecision {
	if !c.degraded {
		return memberRefreshDecision{action: memberRefreshRetryBatch}
	}
	if len(urls) != len(states) || !equalMemberURLs(c.degradedURLs, urls) {
		c.leaveDegraded()
		return memberRefreshDecision{action: memberRefreshRetryBatch}
	}
	for _, state := range states {
		if !state.observed || !isInactiveMemberConnectionState(state.state) {
			c.leaveDegraded()
			return memberRefreshDecision{action: memberRefreshRetryBatch}
		}
	}
	return memberRefreshDecision{action: memberRefreshWait}
}

func (c *memberRefreshController) leaveDegraded() {
	c.degraded = false
	c.degradedURLs = nil
}

func equalMemberURLs(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func isInactiveMemberConnectionState(state connectivity.State) bool {
	return state == connectivity.Idle ||
		state == connectivity.Connecting ||
		state == connectivity.TransientFailure
}

type memberTransportFailureEpisode struct {
	firstFailure     time.Time
	failedAttempts   uint64
	suppressedErrors uint64
}

type memberTransportFailureRecovery struct {
	url              string
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
	episode.suppressedErrors++
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
		url:              url,
		failureDuration:  now.Sub(episode.firstFailure),
		failedAttempts:   episode.failedAttempts,
		suppressedErrors: episode.suppressedErrors,
	}, true
}

func (t *memberTransportFailureTracker) discard(url string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.episodes, url)
}

func (t *memberTransportFailureTracker) cleanup(urls []string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.episodes) == 0 {
		return
	}
	current := make(map[string]struct{}, len(urls))
	for _, url := range urls {
		current[url] = struct{}{}
	}
	for url := range t.episodes {
		if _, ok := current[url]; !ok {
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
		summary.suppressedErrors += episode.suppressedErrors
	}
	summary.failureDuration = now.Sub(earliest)
	return summary, true
}
