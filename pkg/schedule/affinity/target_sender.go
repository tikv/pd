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

package affinity

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	// maxSenderGroups bounds the groups a targetSender tracks, and so the targets it queues.
	maxSenderGroups = 1024
	// minTargetSendBackoff is how long a group waits to send another target after
	// a write, which leaves time for the watcher to sync a written target. It is
	// also the backoff after the first failed write.
	minTargetSendBackoff = time.Second
	// maxTargetSendBackoff caps the backoff, which doubles with each failed write in a row.
	maxTargetSendBackoff = 10 * time.Second
)

// targetSender sends the first-win targets of a Manager to its TargetWriter in
// the background, so that a slow or unreachable PD does not block the checker.
// It sends one target at a time, and a group has at most one target queued or
// in flight. After a write the group waits before it sends again, and backs off
// exponentially after failed writes, so that the Regions of a group do not
// repeat the request while PD is unavailable. A target that is not queued is
// dropped; a later observation of the group decides the target again.
type targetSender struct {
	writer TargetWriter
	// queue holds the targets to send. Each of them belongs to a group that is
	// sending, and there are at most maxSenderGroups groups, so it never blocks.
	queue chan GroupTarget

	mu syncutil.Mutex
	// groups holds the groups that are sending, or that sent a target recently.
	groups map[string]*groupSendState
}

type groupSendState struct {
	// sending is set while a target of the group is queued or in flight.
	sending bool
	// retryAt is when the group may send the next target.
	retryAt time.Time
	// backoff is the wait after the last write if it failed, or zero if it succeeded.
	backoff time.Duration
}

func newTargetSender(writer TargetWriter) *targetSender {
	return &targetSender{
		writer: writer,
		queue:  make(chan GroupTarget, maxSenderGroups),
		groups: make(map[string]*groupSendState),
	}
}

// send queues a target and reports whether it is queued. It does not queue the
// target if its group is sending or waiting after a write, or if too many
// groups are.
func (s *targetSender) send(target GroupTarget) bool {
	id := target.Expected.ID
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.groups[id]
	if ok && (state.sending || now.Before(state.retryAt)) {
		return false
	}
	if !ok {
		if len(s.groups) >= maxSenderGroups && !s.forgetIdleLocked(now) {
			return false
		}
		state = &groupSendState{}
		s.groups[id] = state
	}
	state.sending = true
	s.queue <- target
	return true
}

// forgetIdleLocked removes the groups that may send again and reports whether it removed any.
func (s *targetSender) forgetIdleLocked(now time.Time) bool {
	count := len(s.groups)
	for id, state := range s.groups {
		if !state.sending && !now.Before(state.retryAt) {
			delete(s.groups, id)
		}
	}
	return len(s.groups) < count
}

// run sends the queued targets until ctx is done.
func (s *targetSender) run(ctx context.Context) {
	defer logutil.LogPanic()
	for {
		select {
		case <-ctx.Done():
			return
		case target := <-s.queue:
			_, err := s.writer.WriteGroupTargets(ctx, []GroupTarget{target})
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				targetWriteFailureCounter.Inc()
				log.Warn("failed to write affinity group target", zap.String("group-id", target.Expected.ID), errs.ZapError(err))
			}
			s.finish(target.Expected.ID, err)
		}
	}
}

// finish records the result of a write of a target of the group, and when the group may send again.
func (s *targetSender) finish(id string, err error) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.groups[id]
	if !ok {
		return
	}
	state.sending = false
	if err != nil {
		state.backoff = min(max(2*state.backoff, minTargetSendBackoff), maxTargetSendBackoff)
	} else {
		state.backoff = 0
	}
	state.retryAt = now.Add(max(state.backoff, minTargetSendBackoff))
}
