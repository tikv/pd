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

package controller

import (
	"context"
	"errors"
	"time"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/client/errs"
)

type queuedReservationState uint8

const (
	queuedReservationRejected queuedReservationState = iota
	queuedReservationWaiting
	queuedReservationReady
	queuedReservationCanceled
)

// queuedReservation is the internal, dynamically scheduled counterpart of the
// public fixed Reservation. It is intentionally unexported and pointer-only:
// the limiter owns its FIFO links and all state transitions happen under
// lim.mu.
type queuedReservation struct {
	lim              *Limiter
	prev             *queuedReservation
	next             *queuedReservation
	state            queuedReservationState
	tokens           float64
	timeToAct        time.Time
	needWaitDuration time.Duration
	fillRate         fillRate
	remainingTokens  float64
	err              error
}

var errQueuedReservationCanceled = errors.New("queued reservation canceled")

func (lim *Limiter) effectiveNowLocked(now time.Time) time.Time {
	if now.Before(lim.last) {
		return lim.last
	}
	return now
}

func (lim *Limiter) queuedUpdateChannelLocked() <-chan struct{} {
	if lim.queuedUpdateCh == nil {
		lim.queuedUpdateCh = make(chan struct{})
	}
	return lim.queuedUpdateCh
}

func (lim *Limiter) signalQueuedUpdateLocked() {
	if lim.queuedUpdateCh != nil {
		close(lim.queuedUpdateCh)
	}
	lim.queuedUpdateCh = make(chan struct{})
}

func (lim *Limiter) appendQueuedReservationLocked(reservation *queuedReservation) {
	reservation.prev = lim.queuedTail
	if lim.queuedTail == nil {
		lim.queuedHead = reservation
	} else {
		lim.queuedTail.next = reservation
	}
	lim.queuedTail = reservation
}

func (lim *Limiter) removeQueuedReservationLocked(reservation *queuedReservation) {
	if reservation.prev == nil {
		lim.queuedHead = reservation.next
	} else {
		reservation.prev.next = reservation.next
	}
	if reservation.next == nil {
		lim.queuedTail = reservation.prev
	} else {
		reservation.next.prev = reservation.prev
	}
	reservation.prev = nil
	reservation.next = nil
}

func (lim *Limiter) queuedTokensLocked() float64 {
	var tokens float64
	for reservation := lim.queuedHead; reservation != nil; reservation = reservation.next {
		tokens += reservation.tokens
	}
	return tokens
}

func (lim *Limiter) admitDueQueuedReservationsLocked(now time.Time) bool {
	changed := false
	for lim.queuedHead != nil && !lim.queuedHead.timeToAct.After(now) {
		reservation := lim.queuedHead
		lim.removeQueuedReservationLocked(reservation)
		reservation.state = queuedReservationReady
		changed = true
	}
	return changed
}

// reflowQueuedReservationsLocked rebuilds the dynamic queue from the current
// bucket state. lim.tokens already includes every waiting reservation's debit;
// the rebuild first restores those debits, then replays the FIFO and writes the
// resulting tail debt back to lim.tokens. The restored balance is virtual
// credit already committed to the queued reservations, so it must not be
// clamped to burst here. A later admission clamps any unused positive balance
// through getTokens.
func (lim *Limiter) reflowQueuedReservationsLocked(now time.Time) {
	changed := lim.admitDueQueuedReservationsLocked(now)
	if lim.queuedHead == nil {
		if changed {
			lim.signalQueuedUpdateLocked()
		}
		return
	}

	available := lim.tokens + lim.queuedTokensLocked()

	if lim.burst < 0 || lim.fillRate == Inf {
		for reservation := lim.queuedHead; reservation != nil; {
			next := reservation.next
			reservation.prev = nil
			reservation.next = nil
			reservation.timeToAct = now
			reservation.state = queuedReservationReady
			reservation = next
		}
		lim.queuedHead = nil
		lim.queuedTail = nil
		lim.tokens = available
		lim.signalQueuedUpdateLocked()
		return
	}

	for reservation := lim.queuedHead; reservation != nil; reservation = reservation.next {
		available -= reservation.tokens
		newTimeToAct := now
		if available < 0 {
			newTimeToAct = now.Add(lim.fillRate.durationFromTokens(-available))
		}
		if !reservation.timeToAct.Equal(newTimeToAct) {
			reservation.timeToAct = newTimeToAct
			changed = true
		}
	}
	lim.tokens = available
	if changed {
		lim.signalQueuedUpdateLocked()
	}
}

// waitN reserves tokens for the resource-control request path and waits until
// the dynamically reflowed reservation is ready. maxInitialWait is checked
// only when the reservation is admitted; an accepted reservation keeps its
// FIFO position until it is ready or its context is canceled.
func (lim *Limiter) waitN(
	ctx context.Context, maxInitialWait time.Duration, now time.Time, tokens float64,
) (time.Duration, error) {
	return lim.reserveQueued(ctx, maxInitialWait, now, tokens).wait(ctx)
}

func (lim *Limiter) reserveQueued(
	ctx context.Context, maxInitialWait time.Duration, now time.Time, tokensToReserve float64,
) *queuedReservation {
	if err := ctx.Err(); err != nil {
		return &queuedReservation{state: queuedReservationRejected, err: err}
	}

	lim.mu.Lock()
	defer lim.mu.Unlock()

	effectiveNow := lim.effectiveNowLocked(now)
	_, currentTokens := lim.getTokens(effectiveNow)
	if lim.admitDueQueuedReservationsLocked(effectiveNow) {
		lim.signalQueuedUpdateLocked()
	}
	waitLimit := maxInitialWait
	if deadline, ok := ctx.Deadline(); ok {
		if deadlineWait := deadline.Sub(effectiveNow); deadlineWait < waitLimit {
			waitLimit = deadlineWait
		}
	}
	if lim.burst < 0 || lim.fillRate == Inf {
		lim.updateLast(effectiveNow)
		lim.tokens = currentTokens
		return &queuedReservation{
			lim:       lim,
			state:     queuedReservationReady,
			tokens:    tokensToReserve,
			timeToAct: effectiveNow,
		}
	}

	remainingTokens := currentTokens - tokensToReserve
	var waitDuration time.Duration
	if remainingTokens < 0 {
		waitDuration = lim.fillRate.durationFromTokens(-remainingTokens)
	}
	reservation := &queuedReservation{
		lim:              lim,
		state:            queuedReservationRejected,
		tokens:           tokensToReserve,
		needWaitDuration: waitDuration,
		fillRate:         lim.fillRate,
		remainingTokens:  remainingTokens,
	}
	if waitDuration > waitLimit {
		lim.tokens = currentTokens
		lim.onReservationRejectedLocked(effectiveNow, waitDuration, waitLimit, tokensToReserve)
		return reservation
	}

	lim.updateLast(effectiveNow)
	lim.tokens = remainingTokens
	lim.maybeNotify()
	reservation.timeToAct = effectiveNow.Add(waitDuration)
	if waitDuration <= 0 {
		reservation.state = queuedReservationReady
		return reservation
	}
	reservation.state = queuedReservationWaiting
	lim.appendQueuedReservationLocked(reservation)
	return reservation
}

func (reservation *queuedReservation) wait(ctx context.Context) (time.Duration, error) {
	if reservation == nil {
		return 0, nil
	}
	if reservation.lim == nil {
		if reservation.err != nil {
			return reservation.needWaitDuration, reservation.err
		}
		return reservation.needWaitDuration, errs.ErrClientResourceGroupThrottled.FastGenByArgs(
			reservation.needWaitDuration, reservation.fillRate, reservation.remainingTokens)
	}

	start := time.Now()
	firstSelect := true
	for {
		now := time.Now()
		reservation.lim.mu.Lock()
		switch reservation.state {
		case queuedReservationRejected:
			reservation.lim.mu.Unlock()
			if reservation.err != nil {
				return reservation.needWaitDuration, reservation.err
			}
			return reservation.needWaitDuration, errs.ErrClientResourceGroupThrottled.FastGenByArgs(
				reservation.needWaitDuration, reservation.fillRate, reservation.remainingTokens)
		case queuedReservationReady:
			reservation.lim.mu.Unlock()
			if firstSelect {
				return 0, nil
			}
			return time.Since(start), nil
		case queuedReservationCanceled:
			reservation.lim.mu.Unlock()
			return 0, errQueuedReservationCanceled
		case queuedReservationWaiting:
			if !reservation.timeToAct.After(now) {
				reservation.state = queuedReservationReady
				reservation.lim.removeQueuedReservationLocked(reservation)
				reservation.lim.mu.Unlock()
				return time.Since(start), nil
			}
			delay := reservation.timeToAct.Sub(now)
			updateCh := reservation.lim.queuedUpdateChannelLocked()
			reservation.lim.mu.Unlock()

			if firstSelect {
				failpoint.InjectCall("waitReservationsBeforeSelect")
				firstSelect = false
			}
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-updateCh:
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				if reservation.cancelAt(time.Now()) {
					return 0, ctx.Err()
				}
				reservation.lim.mu.Lock()
				state := reservation.state
				reservation.lim.mu.Unlock()
				if state == queuedReservationReady {
					return time.Since(start), nil
				}
				if state == queuedReservationCanceled {
					return 0, errQueuedReservationCanceled
				}
				return 0, ctx.Err()
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
	}
}

// cancelAt reports whether it won the waiting-to-canceled state transition.
func (reservation *queuedReservation) cancelAt(now time.Time) bool {
	if reservation == nil || reservation.lim == nil {
		return false
	}
	reservation.lim.mu.Lock()
	defer reservation.lim.mu.Unlock()
	if reservation.state != queuedReservationWaiting {
		return false
	}

	effectiveNow := reservation.lim.effectiveNowLocked(now)
	_, tokens := reservation.lim.getTokens(effectiveNow)
	reservation.lim.updateLast(effectiveNow)
	reservation.lim.tokens = tokens
	if reservation.lim.admitDueQueuedReservationsLocked(effectiveNow) {
		reservation.lim.signalQueuedUpdateLocked()
	}
	if reservation.state != queuedReservationWaiting {
		return false
	}

	reservation.lim.tokens += reservation.tokens
	reservation.lim.removeQueuedReservationLocked(reservation)
	reservation.state = queuedReservationCanceled
	reservation.lim.signalQueuedUpdateLocked()
	reservation.lim.reflowQueuedReservationsLocked(effectiveNow)
	return true
}
