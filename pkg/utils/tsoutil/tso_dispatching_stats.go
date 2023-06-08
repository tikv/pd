// Copyright 2023 TiKV Project Authors.
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

package tsoutil

import (
	"sync"
	"sync/atomic"
)

// TSODispatchingStats records the statistics of TSO dispatching.
type TSODispatchingStats struct {
	// Count the number of TSO streaming routines.
	streamingRoutinesLock     sync.RWMutex
	aliveTSOStreamingRoutines int64
	peakTSOStreamingRoutines  int64

	// Count the number of dispatchers.
	dispatcherCountLock  sync.RWMutex
	aliveDispatcherCount int64
	peakDispatcherCount  int64

	dispatcherExitCount atomic.Int64
}

// NewTSODispatchingStats creates a TSODispatchingStats.
func NewTSODispatchingStats(
	aliveTSOStreamingRoutine int64,
	peakTSOStreamingRoutines int64,
	aliveDispatcherCount int64,
	peakDispatcherCount int64,
	dispatcherExitCount int64,
) *TSODispatchingStats {
	stats := &TSODispatchingStats{
		aliveTSOStreamingRoutines: aliveTSOStreamingRoutine,
		peakTSOStreamingRoutines:  peakTSOStreamingRoutines,

		aliveDispatcherCount: aliveDispatcherCount,
		peakDispatcherCount:  peakDispatcherCount,
	}
	stats.dispatcherExitCount.Store(dispatcherExitCount)
	return stats
}

// GetAliveTSOStreamingRoutines returns the current value.
func (s *TSODispatchingStats) GetAliveTSOStreamingRoutines() int64 {
	s.streamingRoutinesLock.RLock()
	defer s.streamingRoutinesLock.RUnlock()
	return s.aliveTSOStreamingRoutines
}

// GetPeakTSOStreamingRoutines returns the current value of peakTSOStreamingRoutines.
func (s *TSODispatchingStats) GetPeakTSOStreamingRoutines() int64 {
	s.streamingRoutinesLock.RLock()
	defer s.streamingRoutinesLock.RUnlock()
	return s.peakTSOStreamingRoutines
}

// GetAliveDispatcherCount returns the current value of aliveDispatcherCount.
func (s *TSODispatchingStats) GetAliveDispatcherCount() int64 {
	s.dispatcherCountLock.RLock()
	defer s.dispatcherCountLock.RUnlock()
	return s.aliveDispatcherCount
}

// GetPeakDispatcherCount returns the current value of peakDispatcherCount.
func (s *TSODispatchingStats) GetPeakDispatcherCount() int64 {
	s.dispatcherCountLock.RLock()
	defer s.dispatcherCountLock.RUnlock()
	return s.peakDispatcherCount
}

// GetDispatcherExitCount returns the current value of dispatcherExitCount.
func (s *TSODispatchingStats) GetDispatcherExitCount() int64 {
	return s.dispatcherExitCount.Load()
}

// EnterTSOStreamingRoutine is called when entering into a TSO streaming routine.
func (s *TSODispatchingStats) EnterTSOStreamingRoutine() {
	s.streamingRoutinesLock.Lock()
	defer s.streamingRoutinesLock.Unlock()
	s.aliveDispatcherCount++
	if s.aliveDispatcherCount > s.peakDispatcherCount {
		s.peakDispatcherCount = s.aliveDispatcherCount
	}
}

// LeaveTSOStreamingRoutine is called when a TSO streaming routine exits.
func (s *TSODispatchingStats) LeaveTSOStreamingRoutine() {
	s.streamingRoutinesLock.Lock()
	defer s.streamingRoutinesLock.Unlock()
	s.aliveDispatcherCount--
}

// EnterDispatcher is called when entering into a dispatcher.
func (s *TSODispatchingStats) EnterDispatcher() {
	s.dispatcherCountLock.Lock()
	defer s.dispatcherCountLock.Unlock()
	s.aliveDispatcherCount++
	if s.aliveDispatcherCount > s.peakDispatcherCount {
		s.peakDispatcherCount = s.aliveDispatcherCount
	}
}

// LeaveDispatcher is called when a dispatcher exits.
func (s *TSODispatchingStats) LeaveDispatcher() {
	s.dispatcherCountLock.Lock()
	s.aliveDispatcherCount--
	s.dispatcherCountLock.Unlock()

	s.dispatcherExitCount.Add(1)
}
