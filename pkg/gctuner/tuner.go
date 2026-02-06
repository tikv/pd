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

package gctuner

import (
	"math"
	"os"
	"strconv"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	util "github.com/tikv/pd/pkg/gogc"
	"github.com/tikv/pd/pkg/memory"
)

var (
	maxGCPercent atomic.Uint32
	minGCPercent atomic.Uint32

	// EnableGOGCTuner is to control whether enable the GOGC tuner.
	EnableGOGCTuner atomic.Bool

	defaultGCPercent uint32 = 100
)

const (
	defaultMaxGCPercent uint32 = 500
	defaultMinGCPercent uint32 = 100
)

// SetMaxGCPercent sets the max cost of memory.
func SetMaxGCPercent(percent uint32) {
	maxGCPercent.Store(percent)
}

// SetMinGCPercent sets the max cost of memory.
func SetMinGCPercent(percent uint32) {
	minGCPercent.Store(percent)
}

func init() {
	if val, err := strconv.Atoi(os.Getenv("GOGC")); err == nil {
		defaultGCPercent = uint32(val)
	}
	SetMinGCPercent(defaultMinGCPercent)
	SetMaxGCPercent(defaultMaxGCPercent)
}

// Tuning sets the threshold of heap which will be respect by gogc tuner.
// When Tuning, the env GOGC will not be take effect.
// threshold: disable tuning if threshold == 0
func Tuning(threshold uint64) {
	if t := globalTuner.Load(); t == nil {
		// init gc tuner only when threshold > 0, otherwise do nothing
		if threshold > 0 {
			t1 := newTuner(threshold)
			globalTuner.CompareAndSwap(nil, &t1)
		}
	} else {
		if threshold <= 0 {
			(*t).stop()
			globalTuner.CompareAndSwap(t, nil)
		} else {
			(*t).setThreshold(threshold)
		}
	}
}

// GetGOGCPercent returns the current GCPercent.
func GetGOGCPercent() (percent uint32) {
	t := globalTuner.Load()
	if t == nil {
		percent = defaultGCPercent
	} else {
		percent = (*t).getGCPercent()
	}
	return percent
}

// only allow one gc tuner in one process
// It is not thread-safe. so it is a private, singleton pattern to avoid misuse.
var globalTuner atomic.Pointer[*tuner]

/*
// 			Heap
//  _______________  => limit: host/cgroup memory hard limit
// |               |
// |---------------| => threshold: increase GCPercent when gc_trigger < threshold
// |               |
// |---------------| => gc_trigger: heap_live + heap_live * GCPercent / 100
// |               |
// |---------------|
// |   heap_live   |
// |_______________|
*/
// Go runtime only trigger GC when hit gc_trigger which affected by GCPercent and heap_live.
// So we can change GCPercent dynamically to tuning GC performance.
type tuner struct {
	finalizer *finalizer
	gcPercent atomic.Uint32
	threshold atomic.Uint64 // high water level, in bytes
}

func newTuner(threshold uint64) *tuner {
	log.Info("new gctuner", zap.Uint64("threshold", threshold))
	t := &tuner{}
	t.gcPercent.Store(defaultGCPercent)
	t.threshold.Store(threshold)
	t.finalizer = newFinalizer(t.tuning) // start tuning
	return t
}

func (t *tuner) stop() {
	t.finalizer.stop()
	log.Info("gctuner stop")
}

func (t *tuner) setThreshold(threshold uint64) {
	t.threshold.Store(threshold)
}

func (t *tuner) getThreshold() uint64 {
	return t.threshold.Load()
}

func (t *tuner) setGCPercent(percent uint32) {
	util.SetGCPercent(int(percent))
	t.gcPercent.Store(percent)
}

func (t *tuner) getGCPercent() uint32 {
	return t.gcPercent.Load()
}

// tuning check the memory inuse and tune GC percent dynamically.
// Go runtime ensure that it will be called serially.
func (t *tuner) tuning() {
	if !EnableGOGCTuner.Load() {
		return
	}

	inuse := readMemoryInuse()
	threshold := t.getThreshold()
	log.Info("tuning", zap.Uint64("inuse", inuse), zap.Uint64("threshold", threshold),
		zap.Bool("enable-go-gc-tuner", EnableGOGCTuner.Load()))
	// stop gc tuning
	if threshold <= 0 {
		return
	}
	if EnableGOGCTuner.Load() {
		percent := calcGCPercent(inuse, threshold)
		log.Info("tuning", zap.Uint64("inuse", inuse),
			zap.Uint64("threshold", threshold),
			zap.Bool("enable-go-gc-tuner", EnableGOGCTuner.Load()),
			zap.Uint32("new-gc-percent", percent),
		)
		t.setGCPercent(percent)
	}
}

// threshold = inuse + inuse * (gcPercent / 100)
// => gcPercent = (threshold - inuse) / inuse * 100
// if threshold < inuse*2, so gcPercent < 100, and GC positively to avoid OOM
// if threshold > inuse*2, so gcPercent > 100, and GC negatively to reduce GC times
func calcGCPercent(inuse, threshold uint64) uint32 {
	// invalid params
	if inuse == 0 || threshold == 0 {
		return defaultGCPercent
	}
	// inuse heap larger than threshold, use min percent
	if threshold <= inuse {
		return minGCPercent.Load()
	}
	gcPercent := uint32(math.Floor(float64(threshold-inuse) / float64(inuse) * 100))
	if gcPercent < minGCPercent.Load() {
		return minGCPercent.Load()
	} else if gcPercent > maxGCPercent.Load() {
		return maxGCPercent.Load()
	}
	return gcPercent
}

// Config holds the configuration for GC tuner initialization/update.
type Config struct {
	EnableGOGCTuner            bool
	GCTunerThreshold           float64
	ServerMemoryLimit          float64
	ServerMemoryLimitGCTrigger float64
}

// State holds the state for detecting config changes.
type State struct {
	totalMem                  uint64
	EnableGCTuner             bool
	GCThresholdBytes          uint64
	MemoryLimitBytes          uint64
	MemoryLimitGCTriggerRatio float64
	MemoryLimitGCTriggerBytes uint64
}

// InitGCTuner initializes the GC tuner with the given config and total memory.
// Returns the initial state that can be used for subsequent updates.
func InitGCTuner(totalMem uint64, cfg *Config) *State {
	state := &State{totalMem: totalMem}
	state.EnableGCTuner = cfg.EnableGOGCTuner
	state.MemoryLimitBytes = uint64(float64(totalMem) * cfg.ServerMemoryLimit)
	state.GCThresholdBytes = uint64(float64(state.MemoryLimitBytes) * cfg.GCTunerThreshold)
	if state.MemoryLimitBytes == 0 {
		state.GCThresholdBytes = uint64(float64(totalMem) * cfg.GCTunerThreshold)
	}
	state.MemoryLimitGCTriggerRatio = cfg.ServerMemoryLimitGCTrigger
	state.MemoryLimitGCTriggerBytes = uint64(float64(state.MemoryLimitBytes) * state.MemoryLimitGCTriggerRatio)

	// Initialize GC tuner
	Tuning(state.GCThresholdBytes)
	EnableGOGCTuner.Store(state.EnableGCTuner)
	log.Info("initialize gc tuner",
		zap.Bool("enable-gc-tuner", state.EnableGCTuner),
		zap.Uint64("gc-threshold-bytes", state.GCThresholdBytes))

	// Initialize memory limit tuner
	memory.ServerMemoryLimit.Store(state.MemoryLimitBytes)
	GlobalMemoryLimitTuner.SetPercentage(state.MemoryLimitGCTriggerRatio)
	GlobalMemoryLimitTuner.UpdateMemoryLimit()
	log.Info("initialize gc memory limit",
		zap.Uint64("memory-limit-bytes", state.MemoryLimitBytes),
		zap.Float64("memory-limit-gc-trigger-ratio", state.MemoryLimitGCTriggerRatio))

	return state
}

// UpdateIfNeeded checks if the config has changed and updates the GC tuner.
// Returns true if any updates were made.
func (s *State) UpdateIfNeeded(cfg *Config) bool {
	updated := false

	newEnableGCTuner := cfg.EnableGOGCTuner
	newMemoryLimitBytes := uint64(float64(s.totalMem) * cfg.ServerMemoryLimit)
	newGCThresholdBytes := uint64(float64(newMemoryLimitBytes) * cfg.GCTunerThreshold)
	if newMemoryLimitBytes == 0 {
		newGCThresholdBytes = uint64(float64(s.totalMem) * cfg.GCTunerThreshold)
	}
	newMemoryLimitGCTriggerRatio := cfg.ServerMemoryLimitGCTrigger
	newMemoryLimitGCTriggerBytes := uint64(float64(newMemoryLimitBytes) * newMemoryLimitGCTriggerRatio)

	// Check and update GC tuner
	if newEnableGCTuner != s.EnableGCTuner || newGCThresholdBytes != s.GCThresholdBytes {
		s.EnableGCTuner = newEnableGCTuner
		s.GCThresholdBytes = newGCThresholdBytes
		Tuning(s.GCThresholdBytes)
		EnableGOGCTuner.Store(s.EnableGCTuner)
		log.Info("update gc tuner",
			zap.Bool("enable-gc-tuner", s.EnableGCTuner),
			zap.Uint64("gc-threshold-bytes", s.GCThresholdBytes))
		updated = true
	}

	// Check and update memory limit tuner
	if newMemoryLimitBytes != s.MemoryLimitBytes || newMemoryLimitGCTriggerRatio != s.MemoryLimitGCTriggerRatio {
		s.MemoryLimitBytes = newMemoryLimitBytes
		s.MemoryLimitGCTriggerRatio = newMemoryLimitGCTriggerRatio
		s.MemoryLimitGCTriggerBytes = newMemoryLimitGCTriggerBytes
		memory.ServerMemoryLimit.Store(s.MemoryLimitBytes)
		GlobalMemoryLimitTuner.SetPercentage(s.MemoryLimitGCTriggerRatio)
		GlobalMemoryLimitTuner.UpdateMemoryLimit()
		log.Info("update gc memory limit",
			zap.Uint64("memory-limit-bytes", s.MemoryLimitBytes),
			zap.Float64("memory-limit-gc-trigger-ratio", s.MemoryLimitGCTriggerRatio))
		updated = true
	}

	return updated
}
