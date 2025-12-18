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
	// disable gc tuner if percent is zero
	if t := globalTuner.Load(); t == nil {
		t1 := newTuner(threshold)
		globalTuner.CompareAndSwap(nil, &t1)
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

// GCTunerCfg is the configuration for the GCTuner.
type GCTunerCfg struct {
	EnableGOGCTuner            bool
	GCTunerThreshold           float64
	ServerMemoryLimit          float64
	ServerMemoryLimitGCTrigger float64
	MemoryLimitBytes           uint64
	MemoryLimitGCTriggerBytes  uint64
	MemoryLimitGCTriggerRatio  float64
	ThresholdBytes             uint64
}

// NewGCTunerCfg creates a new GCTunerCfg with default configuration.
func NewGCTunerCfg(enableGOGCTuner bool, gcTunerThreshold float64, serverMemoryLimit float64, serverMemoryLimitGCTrigger float64) *GCTunerCfg {
	return &GCTunerCfg{
		EnableGOGCTuner:            enableGOGCTuner,
		GCTunerThreshold:           gcTunerThreshold,
		ServerMemoryLimit:          serverMemoryLimit,
		ServerMemoryLimitGCTrigger: serverMemoryLimitGCTrigger,
	}
}

// GCTunerChecker is the checker for the GCTuner.
type GCTunerChecker struct {
	cfg *GCTunerCfg
}

// NewGCTunerChecker creates a new GCTunerChecker with default configuration.
func NewGCTunerChecker() *GCTunerChecker {
	cfg := NewGCTunerCfg(false, 0, 0, 0)
	return &GCTunerChecker{cfg: cfg}
}

func (g *GCTunerChecker) SetGCTunerCfg(cfg *GCTunerCfg) {
	totalMem, err := memory.MemTotal()
	if err != nil {
		log.Warn("fail to get total memory", zap.Error(err))
		return
	}
	log.Info("memory info", zap.Uint64("total-mem", totalMem))
	if cfg.EnableGOGCTuner != g.cfg.EnableGOGCTuner || cfg.GCTunerThreshold != g.cfg.GCTunerThreshold {
		cfg.EnableGOGCTuner = g.cfg.EnableGOGCTuner
		cfg.GCTunerThreshold = g.cfg.GCTunerThreshold
		g.updateGCTuner()
	}

	newMemoryLimitGCTriggerRatio := cfg.ServerMemoryLimitGCTrigger
	newMemoryLimitBytes := uint64(float64(totalMem) * cfg.ServerMemoryLimit)
	newMemoryLimitGCTriggerBytes := uint64(float64(newMemoryLimitBytes) * newMemoryLimitGCTriggerRatio)
	newThresholdBytes := uint64(float64(newMemoryLimitBytes) * cfg.GCTunerThreshold)
	if newMemoryLimitBytes == 0 {
		newThresholdBytes = uint64(float64(totalMem) * cfg.GCTunerThreshold)
	}
	g.cfg.ThresholdBytes = newThresholdBytes
	if g.cfg.MemoryLimitBytes != newMemoryLimitBytes || g.cfg.MemoryLimitGCTriggerBytes != newMemoryLimitGCTriggerBytes {
		g.cfg.MemoryLimitBytes = newMemoryLimitBytes
		g.cfg.MemoryLimitGCTriggerBytes = newMemoryLimitGCTriggerBytes
		g.cfg.MemoryLimitGCTriggerRatio = newMemoryLimitGCTriggerRatio
		g.updateGCMemLimit()
	}
}

func (g *GCTunerChecker) updateGCTuner() {
	Tuning(g.cfg.ThresholdBytes)
	EnableGOGCTuner.Store(g.cfg.EnableGOGCTuner)
	log.Info("update gc tuner",
		zap.Bool("enable-gc-tuner", g.cfg.EnableGOGCTuner),
		zap.Uint64("gc-threshold-bytes", g.cfg.ThresholdBytes))
}

func (g *GCTunerChecker) updateGCMemLimit() {
	memory.ServerMemoryLimit.Store(g.cfg.MemoryLimitBytes)
	GlobalMemoryLimitTuner.SetPercentage(g.cfg.MemoryLimitGCTriggerRatio)
	GlobalMemoryLimitTuner.UpdateMemoryLimit()
	log.Info("update gc memory limit",
		zap.Uint64("memory-limit-bytes", g.cfg.MemoryLimitBytes),
		zap.Float64("memory-limit-gc-trigger-ratio", g.cfg.MemoryLimitGCTriggerRatio))
}
