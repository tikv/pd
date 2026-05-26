// Copyright 2018 TiKV Project Authors.
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

package syncer

import (
	"strconv"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	historyKey                = "historyIndex"
	defaultFlushCount         = 100
	historyBufferCapacityUnit = 10000
	historyBufferGrowFactor   = 8
	historyBufferShrinkRounds = 3
)

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	syncIndexGauge  = regionSyncerStatus.WithLabelValues("sync_index")
	firstIndexGauge = regionSyncerStatus.WithLabelValues("first_index")
	lastIndexGauge  = regionSyncerStatus.WithLabelValues("last_index")
)

var errHistoryBufferRetainOverflow = errors.New("history buffer retained range exceeds maximum capacity")

type historyBuffer struct {
	syncutil.RWMutex
	index                  uint64
	records                []*core.RegionInfo
	head                   int
	tail                   int
	size                   int
	baseCapacity           int
	maxCapacity            int
	capacityUnit           int
	retains                map[uint64]uint64
	nextRetainID           uint64
	observedRequiredWindow uint64
	lowWindowRounds        int
	kv                     kv.Base
	flushCount             int
}

type historyRetainer struct {
	h  *historyBuffer
	id uint64
}

func newHistoryBuffer(size int, kv kv.Base) *historyBuffer {
	baseCapacity := normalizeHistoryBufferCapacity(size, historyBufferCapacityUnit)
	return newHistoryBufferWithConfig(baseCapacity, baseCapacity*historyBufferGrowFactor, historyBufferCapacityUnit, kv)
}

func newHistoryBufferWithConfig(baseCapacity, maxCapacity, capacityUnit int, kv kv.Base) *historyBuffer {
	baseCapacity = normalizeHistoryBufferCapacity(baseCapacity, capacityUnit)
	maxCapacity = normalizeHistoryBufferCapacity(maxCapacity, capacityUnit)
	if maxCapacity < baseCapacity {
		maxCapacity = baseCapacity
	}
	// use an empty space to simplify operation
	size := baseCapacity + 1
	if size < 2 {
		size = 2
	}
	records := make([]*core.RegionInfo, size)
	h := &historyBuffer{
		records:      records,
		size:         size,
		baseCapacity: baseCapacity,
		maxCapacity:  maxCapacity,
		capacityUnit: capacityUnit,
		retains:      make(map[uint64]uint64),
		kv:           kv,
		flushCount:   defaultFlushCount,
	}
	h.reload()
	return h
}

func normalizeHistoryBufferCapacity(size, capacityUnit int) int {
	if capacityUnit <= 0 {
		capacityUnit = historyBufferCapacityUnit
	}
	capacity := capacityUnit
	for capacity < size {
		capacity *= 2
	}
	return capacity
}

func (h *historyBuffer) len() int {
	return h.distanceToTail(h.head)
}

func (h *historyBuffer) distanceToTail(pos int) int {
	if h.tail < pos {
		return h.tail + h.size - pos
	}
	return h.tail - pos
}

func (h *historyBuffer) nextIndex() uint64 {
	return h.index
}

func (h *historyBuffer) firstIndex() uint64 {
	return h.index - uint64(h.len())
}

func (h *historyBuffer) capacity() int {
	return h.size - 1
}

func (h *historyBuffer) record(r *core.RegionInfo) {
	h.Lock()
	defer h.Unlock()
	if retainIndex, ok := h.minRetainIndexLocked(); ok {
		window := uint64(0)
		if h.index+1 > retainIndex {
			window = h.index + 1 - retainIndex
		}
		h.ensureWindowLocked(window)
	}
	syncIndexGauge.Set(float64(h.index))
	h.records[h.tail] = r
	h.tail = (h.tail + 1) % h.size
	if h.tail == h.head {
		h.head = (h.head + 1) % h.size
	}
	h.index++
	h.flushCount--
	if h.flushCount <= 0 {
		h.persist()
		h.flushCount = defaultFlushCount
	}
}

func (h *historyBuffer) recordsFrom(index uint64) []*core.RegionInfo {
	h.RLock()
	defer h.RUnlock()
	var pos int
	if index < h.nextIndex() && index >= h.firstIndex() {
		pos = (h.head + int(index-h.firstIndex())) % h.size
	} else {
		return nil
	}
	records := make([]*core.RegionInfo, 0, h.distanceToTail(pos))
	for i := pos; i != h.tail; i = (i + 1) % h.size {
		records = append(records, h.records[i])
	}
	return records
}

func (h *historyBuffer) retainFrom(index uint64) *historyRetainer {
	h.Lock()
	defer h.Unlock()
	h.nextRetainID++
	id := h.nextRetainID
	h.retains[id] = index
	window := uint64(0)
	if h.index > index {
		window = h.index - index
	}
	h.ensureWindowLocked(window)
	return &historyRetainer{h: h, id: id}
}

func (r *historyRetainer) release() {
	if r == nil || r.h == nil {
		return
	}
	r.h.releaseRetain(r.id)
}

func (r *historyRetainer) overflowed() bool {
	if r == nil || r.h == nil {
		return false
	}
	return r.h.retainOverflowed(r.id)
}

func (h *historyBuffer) releaseRetain(id uint64) {
	h.Lock()
	defer h.Unlock()
	delete(h.retains, id)
}

func (h *historyBuffer) retainOverflowed(id uint64) bool {
	h.RLock()
	defer h.RUnlock()
	index, ok := h.retains[id]
	if !ok {
		return false
	}
	return index < h.firstIndex()
}

func (h *historyBuffer) observeRequiredWindow(window uint64) {
	h.Lock()
	defer h.Unlock()
	h.observeRequiredWindowLocked(window)
}

func (h *historyBuffer) observeRequiredWindowLocked(window uint64) {
	if window > h.observedRequiredWindow {
		h.observedRequiredWindow = window
	}
	if window > uint64(h.capacity()/2) {
		h.growForWindowLocked(window * 2)
	}
}

func (h *historyBuffer) maybeShrink() {
	h.Lock()
	defer h.Unlock()
	if len(h.retains) > 0 || h.capacity() <= h.baseCapacity {
		h.observedRequiredWindow = 0
		h.lowWindowRounds = 0
		return
	}
	if h.observedRequiredWindow > uint64(h.capacity()/4) {
		h.observedRequiredWindow = 0
		h.lowWindowRounds = 0
		return
	}
	h.lowWindowRounds++
	if h.lowWindowRounds < historyBufferShrinkRounds {
		h.observedRequiredWindow = 0
		return
	}
	target := h.baseCapacity
	if h.observedRequiredWindow > 0 {
		target = normalizeHistoryBufferCapacity(int(h.observedRequiredWindow*2), h.capacityUnit)
		if target < h.baseCapacity {
			target = h.baseCapacity
		}
	}
	if target < h.capacity() {
		h.resizeLocked(target)
	}
	h.observedRequiredWindow = 0
	h.lowWindowRounds = 0
}

func (h *historyBuffer) resetWithIndex(index uint64) {
	h.Lock()
	defer h.Unlock()
	h.resetWithIndexLocked(index)
}

func (h *historyBuffer) resetWithIndexLocked(index uint64) {
	h.index = index
	h.head = 0
	h.tail = 0
	h.flushCount = defaultFlushCount
	h.retains = make(map[uint64]uint64)
	h.observedRequiredWindow = 0
	h.lowWindowRounds = 0
	if h.capacity() > h.baseCapacity {
		h.resizeLocked(h.baseCapacity)
	}
}

func (h *historyBuffer) getNextIndex() uint64 {
	h.RLock()
	defer h.RUnlock()
	return h.index
}

func (h *historyBuffer) get(index uint64) *core.RegionInfo {
	h.RLock()
	defer h.RUnlock()
	return h.getLocked(index)
}

func (h *historyBuffer) reload() {
	v, err := h.kv.Load(historyKey)
	if err != nil {
		log.Warn("load history index failed", zap.String("error", err.Error()))
	}
	if v != "" {
		h.index, err = strconv.ParseUint(v, 10, 64)
		if err != nil {
			log.Fatal("load history index failed", errs.ZapError(errs.ErrStrconvParseUint, err))
		}
	}
	log.Info("start from history index", zap.Uint64("start-index", h.firstIndex()))
}

func (h *historyBuffer) persist() {
	firstIndexGauge.Set(float64(h.firstIndex()))
	lastIndexGauge.Set(float64(h.nextIndex()))
	err := h.kv.Save(historyKey, strconv.FormatUint(h.nextIndex(), 10))
	if err != nil {
		log.Warn("persist history index failed", zap.Uint64("persist-index", h.nextIndex()), errs.ZapError(err))
	}
}

func (h *historyBuffer) ensureWindowLocked(window uint64) {
	if window > h.observedRequiredWindow {
		h.observedRequiredWindow = window
	}
	if window <= uint64(h.capacity()) {
		return
	}
	h.growForWindowLocked(window * 2)
}

func (h *historyBuffer) growForWindowLocked(window uint64) {
	target := normalizeHistoryBufferCapacity(int(window), h.capacityUnit)
	if target > h.maxCapacity {
		target = h.maxCapacity
	}
	if target > h.capacity() {
		h.resizeLocked(target)
	}
}

func (h *historyBuffer) resizeLocked(newCapacity int) {
	newCapacity = normalizeHistoryBufferCapacity(newCapacity, h.capacityUnit)
	if newCapacity < h.baseCapacity {
		newCapacity = h.baseCapacity
	}
	if newCapacity > h.maxCapacity {
		newCapacity = h.maxCapacity
	}
	if newCapacity == h.capacity() {
		return
	}
	keep := h.len()
	if keep > newCapacity {
		keep = newCapacity
	}
	startIndex := h.index - uint64(keep)
	records := make([]*core.RegionInfo, newCapacity+1)
	for i := range keep {
		records[i] = h.getLocked(startIndex + uint64(i))
	}
	h.records = records
	h.size = newCapacity + 1
	h.head = 0
	h.tail = keep % h.size
}

func (h *historyBuffer) getLocked(index uint64) *core.RegionInfo {
	if index < h.nextIndex() && index >= h.firstIndex() {
		pos := (h.head + int(index-h.firstIndex())) % h.size
		return h.records[pos]
	}
	return nil
}

func (h *historyBuffer) minRetainIndexLocked() (uint64, bool) {
	var (
		minIndex uint64
		ok       bool
	)
	for _, index := range h.retains {
		if !ok || index < minIndex {
			minIndex = index
			ok = true
		}
	}
	return minIndex, ok
}
