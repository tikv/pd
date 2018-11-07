// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package syncer

import (
	"strconv"

	"github.com/pingcap/pd/server/core"
	log "github.com/sirupsen/logrus"
)

const (
	historyKey        = "historyIndex"
	defaultFlushCount = 100
)

type historyBuffer struct {
	index      uint64
	records    []*core.RegionInfo
	head       int
	tail       int
	size       int
	kv         core.KVBase
	flushCount int
}

func newHistoryBuffer(size int, kv core.KVBase) *historyBuffer {
	// use an empty space to simplify operation
	size++
	if size < 2 {
		size = 2
	}
	records := make([]*core.RegionInfo, size)
	h := &historyBuffer{
		records:    records,
		size:       size,
		kv:         kv,
		flushCount: defaultFlushCount,
	}
	h.reload()
	return h
}

func (h *historyBuffer) len() int {
	if h.tail < h.head {
		return h.tail + h.size - h.head
	}
	return h.tail - h.head
}

func (h *historyBuffer) nextIndex() uint64 {
	return h.index
}

func (h *historyBuffer) firstIndex() uint64 {
	return h.index - uint64(h.len())
}

func (h *historyBuffer) record(r *core.RegionInfo) {
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

func (h *historyBuffer) get(index uint64) *core.RegionInfo {
	if index < h.nextIndex() && index >= h.firstIndex() {
		pos := (h.head + int(index-h.firstIndex())) % h.size
		return h.records[pos]
	}
	return nil
}

func (h *historyBuffer) reload() {
	v, err := h.kv.Load(historyKey)
	if err != nil {
		log.Warnf("load history index failed: %s", err)
	}
	if v != "" {
		h.index, err = strconv.ParseUint(v, 10, 64)
		if err != nil {
			log.Fatalf("load history index failed: %s", err)
		}
	}
}

func (h *historyBuffer) persist() {
	err := h.kv.Save(historyKey, strconv.FormatUint(h.nextIndex(), 10))
	if err != nil {
		log.Warnf("persist history index (%d) failed: %v", h.nextIndex(), err)
	}
}
