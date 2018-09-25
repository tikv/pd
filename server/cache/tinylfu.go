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

package cache

import (
	"math"
)

const (
	falsePositiveProbability = 0.1
	percentEden              = 0.01
	sampleMultiplier         = 10
	sketchDepth              = 4
)

// TinyLFU is
type TinyLFU struct {
	doorkeeper *Doorkeeper
	cms        *CountMinSketch

	additions int
	samples   int

	eden *LRU
	main *SLRU
}

func newTinyLFU(size int) *TinyLFU {
	samples := size * sampleMultiplier
	doorkeeper := newDoorkeeper(size, falsePositiveProbability)
	cms := newCountMinSketch(size)
	edenCapacity := int(float64(size) * percentEden)
	if edenCapacity < 1 {
		edenCapacity = 1
	}
	return &TinyLFU{
		doorkeeper: doorkeeper,
		cms:        cms,
		samples:    samples,
		eden:       newLRU(edenCapacity),
		main:       newSLRU(size - edenCapacity),
	}
}

// Put puts an item into cache.
func (c *TinyLFU) Put(key uint64, value interface{}) {
	c.increase(fnvU64(key))
	candidate := c.eden.put(key, value)
	if candidate == nil {
		return
	}
	victim := c.main.victim()
	if victim == nil {
		c.main.Put(candidate.Key, candidate.Value)
		return
	}
	candidateFeq := c.estimate(fnvU64(candidate.Key))
	victimFeq := c.estimate(fnvU64(victim.Key))
	if candidateFeq > victimFeq {
		c.main.Put(candidate.Key, candidate.Value)
	}
}

// Get retrives an item from cache.
func (c *TinyLFU) Get(key uint64) (interface{}, bool) {
	c.increase(fnvU64(key))
	if val, ok := c.eden.Get(key); ok {
		return val, ok
	}
	if val, ok := c.main.Get(key); ok {
		return val, ok
	}
	return nil, false
}

// Peek reads an item from cache. The action is no considered 'Use'.
func (c *TinyLFU) Peek(key uint64) (interface{}, bool) {
	if val, ok := c.eden.Peek(key); ok {
		return val, ok
	}
	if val, ok := c.main.Peek(key); ok {
		return val, ok
	}
	return nil, false
}

// Remove removes an item from cache.
func (c *TinyLFU) Remove(key uint64) {
	if c.eden.remove(key) {
		return
	}
	if c.main.remove(key) {
		return
	}
}

// Elems return all items in cache.
func (c *TinyLFU) Elems() []*Item {
	size := c.Len()
	elems := make([]*Item, 0, size)
	elems = append(elems, c.eden.Elems()...)
	elems = append(elems, c.main.Elems()...)
	return elems
}

// Len returns current cache size.
func (c *TinyLFU) Len() int {
	return c.eden.Len() + c.main.Len()
}

func (c *TinyLFU) increase(h uint64) {
	c.additions++
	if c.additions >= c.samples {
		c.doorkeeper.reset()
		c.cms.reset()
		c.additions = 0
	}
	if c.doorkeeper.put(h) {
		c.cms.add(h)
	}
}

func (c *TinyLFU) estimate(h uint64) uint8 {
	freq := c.cms.estimate(h)
	if c.doorkeeper.contains(h) {
		freq++
	}
	return freq
}

// Doorkeeper is an implementation of the bloom filter.
type Doorkeeper struct {
	numHashes uint32
	bitsMask  uint32
	bits      []uint64
}

func newDoorkeeper(capacity int, fpp float64) *Doorkeeper {
	ln2 := math.Log(2.0)
	factor := -math.Log(fpp) / math.Pow(ln2, 2)

	numBits := ceilingNextPowerOfTwo(uint32(float64(capacity) * factor))
	if numBits < 1024 {
		numBits = 1024
	}
	bitsMask := numBits - 1

	numHashes := uint32(0.7 * float64(numBits) / float64(capacity))
	if numHashes < 2 {
		numHashes = 2
	}

	size := int(numBits+63) / 64
	bits := make([]uint64, size)
	return &Doorkeeper{
		numHashes: numHashes,
		bitsMask:  bitsMask,
		bits:      bits,
	}
}

func (d *Doorkeeper) put(h uint64) bool {
	h1, h2 := uint32(h), uint32(h>>32)
	var o uint = 1
	for i := uint32(0); i < d.numHashes; i++ {
		o &= d.set((h1 + (i * h2)) & d.bitsMask)
	}
	return o == 1
}

func (d *Doorkeeper) contains(h uint64) bool {
	h1, h2 := uint32(h), uint32(h>>32)
	var o uint = 1
	for i := uint32(0); i < d.numHashes; i++ {
		o &= d.get((h1 + (i * h2)) & d.bitsMask)
	}
	return o == 1
}

func (d *Doorkeeper) set(i uint32) uint {
	idx, shift := i/64, i%64
	val := d.bits[idx]
	mask := uint64(1) << shift
	d.bits[idx] |= mask
	return uint((val & mask) >> shift)
}

func (d *Doorkeeper) get(i uint32) uint {
	idx, shift := i/64, i%64
	val := d.bits[idx]
	mask := uint64(1) << shift
	return uint((val & mask) >> shift)
}

func (d *Doorkeeper) reset() {
	for i := range d.bits {
		d.bits[i] = 0
	}
}

type counter []byte

func newCounter(width int) counter {
	return make(counter, width/2)
}

func (c counter) get(i uint32) byte {
	return byte(c[i/2]>>((i&1)*4)) & 0x0f
}

func (c counter) inc(i uint32) {
	idx := i / 2
	shift := (i & 1) * 4
	v := (c[idx] >> shift) & 0x0f
	if v < 15 {
		c[idx] += 1 << shift
	}
}

func (c counter) reset() {
	for i := range c {
		c[i] = (c[i] >> 1) & 0x77
	}
}

// CountMinSketch is an implementation of count-min sketch algorithm.
type CountMinSketch struct {
	counters [sketchDepth]counter
	mask     uint32
}

func newCountMinSketch(width int) *CountMinSketch {
	size := ceilingNextPowerOfTwo(uint32(width))
	c := CountMinSketch{mask: size - 1}
	for i := 0; i < sketchDepth; i++ {
		c.counters[i] = newCounter(int(size))
	}
	return &c
}

func (c *CountMinSketch) add(h uint64) {
	h1, h2 := uint32(h), uint32(h>>32)

	for i := range c.counters {
		pos := (h1 + uint32(i)*h2) & c.mask
		c.counters[i].inc(pos)
	}
}

func (c *CountMinSketch) estimate(h uint64) uint8 {
	h1, h2 := uint32(h), uint32(h>>32)

	var min byte = 0xFF
	for i := 0; i < sketchDepth; i++ {
		pos := (h1 + uint32(i)*h2) & c.mask
		v := c.counters[i].get(pos)
		if v < min {
			min = v
		}
	}
	return min
}

func (c *CountMinSketch) reset() {
	for _, c := range c.counters {
		c.reset()
	}
}

func ceilingNextPowerOfTwo(i uint32) uint32 {
	n := i - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}

const (
	fnvOffset uint64 = 14695981039346656037
	fnvPrime  uint64 = 1099511628211
)

func fnvU64(v uint64) uint64 {
	h := fnvOffset
	for i := uint(0); i < 64; i += 8 {
		h ^= (v >> i) & 0xFF
		h *= fnvPrime
	}
	return h
}
