// Copyright 2021 TiKV Project Authors.
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

package rangelist

import (
	"bytes"
	"sort"
)

type splitPointType int

const (
	tStart splitPointType = iota
	tEnd
)

// splitPoint represents key that exists in items.
type splitPoint struct {
	ty   splitPointType
	key  []byte
	data any
}

// Builder is used to create key range list.
type Builder struct {
	compare     compareFunc
	splitPoints []splitPoint
}

// NewBuilder creates a key range list builder.
func NewBuilder() *Builder {
	return &Builder{}
}

// SetCompareFunc sets up the comparer to determine item order (ascending) for a key range.
func (b *Builder) SetCompareFunc(f func(a, b any) int) {
	b.compare = f
}

// AddItem pushes an item to key range list.
func (b *Builder) AddItem(start, end []byte, data any) {
	b.splitPoints = append(b.splitPoints, splitPoint{ty: tStart, key: start, data: data})
	if len(end) > 0 {
		b.splitPoints = append(b.splitPoints, splitPoint{ty: tEnd, key: end, data: data})
	}
}

// An item slice that keeps items in ascending order.
type sortedItems struct {
	items []any
}

func (si *sortedItems) insertItem(item any, comparer compareFunc) {
	pos := len(si.items)
	if comparer != nil {
		pos = sort.Search(len(si.items), func(i int) bool {
			return comparer(si.items[i], item) > 0
		})
	}
	if pos == len(si.items) {
		si.items = append(si.items, item)
		return
	}
	si.items = append(si.items[:pos+1], si.items[pos:]...)
	si.items[pos] = item
}

func (si *sortedItems) deleteItem(del any) {
	for i, item := range si.items {
		if item == del {
			si.items = append(si.items[:i], si.items[i+1:]...)
			return
		}
	}
}

// Build creates the key range list.
func (b *Builder) Build() List {
	sort.Slice(b.splitPoints, func(i, j int) bool {
		return bytes.Compare(b.splitPoints[i].key, b.splitPoints[j].key) < 0
	})

	// determine items for each range.
	var l List
	var si sortedItems
	for i, p := range b.splitPoints {
		switch p.ty {
		case tStart:
			si.insertItem(p.data, b.compare)
		case tEnd:
			si.deleteItem(p.data)
		}
		if i == len(b.splitPoints)-1 || !bytes.Equal(p.key, b.splitPoints[i+1].key) {
			// next key is different, push si to l
			seg := segment{
				startKey: p.key,
				data:     append(si.items[:0:0], si.items...),
			}
			l.segments = append(l.segments, seg)
		}
	}
	return l
}
