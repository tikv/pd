// Copyright 2019 PingCAP, Inc.
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

package storelimit

import (
	"time"

	"github.com/juju/ratelimit"
)

const (
	// RegionInfluence represents the influence of a operator step, which is used by store limit.
	RegionInfluence int64 = 1000
	// SmallRegionInfluence represents the influence of a operator step
	// when the region size is smaller than smallRegionThreshold, which is used by store limit.
	SmallRegionInfluence int64 = 200
	// SmallRegionThreshold is used to represent a region which can be regarded as a small region once the size is small than it.
	SmallRegionThreshold int64 = 20
)

// StoreLimitMode indicates the strategy to set store limit
type Mode int

// There are two modes supported now, "auto" indicates the value
// is set by PD itself. "manual" means it is set by the user.
// An auto set value can be overwrite by a manual set value.
const (
	Auto Mode = iota
	Manual
)

type Type int

const (
	RegionAdd Type = iota
	RegionRemove
)

var TypeValue = map[string]Type{
	"RegionAdd":    RegionAdd,
	"RegionRemove": RegionRemove,
}

// String returns the representation of the StoreLimitMode
func (m Mode) String() string {
	switch m {
	case Auto:
		return "auto"
	case Manual:
		return "manual"
	}
	// default to be auto
	return "auto"
}

// String returns the representation of the StoreLimitType
func (t Type) String() string {
	for n, v :=range TypeValue {
		if v == t {
			return n
		}
	}
	return ""
}

// StoreLimit limits the operators of a store
type StoreLimit struct {
	bucket *ratelimit.Bucket
	mode   Mode
}

// NewStoreLimit returns a StoreLimit object
func NewStoreLimit(rate float64, mode Mode) *StoreLimit {
	capacity := RegionInfluence
	if rate > 1 {
		capacity = int64(rate * float64(RegionInfluence))
	}
	rate *= float64(RegionInfluence)
	return &StoreLimit{
		bucket: ratelimit.NewBucketWithRate(rate, capacity),
		mode:   mode,
	}
}

// Available returns the number of available tokens
func (l *StoreLimit) Available() int64 {
	return l.bucket.Available()
}

// Rate returns the fill rate of the bucket, in tokens per second.
func (l *StoreLimit) Rate() float64 {
	return l.bucket.Rate() / float64(RegionInfluence)
}

// Take takes count tokens from the bucket without blocking.
func (l *StoreLimit) Take(count int64) time.Duration {
	return l.bucket.Take(count)
}

// Mode returns the store limit mode
func (l *StoreLimit) Mode() Mode {
	return l.mode
}
