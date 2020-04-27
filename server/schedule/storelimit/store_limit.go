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
	// SmallRegionThreshold is used to represent a region which can be regarded as a small region once the size is small than it.
	SmallRegionThreshold int64 = 20
	// Unlimited is used to control the store limit. Here uses a big enough number to represent unlimited.
	Unlimited = float64(100000000)
)

// RegionInfluence represents the influence of a operator step, which is used by store limit.
var RegionInfluence = map[Type]int64{
	RegionAdd:    1000,
	RegionRemove: 1000,
}

// SmallRegionInfluence represents the influence of a operator step
// when the region size is smaller than smallRegionThreshold, which is used by store limit.
var SmallRegionInfluence = map[Type]int64{
	RegionAdd:    200,
	RegionRemove: 200,
}

// Mode indicates the strategy to set store limit
type Mode int

// There are two modes supported now, "auto" indicates the value
// is set by PD itself. "manual" means it is set by the user.
// An auto set value can be overwrite by a manual set value.
const (
	Auto Mode = iota
	Manual
)

// Type indicates the type of store limit
type Type int

const (
	// RegionAdd indicates the type of store limit that limits the adding region rate
	RegionAdd Type = iota
	// RegionRemove indicates the type of store limit that limits the removing region rate
	RegionRemove
)

// TypeNameValue indicates the name of store limit type and the enum value
var TypeNameValue = map[string]Type{
	"region-add":    RegionAdd,
	"region-remove": RegionRemove,
}

// String returns the representation of the store limit mode
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

// String returns the representation of the Type
func (t Type) String() string {
	for n, v := range TypeNameValue {
		if v == t {
			return n
		}
	}
	return ""
}

// StoreLimit limits the operators of a store
type StoreLimit struct {
	bucket          *ratelimit.Bucket
	mode            Mode
	regionInfluence int64
}

// NewStoreLimit returns a StoreLimit object
func NewStoreLimit(rate float64, mode Mode, regionInfluence int64) *StoreLimit {
	capacity := regionInfluence
	// unlimited
	if rate >= Unlimited {
		capacity = int64(Unlimited)
	} else if rate > 1 {
		capacity = int64(rate * float64(regionInfluence))
		rate *= float64(regionInfluence)
	} else {
		rate *= float64(regionInfluence)
	}
	return &StoreLimit{
		bucket:          ratelimit.NewBucketWithRate(rate, capacity),
		mode:            mode,
		regionInfluence: regionInfluence,
	}
}

// Available returns the number of available tokens
func (l *StoreLimit) Available() int64 {
	return l.bucket.Available()
}

// Rate returns the fill rate of the bucket, in tokens per second.
func (l *StoreLimit) Rate() float64 {
	return l.bucket.Rate() / float64(l.regionInfluence)
}

// Take takes count tokens from the bucket without blocking.
func (l *StoreLimit) Take(count int64) time.Duration {
	return l.bucket.Take(count)
}

// Mode returns the store limit mode
func (l *StoreLimit) Mode() Mode {
	return l.mode
}
