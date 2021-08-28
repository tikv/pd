// Copyright 2019 TiKV Project Authors.
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
	AddPeer:    1000,
	RemovePeer: 1000,
}

// SmallRegionInfluence represents the influence of a operator step
// when the region size is smaller than smallRegionThreshold, which is used by store limit.
var SmallRegionInfluence = map[Type]int64{
	AddPeer:    200,
	RemovePeer: 200,
}

// Type indicates the type of store limit
type Type int

const (
	// AddPeer indicates the type of store limit that limits the adding peer rate
	AddPeer Type = iota
	// RemovePeer indicates the type of store limit that limits the removing peer rate
	RemovePeer
)

// TypeNameValue indicates the name of store limit type and the enum value
var TypeNameValue = map[string]Type{
	"add-peer":    AddPeer,
	"remove-peer": RemovePeer,
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
	regionInfluence int64
	ratePerSec      float64
}

// NewStoreLimit returns a StoreLimit object
func NewStoreLimit(ratePerSec float64, regionInfluence int64) *StoreLimit {
	capacity := regionInfluence
	rate := ratePerSec
	// unlimited
	if rate >= Unlimited {
		capacity = int64(Unlimited)
	} else if ratePerSec > 1 {
		capacity = int64(ratePerSec * float64(regionInfluence))
		ratePerSec *= float64(regionInfluence)
	} else {
		ratePerSec *= float64(regionInfluence)
	}
	return &StoreLimit{
		bucket:          ratelimit.NewBucketWithRate(ratePerSec, capacity),
		regionInfluence: regionInfluence,
		ratePerSec:      rate,
	}
}

// Available returns the number of available tokens
func (l *StoreLimit) Available() int64 {
	return l.bucket.Available()
}

// Rate returns the fill rate of the bucket, in tokens per second.
func (l *StoreLimit) Rate() float64 {
	return l.ratePerSec
}

// Take takes count tokens from the bucket without blocking.
func (l *StoreLimit) Take(count int64) time.Duration {
	return l.bucket.Take(count)
}
