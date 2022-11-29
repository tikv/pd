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
	"github.com/tikv/pd/pkg/ratelimit"
)

const (
	// SmallRegionThreshold is used to represent a region which can be regarded as a small region once the size is small than it.
	SmallRegionThreshold int64 = 20
	// Unlimited is used to control the store limit. Here uses a big enough number to represent unlimited.
	Unlimited = float64(100000000)
)

const influence = 1000

// RegionInfluence represents the influence of a operator step, which is used by store limit.
var RegionInfluence = []int64{
	AddPeer:    influence,
	RemovePeer: influence,
}

var _ StoreLimit = &StoreRateLimit{}

// StoreRateLimit is a rate limiter for store.
type StoreRateLimit struct {
	limits []*limit
}

// NewStoreRateLimit creates a StoreRateLimit.
func NewStoreRateLimit(ratePerSec float64) StoreLimit {
	limits := make([]*limit, storeLimitTypeLen)
	for i := 0; i < len(limits); i++ {
		l := &limit{}
		l.Reset(ratePerSec)
		limits[i] = l
	}
	return &StoreRateLimit{
		limits: limits,
	}
}

// Available returns the number of available tokens
func (l *StoreRateLimit) Available(cost int64, typ Type) bool {
	return l.limits[typ].Available(cost)
}

// Take takes count tokens from the bucket without blocking.
func (l *StoreRateLimit) Take(cost int64, typ Type) bool {
	return l.limits[typ].Take(cost)
}

// Reset resets the rate limit.
func (l *StoreRateLimit) Reset(rate float64, typ Type) {
	l.limits[typ].Reset(rate)
}

// SmallRegionInfluence represents the influence of a operator step
// when the region size is smaller than smallRegionThreshold, which is used by store limit.
var SmallRegionInfluence = []int64{
	AddPeer:    200,
	RemovePeer: 200,
}

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

// limit  the operators of a store
type limit struct {
	limiter    *ratelimit.RateLimiter
	ratePerSec float64
}

func (l *limit) Reset(ratePerSec float64) {
	if l.ratePerSec == ratePerSec {
		return
	}
	capacity := int64(influence)
	rate := ratePerSec
	// unlimited
	if rate >= Unlimited {
		capacity = int64(Unlimited)
	} else if ratePerSec > 1 {
		capacity = int64(ratePerSec * float64(influence))
		ratePerSec *= float64(influence)
	} else {
		ratePerSec *= float64(influence)
	}
	l.limiter = ratelimit.NewRateLimiter(ratePerSec, int(capacity))
	l.ratePerSec = rate
}

// Available returns the number of available tokens
// It returns true if the rate per second is zero.
func (l *limit) Available(n int64) bool {
	if l == nil || l.ratePerSec == 0 {
		return true
	}
	// Unlimited = 1e8, so can convert int64 to int
	return l.limiter.Available(int(n))
}

// Take takes count tokens from the bucket without blocking.
func (l *limit) Take(count int64) bool {
	if l == nil || l.ratePerSec == 0 {
		return true
	}
	return l.limiter.AllowN(int(count))
}
