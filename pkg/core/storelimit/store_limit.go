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
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/ratelimit"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	// SmallRegionThreshold is used to represent a region which can be regarded as a small region once the size is small than it.
	SmallRegionThreshold int64 = 20
	// Unlimited is used to control the store limit. Here uses a big enough number to represent unlimited.
	Unlimited = float64(100000000)
	// influence is the influence of a normal region.
	influence = 1000
	// smallInfluence is the influence of a small region.
	smallInfluence = 200
)

// RegionInfluence represents the influence of an operator step, which is used by store limit.
var RegionInfluence = []int64{
	AddPeer:      influence,
	RemovePeer:   influence,
	SendSnapshot: influence,
}

// SmallRegionInfluence represents the influence of an operator step
// when the region size is smaller than smallRegionThreshold, which is used by store limit.
var SmallRegionInfluence = []int64{
	AddPeer:    smallInfluence,
	RemovePeer: smallInfluence,
}

// TypeNameValue indicates the name of store limit type and the enum value
var TypeNameValue = map[string]Type{
	"add-peer":      AddPeer,
	"remove-peer":   RemovePeer,
	"send-snapshot": SendSnapshot,
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

// Ack does nothing.
func (*StoreRateLimit) Ack(_ int64, _ Type) {}

// Version returns v1
func (*StoreRateLimit) Version() string {
	return VersionV1
}

// Feedback does nothing.
func (*StoreRateLimit) Feedback(_ float64) {}

// Available returns the number of available tokens.
// notice that the priority level is not used.
func (l *StoreRateLimit) Available(cost int64, typ Type, _ constant.PriorityLevel) bool {
	if typ == SendSnapshot {
		return true
	}
	return l.limits[typ].Available(cost)
}

// Rate returns the capacity of the store limit.
func (l *StoreRateLimit) Rate(typ Type) float64 {
	if l.limits[typ] == nil {
		return 0.0
	}
	return l.limits[typ].GetRatePerSec()
}

// Take takes count tokens from the bucket without blocking.
// notice that the priority level is not used.
func (l *StoreRateLimit) Take(cost int64, typ Type, _ constant.PriorityLevel) bool {
	if typ == SendSnapshot {
		return true
	}
	return l.limits[typ].Take(cost)
}

// Reset resets the rate limit.
func (l *StoreRateLimit) Reset(rate float64, typ Type) {
	if typ == SendSnapshot {
		return
	}
	l.limits[typ].Reset(rate)
}

// limit the operators of a store
type limit struct {
	limiter         *ratelimit.RateLimiter
	ratePerSecMutex syncutil.RWMutex
	ratePerSec      float64
}

// Reset resets the rate limit.
func (l *limit) Reset(ratePerSec float64) {
	l.ratePerSecMutex.Lock()
	defer l.ratePerSecMutex.Unlock()
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
	l.ratePerSecMutex.RLock()
	defer l.ratePerSecMutex.RUnlock()
	if l.ratePerSec == 0 {
		return true
	}
	// Unlimited = 1e8, so can convert int64 to int
	return l.limiter.Available(int(n))
}

// Take takes count tokens from the bucket without blocking.
func (l *limit) Take(count int64) bool {
	l.ratePerSecMutex.RLock()
	defer l.ratePerSecMutex.RUnlock()
	if l.ratePerSec == 0 {
		return true
	}
	return l.limiter.AllowN(int(count))
}

// GetRatePerSec returns the rate per second.
func (l *limit) GetRatePerSec() float64 {
	l.ratePerSecMutex.RLock()
	defer l.ratePerSecMutex.RUnlock()
	return l.ratePerSec
}
