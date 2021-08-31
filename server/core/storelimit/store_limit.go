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
	"fmt"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	// SmallRegionThreshold is used to represent a region which can be regarded as a small region once the size is small than it.
	SmallRegionThreshold int64 = 20
	// Unlimited is used to control the store limit. Here uses a big enough number to represent unlimited.
	Unlimited = float64(100000000)
)

// RegionInfluence represents the influence of a operator step, which is used by store limit.
var RegionInfluence = map[Type]int{
	AddPeer:    1000,
	RemovePeer: 1000,
}

// SmallRegionInfluence represents the influence of a operator step
// when the region size is smaller than smallRegionThreshold, which is used by store limit.
var SmallRegionInfluence = map[Type]int{
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
	rate            *rate.Limiter
	regionInfluence int
	ratePerSec      float64
	mu              sync.RWMutex
	releaseTime     time.Time
}

// NewStoreLimit returns a StoreLimit object
func NewStoreLimit(ratePerSec float64, regionInfluence int) *StoreLimit {
	capacity := regionInfluence
	originRate := ratePerSec
	// unlimited
	if originRate >= Unlimited {
		capacity = int(Unlimited)
	} else if ratePerSec > 1 {
		capacity = int(ratePerSec * float64(regionInfluence))
		ratePerSec *= float64(regionInfluence)
	} else {
		ratePerSec *= float64(regionInfluence)
	}

	return &StoreLimit{
		rate:            rate.NewLimiter(rate.Limit(ratePerSec), int(capacity)),
		regionInfluence: regionInfluence,
		ratePerSec:      originRate,
	}
}

// Available whether n events may happen at time now.
// Use this method if you intend to drop / skip events that exceed the rate limit.
// If true, it will takes n tokens.
func (l *StoreLimit) Available(n int) bool {
	now := time.Now()
	r := l.rate.ReserveN(now, n)

	if !r.OK() {
		l.mu.Lock()
		l.releaseTime = now.Add(r.Delay())
		l.mu.Unlock()
		return false
	}
	return true
}

func (l *StoreLimit) IsAvailable(n int) bool {
	l.mu.RLock()
	if time.Now().Before(l.releaseTime) {
		fmt.Sprintln("release")
		return false
	}
	l.mu.RUnlock()
	now := time.Now()
	r := l.rate.ReserveN(now, n)
	fmt.Sprintln("release 1: ", r.OK())
	if !r.OK() {
		l.mu.Lock()
		l.releaseTime = now.Add(r.Delay())
		l.mu.Unlock()
		return false
	}
	return false
}

// Rate returns the fill rate of the bucket, in tokens per second.
func (l *StoreLimit) Rate() float64 {
	return l.ratePerSec
}
