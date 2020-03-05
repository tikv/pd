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

package schedule

import (
	"time"

	"github.com/juju/ratelimit"
	"github.com/pingcap/pd/v4/server/schedule/operator"
)

// StoreLimitMode indicates the strategy to set store limit
type StoreLimitMode int

// There are two modes supported now, "auto" indicates the value
// is set by PD itself. "manual" means it is set by the user.
// An auto set value can be overwrite by a manual set value.
const (
	StoreLimitAuto StoreLimitMode = iota
	StoreLimitManual
)

// String returns the representation of the StoreLimitMode
func (m StoreLimitMode) String() string {
	switch m {
	case StoreLimitAuto:
		return "auto"
	case StoreLimitManual:
		return "manual"
	}
	// default to be auto
	return "auto"
}

// StoreLimit limits the operators of a store
type StoreLimit struct {
	bucket *ratelimit.Bucket
	mode   StoreLimitMode
}

// NewStoreLimit returns a StoreLimit object
func NewStoreLimit(rate float64, mode StoreLimitMode) *StoreLimit {
	capacity := operator.RegionInfluence
	if rate > 1 {
		capacity = int64(rate * float64(operator.RegionInfluence))
	}
	rate *= float64(operator.RegionInfluence)
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
	return l.bucket.Rate() / float64(operator.RegionInfluence)
}

// Take takes count tokens from the bucket without blocking.
func (l *StoreLimit) Take(count int64) time.Duration {
	return l.bucket.Take(count)
}

// Mode returns the store limit mode
func (l *StoreLimit) Mode() StoreLimitMode {
	return l.mode
}
