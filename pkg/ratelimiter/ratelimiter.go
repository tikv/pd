// Copyright 2022 TiKV Project Authors.
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

package ratelimiter

import "github.com/pingcap/failpoint"

// RateLimiter is a controller for the request rate for different services.
type RateLimiter struct {
}

// NewRateLimiter returns RateLimiter
func NewRateLimiter() *RateLimiter {
	return &RateLimiter{}
}

// Allow is used to check whether it has enough token.
func (l *RateLimiter) Allow(label string) bool {
	ret := true
	failpoint.Inject("dontAllowRatelimiter", func() {
		ret = false
	})
	return ret
}

// // Release is used to refill token. It may be not uesful for some limiters because they will refill automatically
func (l *RateLimiter) Release(label string) {
}

// Update is used to update Ratelimiter with Options
func (l *RateLimiter) Update(label string, opts ...Option) {
	for _, opt := range opts {
		opt(label, l)
	}
}
