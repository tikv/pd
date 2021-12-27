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

package server

import (
	"sync"

	"golang.org/x/time/rate"
)

// serviceRateLimiter is used to rimit service request rate
type serviceRateLimiter struct {
	mu sync.RWMutex

	enableQPSLimit bool

	totalQPSRateLimiter *rate.Limiter

	enableComponentQPSLimit bool
	componentQPSRateLimiter map[string]*rate.Limiter
}

// QPSAllow firstly check component token bucket and then check total token bucket
// if component rate limiter doesn't allow, it won't ask total limiter
func (rl *serviceRateLimiter) QPSAllow(componentName string) bool {
	if !rl.enableQPSLimit {
		return true
	}
	isComponentQPSLimit := true
	if rl.enableComponentQPSLimit {
		componentRateLimiter, ok := rl.componentQPSRateLimiter[componentName]
		// The current strategy is to ignore the component limit if it is not found
		if ok {
			isComponentQPSLimit = componentRateLimiter.Allow()
		}
	}
	if !isComponentQPSLimit {
		return isComponentQPSLimit
	}
	isTotalQPSLimit := rl.totalQPSRateLimiter.Allow()
	return isTotalQPSLimit
}

// Allow currentlt only supports QPS rate limit
func (rl *serviceRateLimiter) Allow(componentName string) bool {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.QPSAllow(componentName)
}
