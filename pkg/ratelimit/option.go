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

package ratelimit

import "golang.org/x/time/rate"

// Option is used to create a limiter with the optional settings.
// these setting is used to add a kind of limiter for a service
type Option func(string, *Limiter)

// AddLabelBlockList adds a label into blockList
func AddLabelBlockList() Option {
	return func(label string, l *Limiter) {
		l.labelBlockList[label] = struct{}{}
	}
}

// UpdateConcurrencyLimiter creates a concurrency limiter for a given label if it doesn't exist.
func UpdateConcurrencyLimiter(limit uint64) Option {
	return func(label string, l *Limiter) {
		if _, block := l.labelBlockList[label]; block {
			return
		}
		if limiter, exist := l.concurrencyLimiter.LoadOrStore(label, newConcurrencyLimiter(limit)); exist {
			limiter.(*concurrencyLimiter).setLimit(limit)
		}
	}
}

// DeleteConcurrencyLimiter deletes concurrency limiter of given label
func DeleteConcurrencyLimiter() Option {
	return func(label string, l *Limiter) {
		l.concurrencyLimiter.Delete(label)
	}
}

// UpdateQPSLimiter creates a QPS limiter for a given label if it doesn't exist.
func UpdateQPSLimiter(limit rate.Limit, burst int) Option {
	return func(label string, l *Limiter) {
		if _, block := l.labelBlockList[label]; block {
			return
		}
		if limiter, exist := l.qpsLimiter.LoadOrStore(label, NewRateLimiter(float64(limit), burst)); exist {
			limiter.(*RateLimiter).SetLimit(limit)
			limiter.(*RateLimiter).SetBurst(burst)
		}
	}
}

// DeleteQPSLimiter deletes QPS limiter of given label
func DeleteQPSLimiter() Option {
	return func(label string, l *Limiter) {
		l.qpsLimiter.Delete(label)
	}
}
