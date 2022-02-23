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

import (
	"golang.org/x/time/rate"
)

// CreateOption is used to create a limiter with the optional settings.
// these setting is used to add a kind of limiter for a service
type CreateOption func(string, *Limiter)

// WithConcurrencyLimiter creates a concurrency limiter for a given path if it doesn't exist.
func WithConcurrencyLimiter(limit uint64) CreateOption {
	return func(label string, l *Limiter) {
		// Ignore the return value since we don't care about it.
		l.concurrencyLimiter.LoadOrStore(label, newConcurrencyLimiter(limit))
	}
}

// WithConcurrencyLimiter creates a QPS limiter for a given path if it doesn't exist.
func WithQPSLimiter(limit rate.Limit, burst int) CreateOption {
	return func(label string, l *Limiter) {
		// Ignore the return value since we don't care about it.
		l.qpsLimiter.LoadOrStore(label, rate.NewLimiter(limit, burst))
	}
}
