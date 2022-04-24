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

// AddLabelAllowList adds a label into allow list.
// It means the given label will not be limited
func AddLabelAllowList() Option {
	return func(label string, l *Limiter) {
		l.labelAllowList[label] = struct{}{}
	}
}

func updateConcurrencyConfig(label string, l *Limiter, limit uint64) bool {
	l.configMux.Lock()
	defer l.configMux.Unlock()
	if limit < 1 {
		return false
	}
	cfg, ok := l.labelConfig[label]
	if !ok {
		cfg = DimensionConfig{ConcurrencyLimit: limit}
		l.labelConfig[label] = cfg
		return true
	}
	if cfg.ConcurrencyLimit == limit {
		return false
	}
	cfg.ConcurrencyLimit = limit
	return true
}

func updateQPSConfig(label string, l *Limiter, limit float64, burst int) bool {
	l.configMux.Lock()
	defer l.configMux.Unlock()
	if limit <= 0 && burst < 1 {
		return false
	}
	cfg, ok := l.labelConfig[label]
	if !ok {
		cfg = DimensionConfig{QPS: limit, QPSBrust: burst}
		l.labelConfig[label] = cfg
		return true
	}
	if cfg.QPS == limit && cfg.QPSBrust == burst {
		return false
	}
	cfg.QPS = limit
	cfg.QPSBrust = burst
	return true
}

// UpdateConcurrencyLimiter creates a concurrency limiter for a given label if it doesn't exist.
func UpdateConcurrencyLimiter(limit uint64) Option {
	return func(label string, l *Limiter) {
		if _, allow := l.labelAllowList[label]; allow {
			return
		}
		if !updateConcurrencyConfig(label, l, limit) {
			return
		}
		if limiter, exist := l.concurrencyLimiter.LoadOrStore(label, newConcurrencyLimiter(limit)); exist {
			limiter.(*concurrencyLimiter).setLimit(limit)
		}
	}
}

// UpdateQPSLimiter creates a QPS limiter for a given label if it doesn't exist.
func UpdateQPSLimiter(limit float64, burst int) Option {
	return func(label string, l *Limiter) {
		if _, allow := l.labelAllowList[label]; allow {
			return
		}
		if !updateQPSConfig(label, l, limit, burst) {
			return
		}
		if limiter, exist := l.qpsLimiter.LoadOrStore(label, NewRateLimiter(limit, burst)); exist {
			limiter.(*RateLimiter).SetLimit(rate.Limit(limit))
			limiter.(*RateLimiter).SetBurst(burst)
		}
	}
}

// UpdateDimensionConfig creates QPS limiter and concurrency limiter for a given label by config if it doesn't exist.
func UpdateDimensionConfig(cfg DimensionConfig) Option {
	return func(label string, l *Limiter) {
		if _, allow := l.labelAllowList[label]; allow {
			return
		}
		if updateQPSConfig(label, l, cfg.QPS, cfg.QPSBrust) {
			if limiter, exist := l.qpsLimiter.LoadOrStore(label, NewRateLimiter(cfg.QPS, cfg.QPSBrust)); exist {
				limiter.(*RateLimiter).SetLimit(rate.Limit(cfg.QPS))
				limiter.(*RateLimiter).SetBurst(cfg.QPSBrust)
			}
		}
		if updateConcurrencyConfig(label, l, cfg.ConcurrencyLimit) {
			if limiter, exist := l.concurrencyLimiter.LoadOrStore(label, newConcurrencyLimiter(cfg.ConcurrencyLimit)); exist {
				limiter.(*concurrencyLimiter).setLimit(cfg.ConcurrencyLimit)
			}
		}
	}
}
