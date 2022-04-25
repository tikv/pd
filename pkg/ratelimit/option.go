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

// UpdateStatus is flags for updating limiter config.
type UpdateStatus uint32

// Flags for limiter.
const (
	// QPSNoChange shows that limiter's config isn't changed.
	QPSNoChange UpdateStatus = 1 << iota
	// QPSChanged shows that limiter's config is changed and not deleted.
	QPSChanged
	// QPSDeleted shows that limiter's config is deleted.
	QPSDeleted
	// ConcurrencyNoChange shows that limiter's config isn't changed.
	ConcurrencyNoChange
	// ConcurrencyChanged shows that limiter's config is changed and not deleted.
	ConcurrencyChanged
	// ConcurrencyDeleted shows that limiter's config is deleted.
	ConcurrencyDeleted
	// InAllowList shows that limiter's config isn't changed because it is in in allow list.
	InAllowList
	// Ignore shows that the status can be ignored when initiated
	Ignore
)

// Option is used to create a limiter with the optional settings.
// these setting is used to add a kind of limiter for a service
type Option func(string, *Limiter) UpdateStatus

// AddLabelAllowList adds a label into allow list.
// It means the given label will not be limited
func AddLabelAllowList() Option {
	return func(label string, l *Limiter) UpdateStatus {
		l.labelAllowList[label] = struct{}{}
		return Ignore
	}
}

func updateConcurrencyConfig(l *Limiter, label string, limit uint64) UpdateStatus {
	l.configMux.Lock()
	defer l.configMux.Unlock()

	cfg := l.labelConfig[label]
	if cfg.ConcurrencyLimit == limit {
		return ConcurrencyNoChange
	}
	cfg.ConcurrencyLimit = limit
	l.labelConfig[label] = cfg
	if limit < 1 {
		l.deleteConcurrencyLimiter(label)
		return ConcurrencyDeleted
	}
	if limiter, exist := l.concurrencyLimiter.LoadOrStore(label, newConcurrencyLimiter(limit)); exist {
		limiter.(*concurrencyLimiter).setLimit(limit)
	}
	return ConcurrencyChanged
}

func updateQPSConfig(l *Limiter, label string, limit float64, burst int) UpdateStatus {
	l.configMux.Lock()
	defer l.configMux.Unlock()

	cfg := l.labelConfig[label]
	if cfg.QPS == limit && cfg.QPSBurst == burst {
		return QPSNoChange
	}
	cfg.QPS = limit
	cfg.QPSBurst = burst
	l.labelConfig[label] = cfg
	if limit <= 0 || burst < 1 {
		l.deleteQPSLimiter(label)
		return QPSDeleted
	}
	if limiter, exist := l.qpsLimiter.LoadOrStore(label, NewRateLimiter(limit, burst)); exist {
		limiter.(*RateLimiter).SetLimit(rate.Limit(limit))
		limiter.(*RateLimiter).SetBurst(burst)
	}
	return QPSChanged
}

// UpdateConcurrencyLimiter creates a concurrency limiter for a given label if it doesn't exist.
func UpdateConcurrencyLimiter(limit uint64) Option {
	return func(label string, l *Limiter) UpdateStatus {
		if _, allow := l.labelAllowList[label]; allow {
			return InAllowList
		}
		return updateConcurrencyConfig(l, label, limit)
	}
}

// UpdateQPSLimiter creates a QPS limiter for a given label if it doesn't exist.
func UpdateQPSLimiter(limit float64, burst int) Option {
	return func(label string, l *Limiter) UpdateStatus {
		if _, allow := l.labelAllowList[label]; allow {
			return InAllowList
		}
		return updateQPSConfig(l, label, limit, burst)
	}
}

// UpdateDimensionConfig creates QPS limiter and concurrency limiter for a given label by config if it doesn't exist.
func UpdateDimensionConfig(cfg DimensionConfig) Option {
	return func(label string, l *Limiter) UpdateStatus {
		if _, allow := l.labelAllowList[label]; allow {
			return InAllowList
		}
		status := updateQPSConfig(l, label, cfg.QPS, cfg.QPSBurst)
		status |= updateConcurrencyConfig(l, label, cfg.ConcurrencyLimit)
		return status
	}
}
