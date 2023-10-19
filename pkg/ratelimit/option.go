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

import (
	"golang.org/x/time/rate"
)

// UpdateStatus is flags for updating limiter config.
type UpdateStatus uint32

// Flags for limiter.
const (
	eps float64 = 1e-8
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
)

func updateConcurrencyConfig(l *limiter, limit uint64) UpdateStatus {
	oldConcurrencyLimit, _ := l.getConcurrencyLimiterStatus()
	if oldConcurrencyLimit == limit {
		return ConcurrencyNoChange
	}
	if limit < 1 {
		l.deleteConcurrency()
		return ConcurrencyDeleted
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.concurrency != nil {
		l.concurrency.setLimit(limit)
	} else {
		l.concurrency = newConcurrencyLimiter(limit)
	}
	return ConcurrencyChanged
}

func updateQPSConfig(l *limiter, limit float64, burst int) UpdateStatus {
	oldQPSLimit, oldBurst := l.getQPSLimiterStatus()

	if (float64(oldQPSLimit)-limit < eps && float64(oldQPSLimit)-limit > -eps) && oldBurst == burst {
		return QPSNoChange
	}
	if limit <= eps || burst < 1 {
		l.deleteRateLimiter()
		return QPSDeleted
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.rate != nil {
		l.rate.SetLimit(rate.Limit(limit))
		l.rate.SetBurst(burst)
	} else {
		l.rate = NewRateLimiter(limit, burst)
	}
	return QPSChanged
}

// Option is used to create a limiter with the optional settings.
// these setting is used to add a kind of limiter for a service
type Option func(string, *MultiLimiter) UpdateStatus

// AddLabelAllowList adds a label into allow list.
// It means the given label will not be limited
func AddLabelAllowList() Option {
	return func(label string, l *MultiLimiter) UpdateStatus {
		l.labelAllowList[label] = struct{}{}
		return 0
	}
}

// UpdateConcurrencyLimiter creates a concurrency limiter for a given label if it doesn't exist.
func UpdateConcurrencyLimiter(limit uint64) Option {
	return func(label string, l *MultiLimiter) UpdateStatus {
		if _, allow := l.labelAllowList[label]; allow {
			return InAllowList
		}
		lim, _ := l.limiters.LoadOrStore(label, newLimiter())
		return updateConcurrencyConfig(lim.(*limiter), limit)
	}
}

// UpdateQPSLimiter creates a QPS limiter for a given label if it doesn't exist.
func UpdateQPSLimiter(limit float64, burst int) Option {
	return func(label string, l *MultiLimiter) UpdateStatus {
		if _, allow := l.labelAllowList[label]; allow {
			return InAllowList
		}
		lim, _ := l.limiters.LoadOrStore(label, newLimiter())
		return updateQPSConfig(lim.(*limiter), limit, burst)
	}
}

// UpdateDimensionConfig creates QPS limiter and concurrency limiter for a given label by config if it doesn't exist.
func UpdateDimensionConfig(cfg *DimensionConfig) Option {
	return func(label string, l *MultiLimiter) UpdateStatus {
		if _, allow := l.labelAllowList[label]; allow {
			return InAllowList
		}
		lim, _ := l.limiters.LoadOrStore(label, newLimiter())
		status := updateQPSConfig(lim.(*limiter), cfg.QPS, cfg.QPSBurst)
		status |= updateConcurrencyConfig(lim.(*limiter), cfg.ConcurrencyLimit)
		return status
	}
}
