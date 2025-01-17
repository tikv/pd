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

// Option is used to create a limiter with the optional settings.
// these setting is used to add a kind of limiter for a service
type Option func(string, *Limiter) UpdateStatus

// AddLabelAllowList adds a label into allow list.
// It means the given label will not be limited
func AddLabelAllowList() Option {
<<<<<<< HEAD
	return func(label string, l *Limiter) UpdateStatus {
		l.labelAllowList[label] = struct{}{}
		return 0
=======
	return func(label string, c *Controller) UpdateStatus {
		c.labelAllowList[label] = struct{}{}
		return InAllowList
>>>>>>> 604b0d6d4 (pkg: avoid make channel every time (#9009))
	}
}

func updateConcurrencyConfig(l *Limiter, label string, limit uint64) UpdateStatus {
	oldConcurrencyLimit, _ := l.GetConcurrencyLimiterStatus(label)
	if oldConcurrencyLimit == limit {
		return ConcurrencyNoChange
	}
	if limit < 1 {
		l.ConcurrencyUnlimit(label)
		return ConcurrencyDeleted
	}
	if limiter, exist := l.concurrencyLimiter.LoadOrStore(label, newConcurrencyLimiter(limit)); exist {
		limiter.(*concurrencyLimiter).setLimit(limit)
	}
	return ConcurrencyChanged
}

func updateQPSConfig(l *Limiter, label string, limit float64, burst int) UpdateStatus {
	oldQPSLimit, oldBurst := l.GetQPSLimiterStatus(label)

	if (float64(oldQPSLimit)-limit < eps && float64(oldQPSLimit)-limit > -eps) && oldBurst == burst {
		return QPSNoChange
	}
	if limit <= eps || burst < 1 {
		l.QPSUnlimit(label)
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
<<<<<<< HEAD
	return func(label string, l *Limiter) UpdateStatus {
		if _, allow := l.labelAllowList[label]; allow {
			return InAllowList
		}
		return updateConcurrencyConfig(l, label, limit)
=======
	return func(label string, c *Controller) UpdateStatus {
		if _, allow := c.labelAllowList[label]; allow {
			return InAllowList
		}
		lim, loaded := c.limiters.Load(label)
		if !loaded {
			lim, _ = c.limiters.LoadOrStore(label, newLimiter())
		}
		return lim.(*limiter).updateConcurrencyConfig(limit)
>>>>>>> 604b0d6d4 (pkg: avoid make channel every time (#9009))
	}
}

// UpdateQPSLimiter creates a QPS limiter for a given label if it doesn't exist.
func UpdateQPSLimiter(limit float64, burst int) Option {
<<<<<<< HEAD
	return func(label string, l *Limiter) UpdateStatus {
		if _, allow := l.labelAllowList[label]; allow {
			return InAllowList
		}
		return updateQPSConfig(l, label, limit, burst)
=======
	return func(label string, c *Controller) UpdateStatus {
		if _, allow := c.labelAllowList[label]; allow {
			return InAllowList
		}
		lim, loaded := c.limiters.Load(label)
		if !loaded {
			lim, _ = c.limiters.LoadOrStore(label, newLimiter())
		}
		return lim.(*limiter).updateQPSConfig(limit, burst)
>>>>>>> 604b0d6d4 (pkg: avoid make channel every time (#9009))
	}
}

// UpdateDimensionConfig creates QPS limiter and concurrency limiter for a given label by config if it doesn't exist.
func UpdateDimensionConfig(cfg *DimensionConfig) Option {
<<<<<<< HEAD
	return func(label string, l *Limiter) UpdateStatus {
		if _, allow := l.labelAllowList[label]; allow {
			return InAllowList
		}
		status := updateQPSConfig(l, label, cfg.QPS, cfg.QPSBurst)
		status |= updateConcurrencyConfig(l, label, cfg.ConcurrencyLimit)
		return status
=======
	return func(label string, c *Controller) UpdateStatus {
		if _, allow := c.labelAllowList[label]; allow {
			return InAllowList
		}
		lim, loaded := c.limiters.Load(label)
		if !loaded {
			lim, _ = c.limiters.LoadOrStore(label, newLimiter())
		}
		return lim.(*limiter).updateDimensionConfig(cfg)
	}
}

// InitLimiter creates empty concurrency limiter for a given label by config if it doesn't exist.
func InitLimiter() Option {
	return func(label string, c *Controller) UpdateStatus {
		if _, allow := c.labelAllowList[label]; allow {
			return InAllowList
		}
		_, loaded := c.limiters.Load(label)
		if !loaded {
			c.limiters.LoadOrStore(label, newLimiter())
		}
		return LimiterNotChanged
>>>>>>> 604b0d6d4 (pkg: avoid make channel every time (#9009))
	}
}
