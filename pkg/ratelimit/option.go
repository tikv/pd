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

// UpdateStatus is flags for updating limiter config.
type UpdateStatus uint32

// Flags for limiter.
const (
	eps float64 = 1e-8

	LimiterNotChanged UpdateStatus = 1 << iota
	// LimiterUpdated shows that limiter's config is updated.
	LimiterUpdated
	// LimiterDeleted shows that limiter's config is deleted.
	LimiterDeleted
	// InAllowList shows that limiter's config isn't changed because it is in in allow list.
	InAllowList
)

// Option is used to create a limiter with the optional settings.
// these setting is used to add a kind of limiter for a service
type Option func(string, *Controller) UpdateStatus

// AddLabelAllowList adds a label into allow list.
// It means the given label will not be limited
func AddLabelAllowList() Option {
	return func(label string, c *Controller) UpdateStatus {
		c.labelAllowList[label] = struct{}{}
		return InAllowList
	}
}

// UpdateConcurrencyLimiter creates a concurrency limiter for a given label if it doesn't exist.
func UpdateConcurrencyLimiter(limit uint64) Option {
	return func(label string, c *Controller) UpdateStatus {
		if _, allow := c.labelAllowList[label]; allow {
			return InAllowList
		}
		lim, loaded := c.limiters.Load(label)
		if !loaded {
			lim, _ = c.limiters.LoadOrStore(label, newLimiter())
		}
		return lim.(*limiter).updateConcurrencyConfig(limit)
	}
}

// UpdateQPSLimiter creates a QPS limiter for a given label if it doesn't exist.
func UpdateQPSLimiter(limit float64, burst int) Option {
	return func(label string, c *Controller) UpdateStatus {
		if _, allow := c.labelAllowList[label]; allow {
			return InAllowList
		}
		lim, loaded := c.limiters.Load(label)
		if !loaded {
			lim, _ = c.limiters.LoadOrStore(label, newLimiter())
		}
		return lim.(*limiter).updateQPSConfig(limit, burst)
	}
}

// UpdateDimensionConfig creates QPS limiter and concurrency limiter for a given label by config if it doesn't exist.
func UpdateDimensionConfig(cfg *DimensionConfig) Option {
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
	}
}
