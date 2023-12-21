// Copyright 2023 TiKV Project Authors.
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
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"
)

const limiterMetricsInterval = time.Second * 15

var emptyFunc = func() {}

// Controller is a controller which holds multiple limiters to manage the request rate of different objects.
type Controller struct {
	limiters sync.Map
	// the label which is in labelAllowList won't be limited, and only inited by hard code.
	labelAllowList map[string]struct{}

	ctx              context.Context
	cancel           context.CancelFunc
	apiType          string
	concurrencyGauge *prometheus.GaugeVec
}

// NewController returns a global limiter which can be updated in the later.
func NewController(ctx context.Context, typ string, concurrencyGauge *prometheus.GaugeVec) *Controller {
	ctx, cancel := context.WithCancel(ctx)
	l := &Controller{
		ctx:              ctx,
		cancel:           cancel,
		labelAllowList:   make(map[string]struct{}),
		apiType:          typ,
		concurrencyGauge: concurrencyGauge,
	}
	if concurrencyGauge != nil {
		go l.collectMetrics()
	}
	return l
}

// Close closes the Controller.
func (l *Controller) Close() {
	l.cancel()
}

func (l *Controller) collectMetrics() {
	tricker := time.NewTicker(limiterMetricsInterval)
	defer tricker.Stop()
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-tricker.C:
			l.limiters.Range(func(key, value any) bool {
				limiter := value.(*limiter)
				label := key.(string)
				// Due to not in hot path, no need to save sub Gauge.
				if con := limiter.getConcurrencyLimiter(); con != nil {
					l.concurrencyGauge.WithLabelValues(l.apiType, label).Set(float64(con.getMaxConcurrency()))
				}
				return true
			})
		}
	}
}

// Allow is used to check whether it has enough token.
func (l *Controller) Allow(label string) (DoneFunc, error) {
	var ok bool
	lim, ok := l.limiters.Load(label)
	if ok {
		return lim.(*limiter).allow()
	}
	return emptyFunc, nil
}

// Update is used to update Ratelimiter with Options
func (l *Controller) Update(label string, opts ...Option) UpdateStatus {
	var status UpdateStatus
	for _, opt := range opts {
		status |= opt(label, l)
	}
	return status
}

// GetQPSLimiterStatus returns the status of a given label's QPS limiter.
func (l *Controller) GetQPSLimiterStatus(label string) (limit rate.Limit, burst int) {
	if limit, exist := l.limiters.Load(label); exist {
		return limit.(*limiter).getQPSLimiterStatus()
	}
	return 0, 0
}

// GetConcurrencyLimiterStatus returns the status of a given label's concurrency limiter.
func (l *Controller) GetConcurrencyLimiterStatus(label string) (limit uint64, current uint64) {
	if limit, exist := l.limiters.Load(label); exist {
		return limit.(*limiter).getConcurrencyLimiterStatus()
	}
	return 0, 0
}

// IsInAllowList returns whether this label is in allow list.
// If returns true, the given label won't be limited
func (l *Controller) IsInAllowList(label string) bool {
	_, allow := l.labelAllowList[label]
	return allow
}
