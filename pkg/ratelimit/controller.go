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
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"
)

var emptyFunc = func() {}

// Controller is a controller which holds multiple limiters to manage the request rate of different objects.
type Controller struct {
	ctx      context.Context
	apiType  string
	limiters sync.Map
	// the label which is in labelAllowList won't be limited, and only inited by hard code.
	labelAllowList map[string]struct{}

	counter *prometheus.CounterVec
	gauge   *prometheus.GaugeVec
}

// NewController returns a global limiter which can be updated in the later.
func NewController(ctx context.Context, typ string, baseCounter *prometheus.CounterVec, baseGauge *prometheus.GaugeVec) *Controller {
	l := &Controller{
		ctx:            ctx,
		apiType:        typ,
		labelAllowList: make(map[string]struct{}),
		counter:        baseCounter,
		gauge:          baseGauge,
	}
	go l.collectMetrics()
	return l
}

func (l *Controller) collectMetrics() {
	tricker := time.NewTicker(time.Second)
	defer tricker.Stop()
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-tricker.C:
			if l.gauge != nil {
				l.limiters.Range(func(key, value any) bool {
					limiter := value.(*limiter)
					label := key.(string)
					// Due to not in hot path, no need to save sub Gauge.
					if con := limiter.getConcurrencyLimiter(); con != nil {
						l.gauge.WithLabelValues(l.apiType, label, "concurrency").Set(float64(con.getCurrent()))
						l.gauge.WithLabelValues(l.apiType, label, "concurrency-limit").Set(float64(con.getLimit()))
					}
					if bbr := limiter.getBBR(); bbr != nil {
						if bbr.bbrStatus.getMinRT() != infRT {
							l.gauge.WithLabelValues(l.apiType, label, "bdp").Set(float64(bbr.bbrStatus.getMaxInFlight()))
						} else {
							l.gauge.WithLabelValues(l.apiType, label, "bdp").Set(float64(0))
						}
					}
					return true
				})
			}
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

// GetBBRStatus returns the status of a given label's BBR.
func (l *Controller) GetBBRStatus(label string) (enable bool, limit int64) {
	if limit, exist := l.limiters.Load(label); exist {
		return limit.(*limiter).getBBRStatus()
	}
	return false, 0
}

// IsInAllowList returns whether this label is in allow list.
// If returns true, the given label won't be limited
func (l *Controller) IsInAllowList(label string) bool {
	_, allow := l.labelAllowList[label]
	return allow
}
