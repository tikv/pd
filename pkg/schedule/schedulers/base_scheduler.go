// Copyright 2017 TiKV Project Authors.
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

package schedulers

import (
	"fmt"
	"net/http"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

// options for interval of schedulers
const (
	MaxScheduleInterval     = time.Second * 5
	MinScheduleInterval     = time.Millisecond * 10
	MinSlowScheduleInterval = time.Second * 3

	ScheduleIntervalFactor = 1.3
)

type intervalGrowthType int

const (
	exponentialGrowth intervalGrowthType = iota
	linearGrowth
	zeroGrowth
)

// intervalGrow calculates the next interval of balance.
func intervalGrow(x time.Duration, maxInterval time.Duration, typ intervalGrowthType) time.Duration {
	switch typ {
	case exponentialGrowth:
		return typeutil.MinDuration(time.Duration(float64(x)*ScheduleIntervalFactor), maxInterval)
	case linearGrowth:
		return typeutil.MinDuration(x+MinSlowScheduleInterval, maxInterval)
	case zeroGrowth:
		return x
	default:
		log.Fatal("type error", errs.ZapError(errs.ErrInternalGrowth))
	}
	return 0
}

// BaseScheduler is a basic scheduler for all other complex scheduler
type BaseScheduler struct {
	OpController *operator.Controller
}

// NewBaseScheduler returns a basic scheduler
func NewBaseScheduler(opController *operator.Controller) *BaseScheduler {
	return &BaseScheduler{OpController: opController}
}

func (s *BaseScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "not implements")
}

// GetMinInterval returns the minimal interval for the scheduler
func (s *BaseScheduler) GetMinInterval() time.Duration {
	return MinScheduleInterval
}

// EncodeConfig encode config for the scheduler
func (s *BaseScheduler) EncodeConfig() ([]byte, error) {
	return EncodeConfig(nil)
}

// ReloadConfig reloads the config from the storage.
// By default, the scheduler does not need to reload the config
// if it doesn't support the dynamic configuration.
func (s *BaseScheduler) ReloadConfig() error { return nil }

// GetNextInterval return the next interval for the scheduler
func (s *BaseScheduler) GetNextInterval(interval time.Duration) time.Duration {
	return intervalGrow(interval, MaxScheduleInterval, exponentialGrowth)
}

// PrepareConfig does some prepare work about config.
func (s *BaseScheduler) PrepareConfig(cluster sche.SchedulerCluster) error { return nil }

// CleanConfig does some cleanup work about config.
func (s *BaseScheduler) CleanConfig(cluster sche.SchedulerCluster) {}
