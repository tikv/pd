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
	"math/rand"
	"net/http"
	"time"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/types"
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
	R            *rand.Rand

	name string
	tp   types.CheckerSchedulerType
	conf schedulerConfig
}

// NewBaseScheduler returns a basic scheduler
func NewBaseScheduler(
	opController *operator.Controller,
	tp types.CheckerSchedulerType,
	conf schedulerConfig,
) *BaseScheduler {
	return &BaseScheduler{OpController: opController, tp: tp, conf: conf, R: rand.New(rand.NewSource(time.Now().UnixNano()))}
}

func (*BaseScheduler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	fmt.Fprintf(w, "not implements")
}

// GetMinInterval returns the minimal interval for the scheduler
func (*BaseScheduler) GetMinInterval() time.Duration {
	return MinScheduleInterval
}

// EncodeConfig encode config for the scheduler
func (*BaseScheduler) EncodeConfig() ([]byte, error) {
	return EncodeConfig(nil)
}

// ReloadConfig reloads the config from the storage.
// By default, the scheduler does not need to reload the config
// if it doesn't support the dynamic configuration.
func (*BaseScheduler) ReloadConfig() error { return nil }

// GetNextInterval return the next interval for the scheduler
func (*BaseScheduler) GetNextInterval(interval time.Duration) time.Duration {
	return intervalGrow(interval, MaxScheduleInterval, exponentialGrowth)
}

// PrepareConfig does some prepare work about config.
func (*BaseScheduler) PrepareConfig(sche.SchedulerCluster) error { return nil }

// CleanConfig does some cleanup work about config.
func (*BaseScheduler) CleanConfig(sche.SchedulerCluster) {}

// GetName returns the name of the scheduler
func (s *BaseScheduler) GetName() string {
	if len(s.name) == 0 {
		return s.tp.String()
	}
	return s.name
}

// GetType returns the type of the scheduler
func (s *BaseScheduler) GetType() types.CheckerSchedulerType {
	return s.tp
}

// IsDisable implements the Scheduler interface.
func (s *BaseScheduler) IsDisable() bool {
	if conf, ok := s.conf.(defaultSchedulerConfig); ok {
		return conf.isDisable()
	}
	return false
}

// SetDisable implements the Scheduler interface.
func (s *BaseScheduler) SetDisable(disable bool) error {
	if conf, ok := s.conf.(defaultSchedulerConfig); ok {
		return conf.setDisable(disable)
	}
	return nil
}

// IsDefault returns if the scheduler is a default scheduler.
func (s *BaseScheduler) IsDefault() bool {
	if _, ok := s.conf.(defaultSchedulerConfig); ok {
		return true
	}
	return false
}
