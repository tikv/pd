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
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/config"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage/endpoint"
)

// Scheduler is an interface to schedule resources.
type Scheduler interface {
	http.Handler
	GetName() string
	// GetType should in accordance with the name passing to RegisterScheduler()
	GetType() types.CheckerSchedulerType
	EncodeConfig() ([]byte, error)
	// ReloadConfig reloads the config from the storage.
	ReloadConfig() error
	GetMinInterval() time.Duration
	GetNextInterval(interval time.Duration) time.Duration
	PrepareConfig(cluster sche.SchedulerCluster) error
	CleanConfig(cluster sche.SchedulerCluster)
	Schedule(cluster sche.SchedulerCluster, dryRun bool) ([]*operator.Operator, []plan.Plan)
	IsScheduleAllowed(cluster sche.SchedulerCluster) bool
	// IsDiable returns if the scheduler is disabled, it only works for default schedulers.
	// - BalanceRegionScheduler
	// - BalanceLeaderScheduler
	// - BalanceHotRegionScheduler
	// - EvictSlowStoreScheduler
	IsDisable() bool
	// SetDisable sets the scheduler's disable, it only works for default schedulers.
	SetDisable(bool) error
	// IsDefault returns if the scheduler is a default scheduler.
	IsDefault() bool
}

// EncodeConfig encode the custom config for each scheduler.
func EncodeConfig(v any) ([]byte, error) {
	marshaled, err := json.Marshal(v)
	if err != nil {
		return nil, errs.ErrJSONMarshal.Wrap(err)
	}
	return marshaled, nil
}

// DecodeConfig decode the custom config for each scheduler.
func DecodeConfig(data []byte, v any) error {
	err := json.Unmarshal(data, v)
	if err != nil {
		return errs.ErrJSONUnmarshal.Wrap(err)
	}
	return nil
}

// ToPayload returns the payload of config.
func ToPayload(sches, configs []string) map[string]any {
	payload := make(map[string]any)
	for i, sche := range sches {
		var config any
		err := DecodeConfig([]byte(configs[i]), &config)
		if err != nil {
			log.Error("failed to decode scheduler config",
				zap.String("config", configs[i]),
				zap.String("scheduler", sche),
				errs.ZapError(err))
			continue
		}
		payload[sche] = config
	}
	return payload
}

// ConfigDecoder used to decode the config.
type ConfigDecoder func(v any) error

// ConfigSliceDecoderBuilder used to build slice decoder of the config.
type ConfigSliceDecoderBuilder func([]string) ConfigDecoder

// ConfigJSONDecoder used to build a json decoder of the config.
func ConfigJSONDecoder(data []byte) ConfigDecoder {
	return func(v any) error {
		return DecodeConfig(data, v)
	}
}

// ConfigSliceDecoder the default decode for the config.
func ConfigSliceDecoder(typ types.CheckerSchedulerType, args []string) ConfigDecoder {
	builder, ok := schedulerArgsToDecoder[typ]
	if !ok {
		return func(any) error {
			return errors.Errorf("the config decoder do not register for %v", typ)
		}
	}
	return builder(args)
}

// CreateSchedulerFunc is for creating scheduler.
type CreateSchedulerFunc func(opController *operator.Controller, storage endpoint.ConfigStorage, dec ConfigDecoder, removeSchedulerCb ...func(string) error) (Scheduler, error)

var (
	schedulerMap           = make(map[types.CheckerSchedulerType]CreateSchedulerFunc)
	schedulerArgsToDecoder = make(map[types.CheckerSchedulerType]ConfigSliceDecoderBuilder)
)

// RegisterScheduler binds a scheduler creator. It should be called in init()
// func of a package.
func RegisterScheduler(typ types.CheckerSchedulerType, createFn CreateSchedulerFunc) {
	if _, ok := schedulerMap[typ]; ok {
		log.Fatal("duplicated scheduler", zap.Stringer("type", typ), errs.ZapError(errs.ErrSchedulerDuplicated))
	}
	schedulerMap[typ] = createFn
}

// RegisterSliceDecoderBuilder convert arguments to config. It should be called in init()
// func of package.
func RegisterSliceDecoderBuilder(typ types.CheckerSchedulerType, builder ConfigSliceDecoderBuilder) {
	if _, ok := schedulerArgsToDecoder[typ]; ok {
		log.Fatal("duplicated scheduler", zap.Stringer("type", typ), errs.ZapError(errs.ErrSchedulerDuplicated))
	}
	schedulerArgsToDecoder[typ] = builder
	config.RegisterScheduler(typ)
}

// CreateScheduler creates a scheduler with registered creator func.
func CreateScheduler(
	typ types.CheckerSchedulerType,
	oc *operator.Controller,
	storage endpoint.ConfigStorage,
	dec ConfigDecoder,
	removeSchedulerCb ...func(string) error,
) (Scheduler, error) {
	fn, ok := schedulerMap[typ]
	if !ok {
		return nil, errs.ErrSchedulerCreateFuncNotRegistered.FastGenByArgs(typ)
	}

	return fn(oc, storage, dec, removeSchedulerCb...)
}

// SaveSchedulerConfig saves the config of the specified scheduler.
func SaveSchedulerConfig(storage endpoint.ConfigStorage, s Scheduler) error {
	data, err := s.EncodeConfig()
	if err != nil {
		return err
	}
	return storage.SaveSchedulerConfig(s.GetName(), data)
}

// FindSchedulerTypeByName finds the type of the specified name.
func FindSchedulerTypeByName(name string) types.CheckerSchedulerType {
	var typ types.CheckerSchedulerType
	for registeredType := range schedulerMap {
		if strings.Contains(name, registeredType.String()) {
			// If the name matches multiple types, we should choose the longest one.
			if len(registeredType) > len(typ) {
				typ = registeredType
			}
		}
	}
	// This is for compatibility. Because the string of ScatterRangeScheduler is
	// "scatter-range" before. If user adds a ScatterRangeScheduler(which is
	// "scatter-range" yet) and then upgrades the cluster(its ScatterRangeScheduler
	// is "scatter-range-scheduler"), we need these codes to keep the compatibility.
	if len(typ) == 0 && strings.Contains(name, "scatter-range") {
		return types.ScatterRangeScheduler
	}
	return typ
}
