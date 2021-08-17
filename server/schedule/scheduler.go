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

package schedule

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"go.uber.org/zap"
)

// Scheduler is an interface to schedule resources.
type Scheduler interface {
	http.Handler
	GetName() string
	// GetType should in accordance with the name passing to schedule.RegisterScheduler()
	GetType() string
	EncodeConfig() ([]byte, error)
	GetMinInterval() time.Duration
	GetNextInterval(interval time.Duration) time.Duration
	Prepare(cluster opt.Cluster) error
	Cleanup(cluster opt.Cluster)
	Schedule(cluster opt.Cluster) []*operator.Operator
	IsScheduleAllowed(cluster opt.Cluster) bool
}

// EncodeConfig encode the custom config for each scheduler.
func EncodeConfig(v interface{}) ([]byte, error) {
	marshaled, err := json.Marshal(v)
	if err != nil {
		return nil, errs.ErrJSONMarshal.Wrap(err).FastGenWithCause()
	}
	return marshaled, nil
}

// DecodeConfig decode the custom config for each scheduler.
func DecodeConfig(data []byte, v interface{}) error {
	err := json.Unmarshal(data, v)
	if err != nil {
		return errs.ErrJSONUnmarshal.Wrap(err).FastGenWithCause()
	}
	return nil
}

// ConfigDecoder used to decode the config.
type ConfigDecoder func(v interface{}) error

// ConfigSliceDecoderBuilder used to build slice decoder of the config.
type ConfigSliceDecoderBuilder func([]string) ConfigDecoder

// ConfigJSONDecoder used to build a json decoder of the config.
func ConfigJSONDecoder(data []byte) ConfigDecoder {
	return func(v interface{}) error {
		return DecodeConfig(data, v)
	}
}

// ConfigSliceDecoder the default decode for the config.
func ConfigSliceDecoder(name string, args []string) ConfigDecoder {
	builder, ok := schedulerArgsToDecoder[name]
	if !ok {
		return func(v interface{}) error {
			return errors.Errorf("the config decoder do not register for %s", name)
		}
	}
	return builder(args)
}

// CreateSchedulerFunc is for creating scheduler.
type CreateSchedulerFunc func(opController *OperatorController, storage *core.Storage, dec ConfigDecoder) (Scheduler, error)

var schedulerMap = make(map[string]CreateSchedulerFunc)
var schedulerArgsToDecoder = make(map[string]ConfigSliceDecoderBuilder)

// RegisterScheduler binds a scheduler creator. It should be called in init()
// func of a package.
func RegisterScheduler(typ string, createFn CreateSchedulerFunc) {
	if _, ok := schedulerMap[typ]; ok {
		log.Fatal("duplicated scheduler", zap.String("type", typ), errs.ZapError(errs.ErrSchedulerDuplicated))
	}
	schedulerMap[typ] = createFn
}

// RegisterSliceDecoderBuilder convert arguments to config. It should be called in init()
// func of package.
func RegisterSliceDecoderBuilder(typ string, builder ConfigSliceDecoderBuilder) {
	if _, ok := schedulerArgsToDecoder[typ]; ok {
		log.Fatal("duplicated scheduler", zap.String("type", typ), errs.ZapError(errs.ErrSchedulerDuplicated))
	}
	schedulerArgsToDecoder[typ] = builder
	config.RegisterScheduler(typ)
}

// CreateScheduler creates a scheduler with registered creator func.
func CreateScheduler(typ string, opController *OperatorController, storage *core.Storage, dec ConfigDecoder) (Scheduler, error) {
	fn, ok := schedulerMap[typ]
	if !ok {
		return nil, errs.ErrSchedulerCreateFuncNotRegistered.FastGenByArgs(typ)
	}

	s, err := fn(opController, storage, dec)
	if err != nil {
		return nil, err
	}
	data, err := s.EncodeConfig()
	if err != nil {
		return nil, err
	}
	err = storage.SaveScheduleConfig(s.GetName(), data)
	return s, err
}

// FindSchedulerTypeByName finds the type of the specified name.
func FindSchedulerTypeByName(name string) string {
	var typ string
	for registeredType := range schedulerMap {
		if strings.Contains(name, registeredType) {
			if len(registeredType) > len(typ) {
				typ = registeredType
			}
		}
	}
	return typ
}
