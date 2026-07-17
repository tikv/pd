// Copyright 2026 TiKV Project Authors.
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

package logutil

import (
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/pingcap/log"
)

var sampleLoggerFactory = getSampleLoggerFactory(time.Minute, 5)

// getSampleLoggerFactory returns a factory that lazily creates one sampled logger.
// The logger writes the first `first` entries with the same level and message
// in each tick and drops the remaining matching entries until the next tick.
// Zap hashes messages into 4096 counters per level, so distinct messages may
// share a sampling counter if their hashes collide.
// Callers must pass positive values for `tick` and `first`. The current log.L()
// is captured lazily on the returned factory's first invocation. The created
// logger stays pinned to that global logger, so later log.ReplaceGlobals calls
// do not retarget it.
func getSampleLoggerFactory(tick time.Duration, first int, fields ...zap.Field) func() *zap.Logger {
	var (
		once   sync.Once
		logger *zap.Logger
	)
	return func() *zap.Logger {
		once.Do(func() {
			sampleCore := zap.WrapCore(func(core zapcore.Core) zapcore.Core {
				return zapcore.NewSamplerWithOptions(core, tick, first, 0)
			})
			logger = log.L().With(fields...).With(zap.String("sampled", "")).WithOptions(sampleCore)
		})
		return logger
	}
}

// SamplerLogger returns the shared sampled logger for the PD client.
func SamplerLogger() *zap.Logger {
	return sampleLoggerFactory()
}
