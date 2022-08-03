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

package logutil

import (
	"bytes"
	"testing"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestLoggerManager(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	var testLoggerManager loggerManager
	testLoggerManager.Init()

	// Get a logger that does not exist.
	re.Nil(testLoggerManager.GetPluggableLogger("test", false))
	// Create PluggableLogger.
	logger := testLoggerManager.GetPluggableLogger("test", true)
	re.Nil(logger.GetLogger())
	re.Equal("test", logger.GetName())
	// Plug logger.
	nop := zap.NewNop()
	testLoggerManager.GetPluggableLogger("test", false).PlugLogger(nop)
	re.NotNil(logger.GetLogger())
	logger.Debug("noop")
	// Unplug logger.
	testLoggerManager.GetPluggableLogger("test", false).UnplugLogger(nop)
	re.Nil(logger.GetLogger())
	logger.Debug("noop")
}

func TestPluggableLogger(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	var testLoggerManager loggerManager
	testLoggerManager.Init()

	pl := testLoggerManager.GetPluggableLogger("foo", true)
	buffer := new(bytes.Buffer)
	lg, _, err := log.InitLoggerWithWriteSyncer(&log.Config{
		Level:               "info",
		DisableTimestamp:    true,
		DisableCaller:       true,
		DisableStacktrace:   true,
		DisableErrorVerbose: true,
	}, zapcore.AddSync(buffer))
	re.NoError(err)
	pl.PlugLogger(lg)
	pl.Info("world", zap.Int64("answer", 42))
	re.NoError(lg.Sync())
	re.Equal("[INFO] [world] [answer=42]\n", buffer.String())
	pl.UnplugLogger(lg)
}
