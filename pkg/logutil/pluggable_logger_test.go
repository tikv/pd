// Copyright 2020 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package logutil

import (
	"bytes"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	. "github.com/pingcap/check"
	"github.com/pingcap/log"
)

var _ = Suite(&testPluggableLoggerSuite{})

type testPluggableLoggerSuite struct {
	testLoggerManager loggerManager
}

func (s *testPluggableLoggerSuite) SetUpSuite(c *C) {
	s.testLoggerManager.Init()
}

func (s *testPluggableLoggerSuite) TestLoggerManager(c *C) {
	// Get a logger that does not exist.
	c.Assert(s.testLoggerManager.GetPluggableLogger("test", false), IsNil)
	// Create PluggableLogger.
	logger := s.testLoggerManager.GetPluggableLogger("test", true)
	c.Assert(logger.GetLogger(), IsNil)
	c.Assert(logger.GetName(), Equals, "test")
	// Plug logger.
	s.testLoggerManager.GetPluggableLogger("test", false).SetLogger(zap.NewNop())
	c.Assert(logger.GetLogger(), NotNil)
	logger.Debug("noop")
	// Unplug logger.
	s.testLoggerManager.GetPluggableLogger("test", false).SetLogger(nil)
	c.Assert(logger.GetLogger(), IsNil)
	logger.Debug("noop")
}

func (s *testPluggableLoggerSuite) TestPluggableLogger(c *C) {
	pl := s.testLoggerManager.GetPluggableLogger("foo", true)
	buffer := new(bytes.Buffer)
	lg, _, err := log.InitLoggerWithWriteSyncer(&log.Config{
		Level:               "info",
		DisableTimestamp:    true,
		DisableCaller:       true,
		DisableStacktrace:   true,
		DisableErrorVerbose: true,
	}, zapcore.AddSync(buffer))
	c.Assert(err, IsNil)
	pl.SetLogger(lg)
	pl.Info("bar", zap.Int64("int64", 42))
	c.Assert(lg.Sync(), IsNil)
	c.Assert(buffer.String(), Equals, "[INFO] [bar] [int64=42]\n")
}
