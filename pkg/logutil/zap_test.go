// Copyright 2019 PingCAP, Inc.
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
	"time"

	. "github.com/pingcap/check"
	"go.uber.org/zap"
)

var _ = Suite(&testZapLogSuite{})

type testZapLogSuite struct{}

func (t *testZapLogSuite) TestLog(c *C) {
	zap.RegisterEncoder("custom", NewTextEncoder)
	cc := zap.NewDevelopmentEncoderConfig()
	cc.MessageKey = "message"
	cfg := zap.Config{
		Level:            zap.NewAtomicLevelAt(zap.DebugLevel),
		Development:      true,
		Encoding:         "custom",
		EncoderConfig:    cc,
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
	logger, err := cfg.Build()
	c.Assert(err, IsNil)
	sugar := logger.Sugar()

	defer sugar.Sync()
	sugar.Infow("failed to fetch URL",
		"url", "http://example.com",
		"attempt", 3,
		"backoff", time.Second,
	)
	//	var cfg zap.Config
	sugar.Infof(`failed to "fetch" [URL]: %s`, "http://example.com")
}
