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
)

var _ = Suite(&testZapLogSuite{})

type testZapLogSuite struct{}

func (t *testZapLogSuite) TestLog(c *C) {
	conf := &LogConfig{Level: "debug", File: FileLogConfig{}}
	lg, err := InitZapLogger(conf)
	c.Assert(err, IsNil)
	sugar := lg.Sugar()

	defer sugar.Sync()
	sugar.Infow("failed to fetch URL",
		"url", "http://example.com",
		"attempt", 3,
		"backoff", time.Second,
	)
	//	var cfg zap.Config
	sugar.Infof(`failed to "fetch" [URL]: %s`, "http://example.com")
}

func (t *testZapLogSuite) TestFileLog(c *C) {
	conf := &LogConfig{
		Level: "info",
		File: FileLogConfig{
			Filename: "/tmp/test.log",
			MaxSize:  1,
		},
	}
	lg, err := InitZapLogger(conf)
	c.Assert(err, IsNil)
	var data []byte
	for i := 1; i <= 2*1024*1024; i++ {
		if i%1000 != 0 {
			data = append(data, 'd')
			continue
		}
		lg.Info(string(data))
		data = data[:0]
	}
}
