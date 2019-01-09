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
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	. "github.com/pingcap/check"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var _ = Suite(&testZapLogSuite{})

type testZapLogSuite struct{}

// testingWriter is a WriteSyncer that writes to the the messages.
type testingWriter struct {
	c        *C
	messages []string
}

func newTestingWriter(c *C) *testingWriter {
	return &testingWriter{c: c}
}

func (w *testingWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	p = bytes.TrimRight(p, "\n")
	m := fmt.Sprintf("%s", p)
	w.messages = append(w.messages, m)
	return n, nil
}

func (w *testingWriter) Sync() error {
	return nil
}

type verifyLogger struct {
	*zap.Logger
	w *testingWriter
}

func (v *verifyLogger) AssertMessage(msg ...string) {
	for i, m := range msg {
		v.w.c.Assert(m, Equals, v.w.messages[i])
	}
}

func newZapTestLogger(cfg *LogConfig, c *C) verifyLogger {
	writer := newTestingWriter(c)
	opt := cfg.buildOptions(writer)
	level := zap.NewAtomicLevel()
	err := level.UnmarshalText([]byte(cfg.Level))
	c.Assert(err, IsNil)
	lg := zap.New(zapcore.NewCore(newZapTextEncoder(cfg), writer, level), opt...)
	return verifyLogger{
		Logger: lg,
		w:      writer,
	}
}

func (t *testZapLogSuite) TestLog(c *C) {
	conf := &LogConfig{Level: "debug", File: FileLogConfig{}, DisableTimestamp: true}
	lg := newZapTestLogger(conf, c)
	sugar := lg.Sugar()

	defer sugar.Sync()
	sugar.Infow("failed to fetch URL",
		"url", "http://example.com",
		"attempt", 3,
		"backoff", time.Second,
	)
	sugar.Infof(`failed to "fetch" [URL]: %s`, "http://example.com")
	sugar.Debugw("Slow query",
		"sql", `SELECT * FROM TABLE
	WHERE ID="abc"`,
		"duration", 1300*time.Millisecond,
		"process keys", 1500,
	)

	lg.AssertMessage(
		`[INFO] [logutil/zap_log_test.go:84] ["failed to fetch URL"] [url=http://example.com] [attempt=3] [backoff=1s]`,
		`[INFO] [logutil/zap_log_test.go:89] ["failed to \"fetch\" [URL]: http://example.com"]`,
		`[DEBUG] [logutil/zap_log_test.go:90] ["Slow query"] [sql="SELECT * FROM TABLE\n\tWHERE ID=\"abc\""] [duration=1.3s] ["process keys"=1500]`,
	)
}

func (t *testZapLogSuite) TestRotateLog(c *C) {
	tempDir, _ := ioutil.TempDir("/tmp", "pd-tests-log")
	conf := &LogConfig{
		Level: "info",
		File: FileLogConfig{
			Filename: tempDir + "/test.log",
			MaxSize:  1,
		},
	}
	lg, err := InitZapLogger(conf)
	c.Assert(err, IsNil)
	var data []byte
	for i := 1; i <= 1*1024*1024; i++ {
		if i%1000 != 0 {
			data = append(data, 'd')
			continue
		}
		lg.Info(string(data))
		data = data[:0]
	}
	files, _ := ioutil.ReadDir(tempDir)
	c.Assert(len(files), Equals, 2)
	os.RemoveAll(tempDir)
}
