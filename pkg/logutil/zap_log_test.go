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
	"math"
	"os"
	"time"
	"unsafe"

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
	sugar.Info("Welcome")
	sugar.Info("Welcome TiDB")
	sugar.Info("欢迎")
	sugar.Info("欢迎来到 TiDB")
	sugar.Warnw("Type",
		"Counter", math.NaN(),
		"Score", math.Inf(1),
	)
	lg.AssertMessage(
		`[INFO] [zap_log_test.go:85] ["failed to fetch URL"] [url=http://example.com] [attempt=3] [backoff=1s]`,
		`[INFO] [zap_log_test.go:90] ["failed to \"fetch\" [URL]: http://example.com"]`,
		`[DEBUG] [zap_log_test.go:91] ["Slow query"] [sql="SELECT * FROM TABLE\n\tWHERE ID=\"abc\""] [duration=1.3s] ["process keys"=1500]`,
		`[INFO] [zap_log_test.go:97] [Welcome]`,
		`[INFO] [zap_log_test.go:98] ["Welcome TiDB"]`,
		`[INFO] [zap_log_test.go:99] [欢迎]`,
		`[INFO] [zap_log_test.go:100] ["欢迎来到 TiDB"]`,
		`[WARN] [zap_log_test.go:101] [Type] [Counter=NaN] [Score=+Inf]`,
	)
	c.Assert(func() { sugar.Panic("unknown") }, PanicMatches, `unknown`)
}

func (t *testZapLogSuite) TestTimeEncoder(c *C) {
	sec := int64(1547192741)
	nsec := int64(165279177)
	tt := time.Unix(sec, nsec)
	conf := &LogConfig{Level: "deug", File: FileLogConfig{}, DisableTimestamp: true}
	enc := newZapTextEncoder(conf).(*textEncoder)
	DefaultTimeEncoder(tt, enc)
	c.Assert(enc.buf.String(), Equals, `2019/01/11 15:45:41.165 +08:00`)
	enc.buf.Reset()
	loc, err := time.LoadLocation("UTC")
	c.Assert(err, IsNil)
	utcTime := tt.In(loc)
	DefaultTimeEncoder(utcTime, enc)
	c.Assert(enc.buf.String(), Equals, `2019/01/11 07:45:41.165 +00:00`)
}

// https://github.com/tikv/rfcs/blob/master/text/2018-12-19-unified-log-format.md#log-header-section
func (t *testZapLogSuite) TestZapCaller(c *C) {
	data := []zapcore.EntryCaller{
		{Defined: true, PC: uintptr(unsafe.Pointer(nil)), File: "server.go", Line: 132},
		{Defined: true, PC: uintptr(unsafe.Pointer(nil)), File: "server/coordinator.go", Line: 20},
		{Defined: true, PC: uintptr(unsafe.Pointer(nil)), File: `z\test_coordinator1.go`, Line: 20},
		{Defined: false, PC: uintptr(unsafe.Pointer(nil)), File: "", Line: 0},
	}
	expect := []string{
		"server.go:132",
		"coordinator.go:20",
		"ztest_coordinator1.go:20",
		"<unknown>",
	}
	conf := &LogConfig{Level: "deug", File: FileLogConfig{}, DisableTimestamp: true}
	enc := newZapTextEncoder(conf).(*textEncoder)

	for i, d := range data {
		ShortCallerEncoder(d, enc)
		c.Assert(enc.buf.String(), Equals, expect[i])
		enc.buf.Reset()
	}
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
