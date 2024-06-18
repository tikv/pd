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

package logutil

import (
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// FileLogConfig serializes file log related config in toml/json.
type FileLogConfig struct {
	// Log filename, leave empty to disable file log.
	Filename string `toml:"filename" json:"filename"`
	// Max size for a single file, in MB.
	MaxSize int `toml:"max-size" json:"max-size"`
	// Max log keep days, default is never deleting.
	MaxDays int `toml:"max-days" json:"max-days"`
	// Maximum number of old log files to retain.
	MaxBackups int `toml:"max-backups" json:"max-backups"`
}

// LogConfig serializes log related config in toml/json.
type LogConfig struct {
	// Log level.
	Level string `toml:"level" json:"level"`
	// Log format. one of json, text, or console.
	Format string `toml:"format" json:"format"`
	// Disable automatic timestamps in output.
	DisableTimestamp bool `toml:"disable-timestamp" json:"disable-timestamp"`
	// File log config.
	File FileLogConfig `toml:"file" json:"file"`
}

// StringToZapLogLevel translates log level string to log level.
func StringToZapLogLevel(level string) zapcore.Level {
	switch strings.ToLower(level) {
	case "fatal":
		return zapcore.FatalLevel
	case "error":
		return zapcore.ErrorLevel
	case "warn", "warning":
		return zapcore.WarnLevel
	case "debug":
		return zapcore.DebugLevel
	case "info":
		return zapcore.InfoLevel
	}
	return zapcore.InfoLevel
}

// SetupLogger setup the logger.
func SetupLogger(
	logConfig log.Config,
	logger **zap.Logger,
	logProps **log.ZapProperties,
	redactInfoLog bool, redactInfoMark string,
) error {
	lg, p, err := log.InitLogger(&logConfig, zap.AddStacktrace(zapcore.FatalLevel))
	if err != nil {
		return errs.ErrInitLogger.Wrap(err)
	}
	*logger = lg
	*logProps = p
	setRedactType(redactInfoLog, redactInfoMark)
	return nil
}

// LogPanic logs the panic reason and stack, then exit the process.
// Commonly used with a `defer`.
func LogPanic() {
	if e := recover(); e != nil {
		log.Fatal("panic", zap.Reflect("recover", e))
	}
}

type redactType int

const (
	// redactOFF indicates no redaction.
	redactOFF redactType = iota
	// redactON indicates redaction.
	redactON
	// redactWithMarker indicates redaction with marker.
	redactWithMarker
)

var (
	curRedactType   atomic.Value
	redactMarkLeft  atomic.Value
	redactMarkRight atomic.Value
)

func init() {
	curRedactType.Store(redactOFF)
}

func getRedactType() redactType {
	return curRedactType.Load().(redactType)
}

func setRedactType(redactInfoLog bool, redactInfoMark string) {
	if !redactInfoLog {
		curRedactType.Store(redactOFF)
		return
	}
	markLength := len(redactInfoMark)
	if markLength == 0 || markLength != 2 {
		curRedactType.Store(redactON)
		return
	}
	curRedactType.Store(redactWithMarker)
	redactMarkLeft.Store(rune(redactInfoMark[0]))
	redactMarkRight.Store(rune(redactInfoMark[1]))
}

func redactInfo(input string) string {
	res := &strings.Builder{}
	res.Grow(len(input) + 2)
	leftMark, rightMark := redactMarkLeft.Load().(rune), redactMarkRight.Load().(rune)
	_, _ = res.WriteRune(leftMark)
	for _, c := range input {
		if c == leftMark || c == rightMark {
			_, _ = res.WriteRune(c)
			_, _ = res.WriteRune(c)
		} else {
			_, _ = res.WriteRune(c)
		}
	}
	_, _ = res.WriteRune(rightMark)
	return res.String()
}

// ZapRedactByteString receives []byte argument and return omitted information zap.Field if redact log enabled
func ZapRedactByteString(key string, arg []byte) zap.Field {
	return zap.ByteString(key, RedactBytes(arg))
}

// ZapRedactString receives string argument and return omitted information in zap.Field if redact log enabled
func ZapRedactString(key, arg string) zap.Field {
	return zap.String(key, RedactString(arg))
}

// ZapRedactStringer receives stringer argument and return omitted information in zap.Field  if redact log enabled
func ZapRedactStringer(key string, arg fmt.Stringer) zap.Field {
	return zap.Stringer(key, RedactStringer(arg))
}

// RedactBytes receives []byte argument and return omitted information if redact log enabled
func RedactBytes(arg []byte) []byte {
	switch getRedactType() {
	case redactON:
		return []byte("?")
	case redactWithMarker:
		return typeutil.StringToBytes(redactInfo(typeutil.BytesToString(arg)))
	default:
	}
	return arg
}

// RedactString receives string argument and return omitted information if redact log enabled
func RedactString(arg string) string {
	switch getRedactType() {
	case redactON:
		return "?"
	case redactWithMarker:
		return redactInfo(arg)
	default:
	}
	return arg
}

// RedactStringer receives stringer argument and return omitted information if redact log enabled
func RedactStringer(arg fmt.Stringer) fmt.Stringer {
	switch getRedactType() {
	case redactON:
		return &redactedStringer{"?"}
	case redactWithMarker:
		return &redactedStringer{redactInfo(arg.String())}
	default:
	}
	return arg
}

type redactedStringer struct {
	content string
}

// String implement fmt.Stringer
func (rs *redactedStringer) String() string {
	return rs.content
}

// CondUint32 constructs a field with the given key and value conditionally.
// If the condition is true, it constructs a field with uint32 type; otherwise,
// skip the field.
func CondUint32(key string, val uint32, condition bool) zap.Field {
	if condition {
		return zap.Uint32(key, val)
	}
	return zap.Skip()
}

// IsLevelLegal checks whether the level is legal.
func IsLevelLegal(level string) bool {
	switch strings.ToLower(level) {
	case "fatal", "error", "warn", "warning", "debug", "info":
		return true
	default:
		return false
	}
}
