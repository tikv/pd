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
func SetupLogger(logConfig log.Config, logger **zap.Logger, logProps **log.ZapProperties, enabled ...bool) error {
	lg, p, err := log.InitLogger(&logConfig, zap.AddStacktrace(zapcore.FatalLevel))
	if err != nil {
		return errs.ErrInitLogger.Wrap(err)
	}
	*logger = lg
	*logProps = p
	if len(enabled) > 0 {
		SetRedactLog(enabled[0])
	}
	return nil
}

// LogPanic logs the panic reason and stack, then exit the process.
// Commonly used with a `defer`.
func LogPanic() {
	if e := recover(); e != nil {
		log.Fatal("panic", zap.Reflect("recover", e))
	}
}

var (
	enabledRedactLog atomic.Value
)

func init() {
	SetRedactLog(false)
}

// IsRedactLogEnabled indicates whether the log desensitization is enabled
func IsRedactLogEnabled() bool {
	return enabledRedactLog.Load().(bool)
}

// SetRedactLog sets enabledRedactLog
func SetRedactLog(enabled bool) {
	enabledRedactLog.Store(enabled)
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
	if IsRedactLogEnabled() {
		return []byte("?")
	}
	return arg
}

// RedactString receives string argument and return omitted information if redact log enabled
func RedactString(arg string) string {
	if IsRedactLogEnabled() {
		return "?"
	}
	return arg
}

// RedactStringer receives stringer argument and return omitted information if redact log enabled
func RedactStringer(arg fmt.Stringer) fmt.Stringer {
	if IsRedactLogEnabled() {
		return stringer{}
	}
	return arg
}

type stringer struct {
}

// String implement fmt.Stringer
func (s stringer) String() string {
	return "?"
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
