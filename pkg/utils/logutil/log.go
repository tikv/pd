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
	"encoding/json"
	"errors"
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
func SetupLogger(
	logConfig log.Config,
	logger **zap.Logger,
	logProps **log.ZapProperties,
	redactInfoLogType RedactInfoLogType,
) error {
	lg, p, err := log.InitLogger(&logConfig, zap.AddStacktrace(zapcore.FatalLevel))
	if err != nil {
		return errs.ErrInitLogger.Wrap(err)
	}
	*logger = lg
	*logProps = p
	setRedactType(redactInfoLogType)
	return nil
}

// LogPanic logs the panic reason and stack, then exit the process.
// Commonly used with a `defer`.
func LogPanic() {
	if e := recover(); e != nil {
		log.Fatal("panic", zap.Reflect("recover", e))
	}
}

// RedactInfoLogType is the behavior of redacting sensitive information in logs.
type RedactInfoLogType int

const (
	// RedactInfoLogOFF means log redaction is disabled.
	RedactInfoLogOFF RedactInfoLogType = iota
	// RedactInfoLogON means log redaction is enabled, and will replace the sensitive information with "?".
	RedactInfoLogON
	// RedactInfoLogMarker means log redaction is enabled, and will use single guillemets ‹› to enclose the sensitive information.
	RedactInfoLogMarker
)

// MarshalJSON implements the `json.Marshaler` interface to ensure the compatibility.
func (t RedactInfoLogType) MarshalJSON() ([]byte, error) {
	switch t {
	case RedactInfoLogON:
		return json.Marshal(true)
	case RedactInfoLogMarker:
		return json.Marshal("MARKER")
	default:
	}
	return json.Marshal(false)
}

const invalidRedactInfoLogTypeErrMsg = `the "redact-info-log" value is invalid; it should be either false, true, or "MARKER"`

// UnmarshalJSON implements the `json.Marshaler` interface to ensure the compatibility.
func (t *RedactInfoLogType) UnmarshalJSON(data []byte) error {
	var s string
	err := json.Unmarshal(data, &s)
	if err == nil && strings.ToUpper(s) == "MARKER" {
		*t = RedactInfoLogMarker
		return nil
	}
	var b bool
	err = json.Unmarshal(data, &b)
	if err != nil {
		return errors.New(invalidRedactInfoLogTypeErrMsg)
	}
	if b {
		*t = RedactInfoLogON
	} else {
		*t = RedactInfoLogOFF
	}
	return nil
}

// UnmarshalTOML implements the `toml.Unmarshaler` interface to ensure the compatibility.
func (t *RedactInfoLogType) UnmarshalTOML(data any) error {
	switch v := data.(type) {
	case bool:
		if v {
			*t = RedactInfoLogON
		} else {
			*t = RedactInfoLogOFF
		}
		return nil
	case string:
		if strings.ToUpper(v) == "MARKER" {
			*t = RedactInfoLogMarker
			return nil
		}
		return errors.New(invalidRedactInfoLogTypeErrMsg)
	default:
	}
	return errors.New(invalidRedactInfoLogTypeErrMsg)
}

var (
	curRedactType atomic.Value
)

func init() {
	setRedactType(RedactInfoLogOFF)
}

func getRedactType() RedactInfoLogType {
	return curRedactType.Load().(RedactInfoLogType)
}

func setRedactType(redactInfoLogType RedactInfoLogType) {
	curRedactType.Store(redactInfoLogType)
}

const (
	leftMark  = '‹'
	rightMark = '›'
)

func redactInfo(input string) string {
	res := &strings.Builder{}
	res.Grow(len(input) + 2)
	_, _ = res.WriteRune(leftMark)
	for _, c := range input {
		// Double the mark character if it is already in the input string.
		// to avoid the ambiguity of the redacted content.
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
	case RedactInfoLogON:
		return []byte("?")
	case RedactInfoLogMarker:
		// Use unsafe conversion to avoid copy.
		return []byte(redactInfo(string(arg)))
	default:
	}
	return arg
}

// RedactString receives string argument and return omitted information if redact log enabled
func RedactString(arg string) string {
	switch getRedactType() {
	case RedactInfoLogON:
		return "?"
	case RedactInfoLogMarker:
		return redactInfo(arg)
	default:
	}
	return arg
}

// RedactStringer receives stringer argument and return omitted information if redact log enabled
func RedactStringer(arg fmt.Stringer) fmt.Stringer {
	switch getRedactType() {
	case RedactInfoLogON:
		return &redactedStringer{"?"}
	case RedactInfoLogMarker:
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
