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
	"sync"
	"sync/atomic"
	"unsafe"

	"go.uber.org/zap"
)

// PluggableLogger is a pluggable zap.Logger.
type PluggableLogger struct {
	name   string
	logger unsafe.Pointer // zap.Logger
}

// GetName gets the pluggable logger's name.
func (l *PluggableLogger) GetName() string {
	return l.name
}

// GetLogger gets the logger. If it is nil,
// it means that the current related log does not need to be output.
func (l *PluggableLogger) GetLogger() *zap.Logger {
	logger := (*zap.Logger)(atomic.LoadPointer(&l.logger))
	if logger == nil {
		return nil
	}
	return logger.WithOptions(zap.AddCallerSkip(1))
}

// PlugLogger plugs the logger. If there is other logger, it will be replaced.
func (l *PluggableLogger) PlugLogger(logger *zap.Logger) {
	atomic.StorePointer(&l.logger, unsafe.Pointer(logger))
}

// UnplugLogger unplugs the logger. If it has been replaced, nothing will be done.
func (l *PluggableLogger) UnplugLogger(logger *zap.Logger) {
	atomic.CompareAndSwapPointer(&l.logger, unsafe.Pointer(logger), nil)
}

// Debug logs a message at DebugLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func (l *PluggableLogger) Debug(msg string, fields ...zap.Field) {
	if logger := l.GetLogger(); logger != nil {
		logger.Debug(msg, fields...)
	}
}

// Info logs a message at InfoLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func (l *PluggableLogger) Info(msg string, fields ...zap.Field) {
	if logger := l.GetLogger(); logger != nil {
		logger.Info(msg, fields...)
	}
}

// Warn logs a message at WarnLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func (l *PluggableLogger) Warn(msg string, fields ...zap.Field) {
	if logger := l.GetLogger(); logger != nil {
		logger.Warn(msg, fields...)
	}
}

// Error logs a message at ErrorLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func (l *PluggableLogger) Error(msg string, fields ...zap.Field) {
	if logger := l.GetLogger(); logger != nil {
		logger.Error(msg, fields...)
	}
}

// Panic logs a message at PanicLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
//
// The logger then panics, even if logging at PanicLevel is disabled.
func (l *PluggableLogger) Panic(msg string, fields ...zap.Field) {
	if logger := l.GetLogger(); logger != nil {
		logger.Panic(msg, fields...)
	}
}

// Fatal logs a message at FatalLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
//
// The logger then calls os.Exit(1), even if logging at FatalLevel is
// disabled.
func (l *PluggableLogger) Fatal(msg string, fields ...zap.Field) {
	if logger := l.GetLogger(); logger != nil {
		logger.Fatal(msg, fields...)
	}
}

type loggerManager struct {
	sync.Mutex
	Loggers map[string]*PluggableLogger
}

func (m *loggerManager) Init() {
	m.Lock()
	defer m.Unlock()
	m.Loggers = make(map[string]*PluggableLogger)
}

// GetPluggableLogger gets a pluggable zap.Logger.
func (m *loggerManager) GetPluggableLogger(name string, createIfNotExist bool) *PluggableLogger {
	m.Lock()
	defer m.Unlock()
	l, ok := m.Loggers[name]
	if !ok {
		if !createIfNotExist {
			return nil
		}
		l = &PluggableLogger{name: name}
		m.Loggers[name] = l
	}
	return l
}

var globalLoggerManager loggerManager

func init() {
	globalLoggerManager.Init()
}

// GetPluggableLogger gets a pluggable zap.Logger.
func GetPluggableLogger(name string, createIfNotExist bool) *PluggableLogger {
	return globalLoggerManager.GetPluggableLogger(name, createIfNotExist)
}
