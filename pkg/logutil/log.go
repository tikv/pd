// Copyright 2017 PingCAP, Inc.
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
	"os"
	"path"
	"runtime"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/coreos/pkg/capnslog"
	"github.com/juju/errors"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

const (
	defaultLogTimeFormat = "2006/01/02 15:04:05"
	defaultLogMaxSize    = 300 // MB
	defaultLogMaxBackups = 3
	defaultLogMaxAge     = 28 // days
	defaultLogLevel      = log.InfoLevel

	logDirMode = 0755
)

// FileLogConfig serializes file log related config in toml/json.
type FileLogConfig struct {
	Filename string `toml:"filename" json:"filename"`
}

// LogConfig serializes log related config in toml/json.
type LogConfig struct {
	// Log level.
	Level string `toml:"level" json:"level"`
	// Log file.
	File FileLogConfig `toml:"file" json:"file"`
}

// redirectFormatter will redirect etcd logs to logrus logs.
type redirectFormatter struct{}

// Format implements capnslog.Formatter hook.
func (rf *redirectFormatter) Format(pkg string, level capnslog.LogLevel, depth int, entries ...interface{}) {
	if pkg != "" {
		pkg = fmt.Sprint(pkg, ": ")
	}

	logStr := fmt.Sprint(pkg, entries)

	switch level {
	case capnslog.CRITICAL:
		log.Fatalf(logStr)
	case capnslog.ERROR:
		log.Errorf(logStr)
	case capnslog.WARNING:
		log.Warningf(logStr)
	case capnslog.NOTICE:
		log.Infof(logStr)
	case capnslog.INFO:
		log.Infof(logStr)
	case capnslog.DEBUG, capnslog.TRACE:
		log.Debugf(logStr)
	}
}

// Flush only for implementing Formatter.
func (rf *redirectFormatter) Flush() {}

// isSKippedPackageName tests wether path name is on log library calling stack.
func isSkippedPackageName(name string) bool {
	return strings.Contains(name, "github.com/Sirupsen/logrus") ||
		strings.Contains(name, "github.com/coreos/pkg/capnslog")
}

// modifyHook injects file name and line pos into log entry.
type contextHook struct{}

// Fire implements logrus.Hook interface
// https://github.com/sirupsen/logrus/issues/63
func (hook *contextHook) Fire(entry *log.Entry) error {
	pc := make([]uintptr, 3)
	cnt := runtime.Callers(6, pc)

	for i := 0; i < cnt; i++ {
		fu := runtime.FuncForPC(pc[i] - 1)
		name := fu.Name()
		if !isSkippedPackageName(name) {
			file, line := fu.FileLine(pc[i] - 1)
			entry.Data["file"] = path.Base(file)
			entry.Data["line"] = line
			break
		}
	}
	return nil
}

// Levels implements logrus.Hook interface.
func (hook *contextHook) Levels() []log.Level {
	return log.AllLevels
}

func stringToLogLevel(level string) log.Level {
	switch strings.ToLower(level) {
	case "fatal":
		return log.FatalLevel
	case "error":
		return log.ErrorLevel
	case "warn", "warning":
		return log.WarnLevel
	case "debug":
		return log.DebugLevel
	case "info":
		return log.InfoLevel
	}
	return defaultLogLevel
}

// textFormatter is for compatability with ngaut/log
type textFormatter struct{}

// Format implements logrus.Formatter
func (f *textFormatter) Format(entry *log.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}
	b.WriteString(entry.Time.Format(defaultLogTimeFormat))
	if file, ok := entry.Data["file"]; ok {
		fmt.Fprintf(b, " %s:%v:", file, entry.Data["line"])
	}
	fmt.Fprintf(b, " [%s] %s", entry.Level.String(), entry.Message)
	for k, v := range entry.Data {
		if k != "file" && k != "line" {
			fmt.Fprintf(b, " %v=%v", k, v)
		}
	}
	b.WriteByte('\n')
	return b.Bytes(), nil
}

// setLogOutput sets output path for all logs.
func setLogOutput(filename string) error {
	// PD log
	if st, err := os.Stat(filename); err == nil {
		if st.IsDir() {
			return errors.New("can't use directory as log file name")
		}
	}
	dir := path.Dir(filename)
	err := os.MkdirAll(dir, logDirMode)
	if err != nil {
		return errors.Trace(err)
	}

	// use lumberjack to logrotate
	output := &lumberjack.Logger{
		Filename:   filename,
		MaxSize:    defaultLogMaxSize, // megabytes
		MaxBackups: defaultLogMaxBackups,
		MaxAge:     defaultLogMaxAge, // days
		LocalTime:  true,
	}

	if _, err := output.Write([]byte{}); err != nil {
		return errors.Errorf("log file is not writable: %v", err)
	}

	log.SetOutput(output)
	return nil
}

// InitLogger initalizes PD's logger.
func InitLogger(cfg *LogConfig) error {
	log.SetLevel(stringToLogLevel(cfg.Level))
	log.AddHook(&contextHook{})
	log.SetFormatter(&textFormatter{})

	// etcd log
	capnslog.SetFormatter(&redirectFormatter{})

	if len(cfg.File.Filename) == 0 {
		return nil
	}

	err := setLogOutput(cfg.File.Filename)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}
