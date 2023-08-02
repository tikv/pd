// Copyright 2019 TiKV Project Authors.
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

package main

import (
	"flag"
	"os"

	"github.com/pingcap/log"
	"github.com/tikv/pd/tools/pd-analysis/analysis"
	"go.uber.org/zap"
)

var (
	input    = flag.String("input", "", "input pd log file, required")
	output   = flag.String("output", "", "output file, default output to stdout")
	logLevel = flag.String("logLevel", "info", "log level, default info")
	style    = flag.String("style", "", "analysis style, e.g. transfer-counter")
	operator = flag.String("operator", "", "operator style, e.g. balance-region, balance-leader, transfer-hot-read-leader, move-hot-read-region, transfer-hot-write-leader, move-hot-write-region")
	start    = flag.String("start", "", "start time, e.g. 2019/09/10 12:20:07, default: total file")
	end      = flag.String("end", "", "end time, e.g. 2019/09/10 14:20:07, default: total file")
)

// Logger is the global logger used for simulator.
var Logger *zap.Logger

// InitLogger initializes the Logger with -log level.
func InitLogger(l string) {
	conf := &log.Config{Level: l, File: log.FileLogConfig{}}
	lg, _, _ := log.InitLogger(conf)
	Logger = lg
}

func main() {
	flag.Parse()
	InitLogger(*logLevel)
	analysis.GetTransferCounter().Init(0, 0)
	if *input == "" {
		Logger.Fatal("need to specify one input pd log")
	}
	if *output != "" {
		f, err := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE|os.O_SYNC|os.O_APPEND, 0600)
		if err != nil {
			Logger.Fatal(err.Error())
		} else {
			os.Stdout = f
		}
	}

	switch *style {
	case "transfer-counter":
		{
			if *operator == "" {
				Logger.Fatal("need to specify one operator")
			}
			r, err := analysis.GetTransferCounter().CompileRegex(*operator)
			if err != nil {
				Logger.Fatal(err.Error())
			}
			err = analysis.GetTransferCounter().ParseLog(*input, *start, *end, analysis.DefaultLayout, r)
			if err != nil {
				Logger.Fatal(err.Error())
			}
			analysis.GetTransferCounter().PrintResult()
			break
		}
	default:
		Logger.Fatal("style is not exist")
	}
}
