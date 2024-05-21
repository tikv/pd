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

package analysis

import (
	"flag"
	"os"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/tools/pd-dev/pd-analysis/analysis"
	"go.uber.org/zap"
)

func Run() {
	cfg := newConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Fatal("parse cmd flags error", zap.Error(err))
	}

	if cfg.Input == "" {
		log.Fatal("need to specify one input pd log")
	}
	analysis.GetTransferCounter().Init(0, 0)
	if cfg.Output != "" {
		f, err := os.OpenFile(cfg.Output, os.O_WRONLY|os.O_CREATE|os.O_SYNC|os.O_APPEND, 0600)
		if err != nil {
			log.Fatal(err.Error())
		}
		os.Stdout = f
	}

	switch cfg.Style {
	case "transfer-counter":
		{
			if cfg.Operator == "" {
				log.Fatal("need to specify one operator")
			}
			r, err := analysis.GetTransferCounter().CompileRegex(cfg.Operator)
			if err != nil {
				log.Fatal(err.Error())
			}
			err = analysis.GetTransferCounter().ParseLog(cfg.Input, cfg.Start, cfg.End, analysis.DefaultLayout, r)
			if err != nil {
				log.Fatal(err.Error())
			}
			analysis.GetTransferCounter().PrintResult()
			break
		}
	default:
		log.Fatal("style is not exist")
	}
}
