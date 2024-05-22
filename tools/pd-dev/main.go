// Copyright 2024 TiKV Project Authors.
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
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/log"
	flag "github.com/spf13/pflag"
	analysis "github.com/tikv/pd/tools/pd-dev/pd-analysis"
	api "github.com/tikv/pd/tools/pd-dev/pd-api-bench"
	backup "github.com/tikv/pd/tools/pd-dev/pd-backup"
	heartbeat "github.com/tikv/pd/tools/pd-dev/pd-heartbeat-bench"
	simulator "github.com/tikv/pd/tools/pd-dev/pd-simulator"
	tso "github.com/tikv/pd/tools/pd-dev/pd-tso-bench"
	ut "github.com/tikv/pd/tools/pd-dev/pd-ut"
	region "github.com/tikv/pd/tools/pd-dev/regions-dump"
	store "github.com/tikv/pd/tools/pd-dev/stores-dump"
	"go.uber.org/zap"
)

func switchDevelopmentMode(ctx context.Context, mode string) {
	log.Info("pd-dev start", zap.String("mode", mode))
	switch mode {
	case "api":
		api.Run(ctx)
	case "heartbeat":
		heartbeat.Run(ctx)
	case "tso":
		tso.Run(ctx)
	case "simulator":
		simulator.Run(ctx)
	case "regions-dump":
		region.Run()
	case "stores-dump":
		store.Run()
	case "analysis":
		analysis.Run()
	case "backup":
		backup.Run()
	case "ut":
		ut.Run()
	default:
		log.Fatal("please input a mode to run, or provide a config file to run all modes")
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	var sig os.Signal
	go func() {
		sig = <-sc
		cancel()
	}()

	// parse first argument
	if len(os.Args) < 2 {
		log.Fatal("please input a mode to run, or provide a config file to run all modes")
	}

	var mode string
	fs := flag.NewFlagSet("pd-dev", flag.ContinueOnError)
	fs.StringVar(&mode, "mode", "", "mode to run")
	fs.Parse(os.Args[1:])
	switchDevelopmentMode(ctx, mode)

	log.Info("pd-dev Exit")
	switch sig {
	case syscall.SIGTERM:
		exit(0)
	default:
		exit(1)
	}
}

func exit(code int) {
	os.Exit(code)
}
