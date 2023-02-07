// Copyright 2016 TiKV Project Authors.
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
	"flag"
	"os"
	"os/signal"
	"syscall"

	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	basicsvr "github.com/tikv/pd/pkg/basic_server"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mode"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"go.uber.org/zap"
)

func main() {
	ctx, cancel, svr := createServerWrapper(os.Args[1:])

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

	if err := svr.Run(); err != nil {
		log.Fatal("run server failed", errs.ZapError(err))
	}

	<-ctx.Done()
	log.Info("Got signal to exit", zap.String("signal", sig.String()))

	svr.Close()
	switch sig {
	case syscall.SIGTERM:
		exit(0)
	default:
		exit(1)
	}
}

func createServerWrapper(args []string) (context.Context, context.CancelFunc, basicsvr.Server) {
	cfg := config.NewConfig()
	err := cfg.Parse(args)

	if cfg.Version {
		server.PrintPDInfo()
		exit(0)
	}

	defer logutil.LogPanic()

	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		exit(0)
	default:
		log.Fatal("parse cmd flags error", errs.ZapError(err))
	}

	if cfg.ConfigCheck {
		server.PrintConfigCheckMsg(cfg)
		exit(0)
	}

	// New zap logger
	err = cfg.SetupLogger()
	if err == nil {
		log.ReplaceGlobals(cfg.GetZapLogger(), cfg.GetZapLogProperties())
	} else {
		log.Fatal("initialize logger error", errs.ZapError(err))
	}
	// Flushing any buffered log entries
	defer log.Sync()

	server.LogPDInfo()

	for _, msg := range cfg.WarningMsgs {
		log.Warn(msg)
	}

	// TODO: Make it configurable if it has big impact on performance.
	grpcprometheus.EnableHandlingTimeHistogram()

	metricutil.Push(&cfg.Metric)

	// todo: add more service mode and multi service mode
	ctx, cancel := context.WithCancel(context.Background())
	m, err := config.ParseServiceMode(cfg.ServiceModes)
	if err != nil {
		log.Fatal("parse service mode error", errs.ZapError(err))
	}
	switch m {
	case config.APIService:
		log.Info("start pd api service, no implementation yet")
	case config.SchedulerService:
		log.Info("start pd scheduler service, no implementation yet")
	case config.TSOService:
		log.Info("start pd tso service, no implementation yet")
	case config.ResourceManagerService:
		return ctx, cancel, mode.ResourceManagerStart(ctx, cfg)
	default: // case all
		return ctx, cancel, mode.LegacyStart(ctx, cfg)
	}
	return ctx, cancel, nil
}

func exit(code int) {
	log.Sync()
	os.Exit(code)
}
