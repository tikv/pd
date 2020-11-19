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
	"github.com/tikv/pd/pkg/autoscaling"
	"github.com/tikv/pd/pkg/dashboard"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/pkg/metricutil"
	"github.com/tikv/pd/pkg/swaggerserver"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/join"
	"go.uber.org/zap"

	// Register schedulers.
	_ "github.com/tikv/pd/server/schedulers"
)

func main() {
	cfg := config.NewConfig()
	err := cfg.Parse(os.Args[1:])

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
		log.Fatal("parse cmd flags error", errs.ZapError(errs.ErrParseFlags))
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

	// The old logger
	err = logutil.InitLogger(&cfg.Log)
	if err != nil {
		log.Fatal("initialize logger error", errs.ZapError(err))
	}

	server.LogPDInfo()

	for _, msg := range cfg.WarningMsgs {
		log.Warn(msg)
	}

	// TODO: Make it configurable if it has big impact on performance.
	grpcprometheus.EnableHandlingTimeHistogram()

	metricutil.Push(&cfg.Metric)

	err = join.PrepareJoinCluster(cfg)
	if err != nil {
		log.Fatal("join meet error", errs.ZapError(err))
	}

	// Creates server.
	ctx, cancel := context.WithCancel(context.Background())
	serviceBuilders := []server.HandlerBuilder{api.NewHandler, swaggerserver.NewHandler, autoscaling.NewHandler}
	serviceBuilders = append(serviceBuilders, dashboard.GetServiceBuilders()...)
	svr, err := server.CreateServer(ctx, cfg, serviceBuilders...)
	if err != nil {
		log.Fatal("create server failed", errs.ZapError(err))
	}

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

func exit(code int) {
	log.Sync()
	os.Exit(code)
}
