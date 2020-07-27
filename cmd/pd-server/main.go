// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/pkg/dashboard"
	errs "github.com/pingcap/pd/v4/pkg/errors"
	"github.com/pingcap/pd/v4/pkg/logutil"
	"github.com/pingcap/pd/v4/pkg/metricutil"
	"github.com/pingcap/pd/v4/pkg/swaggerserver"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/server/api"
	"github.com/pingcap/pd/v4/server/config"
	"github.com/pingcap/pd/v4/server/join"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	// Register schedulers.
	_ "github.com/pingcap/pd/v4/server/schedulers"
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
		log.Fatal(errs.ErrFormatParseCmd.MessageTemplate(), zap.Error(errs.ErrFormatParseCmd))
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
		log.Fatal(errs.ErrOtherInitLog.MessageTemplate(), zap.Error(errs.ErrOtherInitLog))
	}
	// Flushing any buffered log entries
	defer log.Sync()

	// The old logger
	err = logutil.InitLogger(&cfg.Log)
	if err != nil {
		log.Fatal(errs.ErrOtherInitLog.MessageTemplate(), zap.Error(errs.ErrOtherInitLog))
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
		log.Fatal(errs.ErrIOJoinConfig.MessageTemplate(), zap.Error(errs.ErrIOJoinConfig))
	}

	// Creates server.
	ctx, cancel := context.WithCancel(context.Background())
	serviceBuilders := []server.HandlerBuilder{api.NewHandler, swaggerserver.NewHandler}
	serviceBuilders = append(serviceBuilders, dashboard.GetServiceBuilders()...)
	svr, err := server.CreateServer(ctx, cfg, serviceBuilders...)
	if err != nil {
		log.Fatal(errs.ErrFormatParseURL.MessageTemplate(), zap.Error(errs.ErrFormatParseURL))
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
		log.Fatal(errs.ErrStorageEtcdLoad.MessageTemplate(), zap.Error(errs.ErrStorageEtcdLoad))
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
