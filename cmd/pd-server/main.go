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

	"github.com/grpc-ecosystem/go-grpc-prometheus"
	log "github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/logutil"
	"github.com/pingcap/pd/pkg/metricutil"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	// Register schedulers.
	_ "github.com/pingcap/pd/server/schedulers"
	// Register namespace classifiers.
	_ "github.com/pingcap/pd/table"
)

func main() {
	cfg := server.NewConfig()
	err := cfg.Parse(os.Args[1:])

	if cfg.Version {
		server.PrintPDInfo()
		os.Exit(0)
	}

	defer logutil.LogPanic()

	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.L().Fatal("parse cmd flags error", zap.Error(err))
	}
	// New zap logger
	err = cfg.SetupLogger()
	if err != nil {
		log.L().Fatal("initialize logger error", zap.Error(err))
	}
	// Flushing any buffered log entries
	defer func() {
		log.S().Sync()
		log.L().Sync()
	}()

	// The old logger
	err = logutil.InitLogger(&cfg.Log)
	if err != nil {
		log.L().Fatal("initialize logger error", zap.Error(err))
	}

	server.LogPDInfo()

	for _, msg := range cfg.WarningMsgs {
		log.L().Warn(msg)
	}

	// TODO: Make it configurable if it has big impact on performance.
	grpc_prometheus.EnableHandlingTimeHistogram()

	metricutil.Push(&cfg.Metric)

	err = server.PrepareJoinCluster(cfg)
	if err != nil {
		log.L().Fatal("join meet error", zap.Error(err))
	}
	svr, err := server.CreateServer(cfg, api.NewHandler)
	if err != nil {
		log.L().Fatal("create server failed", zap.Error(err))
	}

	if err = server.InitHTTPClient(svr); err != nil {
		log.L().Fatal("initial http client for api handler failed", zap.Error(err))
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	ctx, cancel := context.WithCancel(context.Background())
	var sig os.Signal
	go func() {
		sig = <-sc
		cancel()
	}()

	if err := svr.Run(ctx); err != nil {
		log.L().Fatal("run server failed", zap.Error(err))
	}

	<-ctx.Done()
	log.L().Info("Got signal to exit", zap.String("signal", sig.String()))

	svr.Close()
	switch sig {
	case syscall.SIGTERM:
		os.Exit(0)
	default:
		os.Exit(1)
	}
}
