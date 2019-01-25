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
	"fmt"
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
		log.S().Fatalf("parse cmd flags error: %s\n", fmt.Sprintf("%+v", err))
	}
	// New zap logger
	err = cfg.SetupLogger()
	if err != nil {
		log.S().Fatalf("initialize logger error: %s\n", fmt.Sprintf("%+v", err))
	}

	// The old logger
	err = logutil.InitLogger(&cfg.Log)
	if err != nil {
		log.S().Fatalf("initialize logger error: %s\n", fmt.Sprintf("%+v", err))
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
		log.S().Fatal("join error ", fmt.Sprintf("%+v", err))
	}
	svr, err := server.CreateServer(cfg, api.NewHandler)
	if err != nil {
		log.S().Fatalf("create server failed: %v", fmt.Sprintf("%+v", err))
	}

	if err = server.InitHTTPClient(svr); err != nil {
		log.S().Fatalf("initial http client for api handler failed: %v", fmt.Sprintf("%+v", err))
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
		log.S().Fatalf("run server failed: %v", fmt.Sprintf("%+v", err))
	}

	<-ctx.Done()
	log.S().Infof("Got signal [%d] to exit.", sig)

	svr.Close()
	switch sig {
	case syscall.SIGTERM:
		os.Exit(0)
	default:
		os.Exit(1)
	}
}
