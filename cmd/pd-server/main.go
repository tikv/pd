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
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/juju/errors"
	"github.com/pingcap/pd/pkg/logutil"
	"github.com/pingcap/pd/pkg/metricutil"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api"
	log "github.com/sirupsen/logrus"

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
		log.Fatalf("parse cmd flags error: %s\n", err)
	}

	dataDir, err := filepath.Abs(cfg.DataDir)
	logFile, err := filepath.Abs(cfg.Log.File.Filename)
	rel, err := filepath.Rel(dataDir, filepath.Dir(logFile))
	if !strings.HasPrefix(rel, "..") {
		log.Fatalf("initialize logger error: log directory shouldn't be the subdirectory of data directory")
	}

	err = logutil.InitLogger(&cfg.Log)
	if err != nil {
		log.Fatalf("initialize logger error: %s\n", err)
	}

	server.LogPDInfo()

	for _, msg := range cfg.WarningMsgs {
		log.Warn(msg)
	}

	// TODO: Make it configurable if it has big impact on performance.
	grpc_prometheus.EnableHandlingTimeHistogram()

	metricutil.Push(&cfg.Metric)

	err = server.PrepareJoinCluster(cfg)
	if err != nil {
		log.Fatal("join error ", errors.ErrorStack(err))
	}
	svr, err := server.CreateServer(cfg, api.NewHandler)
	if err != nil {
		log.Fatalf("create server failed: %v", errors.ErrorStack(err))
	}

	if err = server.InitHTTPClient(svr); err != nil {
		log.Fatalf("initial http client for api handler failed: %v", errors.ErrorStack(err))
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	if err := svr.Run(); err != nil {
		log.Fatalf("run server failed: %v", errors.ErrorStack(err))
	}

	sig := <-sc
	log.Infof("Got signal [%d] to exit.", sig)

	svr.Close()
	switch sig {
	case syscall.SIGTERM:
		os.Exit(0)
	default:
		os.Exit(1)
	}
}
