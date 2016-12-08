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
	"syscall"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/pd/pkg/metricutil"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api"
)

func main() {
	cfg := server.NewConfig()
	err := cfg.Parse(os.Args[1:])

	if cfg.Version {
		server.PrintPDInfo()
		os.Exit(0)
	}

	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Fatalf("parse cmd flags err %s\n", err)
	}

	err = server.InitLogger(cfg)
	if err != nil {
		log.Fatalf("initalize logger err %s\n", err)
	}

	server.LogPDInfo()

	metricutil.Push(&cfg.MetricCfg)

	svr, err := server.CreateServer(cfg)
	if err != nil {
		log.Fatalf("create pd server err %s\n", err)
	}
	err = svr.StartEtcd(api.NewHandler(svr))
	if err != nil {
		log.Fatalf("server start etcd failed - %v", errors.Trace(err))
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go svr.Run()

	sig := <-sc
	svr.Close()
	log.Infof("Got signal [%d] to exit.", sig)
	switch sig {
	case syscall.SIGTERM:
		os.Exit(0)
	default:
		os.Exit(1)
	}
}
