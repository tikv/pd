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

	"github.com/coreos/pkg/capnslog"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api"
)

func main() {
	cfg := server.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Errorf("parse cmd flags err %s\n", err)
		os.Exit(2)
	}

	log.SetLevelByString(cfg.LogLevel)
	log.SetHighlighting(false)
	// Output log to a log file.
	if len(cfg.LogFile) > 0 {
		logFile, err := os.OpenFile(cfg.LogFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
		if err != nil {
			log.Errorf("open log file err %s\n", err)
		}

		log.SetOutput(logFile)
		log.SetRotateByDay()

		// Hijack etcd log.
		f := capnslog.NewDefaultFormatter(logFile)
		capnslog.SetFormatter(f)
	}

	svr, err := server.CreateServer(cfg)
	if err != nil {
		log.Errorf("create pd server err %s\n", err)
		return
	}
	err = svr.StartEtcd(api.NewHandler(svr))
	if err != nil {
		log.Errorf("server start etcd failed - %v", errors.Trace(err))
		return
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		log.Infof("Got signal [%d] to exit.", sig)
		svr.Close()
		os.Exit(0)
	}()

	svr.Run()
}
