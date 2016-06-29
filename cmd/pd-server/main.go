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
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api"
)

var (
	config                 = flag.String("c", "", "config file")
	addr                   = flag.String("addr", "127.0.0.1:1234", "server listening address")
	advertiseAddr          = flag.String("advertise-addr", "", "server advertise listening address [127.0.0.1:1234] for client communication")
	etcdAddrs              = flag.String("etcd-addrs", "127.0.0.1:2379", "Etcd endpoints, separated by comma")
	httpAddr               = flag.String("http-addr", ":9090", "http server listening address")
	pprofAddr              = flag.String("pprof-addr", ":6060", "pprof HTTP listening address")
	rootPath               = flag.String("root", "/pd", "pd root path in etcd")
	leaderLease            = flag.Int64("lease", 3, "leader lease time (second)")
	logLevel               = flag.String("log-level", "debug", "log level: info, debug, warn, error, fatal")
	tsoSaveInterval        = flag.Int64("tso-save-interval", 2000, "the interval time (ms) to save timestamp")
	clusterID              = flag.Uint64("cluster-id", 0, "cluster ID")
	maxPeerCount           = flag.Uint64("max-peer-count", 3, "max peer count for the region")
	metricAddr             = flag.String("metric-addr", "", "StatsD metric address")
	metricPrefix           = flag.String("metric-prefix", "pd", "metric prefix")
	minCapUsedRatio        = flag.Float64("min-capacity-used-ratio", 0.4, "min capacity used ratio for choosing store in balance")
	maxCapUsedRatio        = flag.Float64("max-capacity-used-ratio", 0.9, "max capacity used ratio for choosing store in balance")
	maxSnapSendCount       = flag.Uint64("max-snap-sending-count", 3, "max sending snapshot count for choosing store in balance")
	maxSnapRecvCount       = flag.Uint64("max-snap-receiving-count", 3, "max receiving snapshot count for choosing store in balance")
	maxDiffScoreFrac       = flag.Float64("max-diff-score-fraction", 0.1, "max diff score fraction for choosing store in balance")
	balanceInterval        = flag.Uint64("balance-interval", 30, "the interval time (s) to do balance")
	maxBalanceCount        = flag.Uint64("max-balance-count", 16, "the max region count to balance at the same time")
	maxBalanceRetryPerLoop = flag.Uint64("max-balance-retry-per-loop", 10, "the max retry count to balance in a balance schedule")
	maxBalanceCountPerLoop = flag.Uint64("max-balance-count-per-loop", 3, "the max region count to balance in a balance schedule")
)

func main() {
	flag.Parse()

	cfg := &server.Config{}
	flag.Visit(cfg.VisitArgs)

	if *config != "" {
		if err := cfg.LoadFromFile(*config); err != nil {
			log.Fatalf("load config failed - %s", err)
		}

		log.Infof("PD init config - %v", cfg)
	}

	cfg.SetStringFlagConfig(&cfg.Addr, "addr", *addr)
	cfg.SetStringFlagConfig(&cfg.AdvertiseAddr, "advertise-addr", *advertiseAddr)
	cfg.SetStringSliceFlagConfig(&cfg.EtcdAddrs, "etcd", *etcdAddrs)
	cfg.SetStringFlagConfig(&cfg.HTTPAddr, "http-addr", *httpAddr)
	cfg.SetStringFlagConfig(&cfg.PprofAddr, "pprof", *pprofAddr)
	cfg.SetStringFlagConfig(&cfg.RootPath, "root", *rootPath)
	cfg.SetIntFlagConfig(&cfg.LeaderLease, "lease", *leaderLease)
	cfg.SetStringFlagConfig(&cfg.LogLevel, "L", *logLevel)
	cfg.SetIntFlagConfig(&cfg.TsoSaveInterval, "tso-save-interval", *tsoSaveInterval)
	cfg.SetUintFlagConfig(&cfg.ClusterID, "cluster-id", *clusterID)
	cfg.SetUintFlagConfig(&cfg.MaxPeerCount, "max-peer-count", *maxPeerCount)
	cfg.SetStringFlagConfig(&cfg.MetricAddr, "metric-addr", *metricAddr)
	cfg.SetFloatFlagConfig(&cfg.BCfg.MinCapacityUsedRatio, "min-capacity-used-ratio", *minCapUsedRatio)
	cfg.SetFloatFlagConfig(&cfg.BCfg.MaxCapacityUsedRatio, "max-capacity-used-ratio", *maxCapUsedRatio)
	cfg.SetUintFlagConfig(&cfg.BCfg.MaxSnapSendingCount, "max-snap-sending-count", *maxSnapSendCount)
	cfg.SetUintFlagConfig(&cfg.BCfg.MaxSnapReceivingCount, "max-snap-receiving-count", *maxSnapRecvCount)
	cfg.SetFloatFlagConfig(&cfg.BCfg.MaxDiffScoreFraction, "max-diff-score-fraction", *maxDiffScoreFrac)
	cfg.SetUintFlagConfig(&cfg.BCfg.BalanceInterval, "balance-interval", *balanceInterval)
	cfg.SetUintFlagConfig(&cfg.BCfg.MaxBalanceCount, "max-balance-count", *maxBalanceCount)
	cfg.SetUintFlagConfig(&cfg.BCfg.MaxBalanceRetryPerLoop, "max-balance-retry-per-loop", *maxBalanceRetryPerLoop)
	cfg.SetUintFlagConfig(&cfg.BCfg.MaxBalanceCountPerLoop, "max-balance-count-per-loop", *maxBalanceCountPerLoop)

	log.SetLevelByString(cfg.LogLevel)

	log.Infof("PD config - %v", cfg)

	go func() {
		http.ListenAndServe(cfg.PprofAddr, nil)
	}()

	svr, err := server.NewServer(cfg)
	if err != nil {
		log.Errorf("create pd server err %s\n", err)
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

	go func() {
		err = api.ServeHTTP(cfg.HTTPAddr, svr)
		if err != nil {
			log.Fatalf("serve http failed - %v", errors.Trace(err))
		}
	}()

	err = svr.Run()
	if err != nil {
		log.Fatalf("server run failed - %v", errors.Trace(err))
	}
}
