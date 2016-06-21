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
	"strings"
	"syscall"

	"github.com/ngaut/log"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api"
)

var (
	addr            = flag.String("addr", "127.0.0.1:1234", "server listening address")
	advertiseAddr   = flag.String("advertise-addr", "", "server advertise listening address [127.0.0.1:1234] for client communication")
	etcdAddrs       = flag.String("etcd", "127.0.0.1:2379", "Etcd endpoints, separated by comma")
	rootPath        = flag.String("root", "/pd", "pd root path in etcd")
	leaderLease     = flag.Int64("lease", 3, "leader lease time (second)")
	logLevel        = flag.String("L", "debug", "log level: info, debug, warn, error, fatal")
	httpAddr        = flag.String("httpAddr", ":9090", "http server listening address")
	pprofAddr       = flag.String("pprof", ":6060", "pprof HTTP listening address")
	clusterID       = flag.Uint64("cluster-id", 0, "cluster ID")
	maxPeerCount    = flag.Uint("max-peer-count", 3, "max peer count for the region")
	metricAddr      = flag.String("metric-addr", "", "StatsD metric address")
	metricPrefix    = flag.String("metric-prefix", "pd", "metric prefix")
	minCapUsedRatio = flag.Float64("min-capacity-used-ratio", 0.4, "min capacity used ratio for choosing store in balance")
	maxCapUsedRatio = flag.Float64("max-capacity-used-ratio", 0.9, "max capacity used ratio for choosing store in balance")
)

func main() {
	flag.Parse()

	if *clusterID == 0 {
		log.Warn("cluster id is 0, don't use it in production")
	}

	log.SetLevelByString(*logLevel)

	go func() {
		http.ListenAndServe(*pprofAddr, nil)
	}()

	cfg := &server.Config{
		Addr:                 *addr,
		HTTPAddr:             *httpAddr,
		AdvertiseAddr:        *advertiseAddr,
		EtcdAddrs:            strings.Split(*etcdAddrs, ","),
		RootPath:             *rootPath,
		LeaderLease:          *leaderLease,
		ClusterID:            *clusterID,
		MaxPeerCount:         uint32(*maxPeerCount),
		MetricAddr:           *metricAddr,
		MetricPrefix:         *metricPrefix,
		MinCapacityUsedRatio: *minCapUsedRatio,
		MaxCapacityUsedRatio: *maxCapUsedRatio,
	}

	go func() {
		api.ServeHTTP(cfg.HTTPAddr)
	}()

	var err error
	server.PdServer, err = server.NewServer(cfg)
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
		server.PdServer.Close()
		os.Exit(0)
	}()

	server.PdServer.Run()
}
