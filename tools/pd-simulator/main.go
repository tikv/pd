// Copyright 2017 PingCAP, Inc.
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
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/pkg/logutil"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/server/api"
	"github.com/pingcap/pd/v4/server/config"
	"github.com/pingcap/pd/v4/server/statistics"
	"github.com/pingcap/pd/v4/tools/pd-analysis/analysis"
	"github.com/pingcap/pd/v4/tools/pd-simulator/simulator"
	"github.com/pingcap/pd/v4/tools/pd-simulator/simulator/cases"
	"github.com/pingcap/pd/v4/tools/pd-simulator/simulator/simutil"
	"go.uber.org/zap"

	// Register schedulers.
	_ "github.com/pingcap/pd/v4/server/schedulers"
)

var (
	pdAddr                      = flag.String("pd", "", "pd address")
	configFile                  = flag.String("config", "conf/simconfig.toml", "config file")
	caseName                    = flag.String("case", "", "case name")
	serverLogLevel              = flag.String("serverLog", "fatal", "pd server log level.")
	simLogLevel                 = flag.String("simLog", "fatal", "simulator log level.")
	regionNum                   = flag.Int("regionNum", 0, "regionNum of one store")
	storeNum                    = flag.Int("storeNum", 0, "storeNum")
	enableTransferRegionCounter = flag.Bool("enableTransferRegionCounter", false, "enableTransferRegionCounter")
)

func main() {
	flag.Parse()

	simutil.InitLogger(*simLogLevel)
	simutil.InitCaseConfig(*storeNum, *regionNum, *enableTransferRegionCounter)
	statistics.Denoising = false
	if simutil.CaseConfigure.EnableTransferRegionCounter {
		analysis.GetTransferCounter().Init(simutil.CaseConfigure.StoreNum, simutil.CaseConfigure.RegionNum)
	}

	if *caseName == "" {
		if *pdAddr != "" {
			simutil.Logger.Fatal("need to specify one config name")
		}
		for simCase := range cases.CaseMap {
			run(simCase)
		}
	} else {
		run(*caseName)
	}
}

func run(simCase string) {
	simConfig := simulator.NewSimConfig(*serverLogLevel)
	if *configFile != "" {
		if _, err := toml.DecodeFile(*configFile, simConfig); err != nil {
			simutil.Logger.Fatal("failed to decode file ", zap.Error(err))
		}
	}
	if err := simConfig.Adjust(); err != nil {
		simutil.Logger.Fatal("failed to adjust simulator configuration", zap.Error(err))
	}

	if *pdAddr != "" {
		simStart(*pdAddr, simCase, simConfig)
	} else {
		local, clean := NewSingleServer(context.Background(), simConfig)
		err := local.Run()
		if err != nil {
			simutil.Logger.Fatal("run server error", zap.Error(err))
		}
		for {
			if !local.IsClosed() && local.GetMember().IsLeader() {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		simStart(local.GetAddr(), simCase, simConfig, clean)
	}
}

// NewSingleServer creates a pd server for simulator.
func NewSingleServer(ctx context.Context, simConfig *simulator.SimConfig) (*server.Server, server.CleanupFunc) {
	err := simConfig.ServerConfig.SetupLogger()
	if err == nil {
		log.ReplaceGlobals(simConfig.ServerConfig.GetZapLogger(), simConfig.ServerConfig.GetZapLogProperties())
	} else {
		log.Fatal("setup logger error", zap.Error(err))
	}

	err = logutil.InitLogger(&simConfig.ServerConfig.Log)
	if err != nil {
		log.Fatal("initialize logger error", zap.Error(err))
	}

	s, err := server.CreateServer(ctx, simConfig.ServerConfig, api.NewHandler)
	if err != nil {
		panic("create server failed")
	}

	cleanup := func() {
		s.Close()
		cleanServer(simConfig.ServerConfig)
	}
	return s, cleanup
}

func cleanServer(cfg *config.Config) {
	// Clean data directory
	os.RemoveAll(cfg.DataDir)
}

func simStart(pdAddr string, simCase string, simConfig *simulator.SimConfig, clean ...server.CleanupFunc) {
	start := time.Now()
	driver, err := simulator.NewDriver(pdAddr, simCase, simConfig)
	if err != nil {
		simutil.Logger.Fatal("create driver error", zap.Error(err))
	}

	err = driver.Prepare()
	if err != nil {
		simutil.Logger.Fatal("simulator prepare error", zap.Error(err))
	}

	tickInterval := simConfig.SimTickInterval.Duration

	tick := time.NewTicker(tickInterval)
	defer tick.Stop()
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	simResult := "FAIL"

EXIT:
	for {
		select {
		case <-tick.C:
			driver.Tick()
			if driver.Check() {
				simResult = "OK"
				break EXIT
			}
		case <-sc:
			break EXIT
		}
	}

	driver.Stop()
	if len(clean) != 0 {
		clean[0]()
	}

	fmt.Printf("%s [%s] total iteration: %d, time cost: %v\n", simResult, simCase, driver.TickCount(), time.Since(start))
	driver.PrintStatistics()
	if analysis.GetTransferCounter().IsValid {
		analysis.GetTransferCounter().PrintResult()
	}

	if simResult != "OK" {
		os.Exit(1)
	}
}
