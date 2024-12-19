// Copyright 2017 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	flag "github.com/spf13/pflag"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tools/pd-analysis/analysis"
	"github.com/tikv/pd/tools/pd-simulator/simulator"
	"github.com/tikv/pd/tools/pd-simulator/simulator/cases"
	sc "github.com/tikv/pd/tools/pd-simulator/simulator/config"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
)

var (
	pdAddr         = flag.String("pd-endpoints", "", "pd address")
	configFile     = flag.String("config", "conf/simconfig.toml", "config file")
	caseName       = flag.String("case", "", "case name")
	serverLogLevel = flag.String("serverLog", "info", "pd server log level")
	simLogLevel    = flag.String("simLog", "info", "simulator log level")
	simLogFile     = flag.String("log-file", "", "simulator log file")
	statusAddress  = flag.String("status-addr", "0.0.0.0:20180", "status address")
)

func main() {
	// wait PD start. Otherwise, it will happen error when getting cluster ID.
	time.Sleep(3 * time.Second)
	// ignore some undefined flag
	flag.CommandLine.ParseErrorsWhitelist.UnknownFlags = true
	flag.Parse()

	simutil.InitLogger(*simLogLevel, *simLogFile)
	statistics.Denoising = false
	schedulers.Register() // register schedulers, which is needed by simConfig.Adjust
	simConfig := sc.NewSimConfig(*serverLogLevel)
	if simConfig.EnableTransferRegionCounter {
		analysis.GetTransferCounter().Init(simConfig.TotalStore, simConfig.TotalRegion)
	}
	var meta toml.MetaData
	var err error
	if *configFile != "" {
		if meta, err = toml.DecodeFile(*configFile, simConfig); err != nil {
			simutil.Logger.Fatal("failed to decode config file, please check the path of the config file",
				zap.Error(err), zap.String("config-file", *configFile))
		}
	}
	if err = simConfig.Adjust(&meta); err != nil {
		simutil.Logger.Fatal("failed to adjust simulator configuration", zap.Error(err))
	}
	if len(*caseName) == 0 {
		*caseName = simConfig.CaseName
	}

	if *caseName == "" {
		if *pdAddr != "" {
			simutil.Logger.Fatal("need to specify one config name")
		}
		for simCase := range cases.CaseMap {
			run(simCase, simConfig)
		}
	} else {
		run(*caseName, simConfig)
	}
}

func run(simCase string, simConfig *sc.SimConfig) {
	if *pdAddr != "" {
		simStart(*pdAddr, *statusAddress, simCase, simConfig)
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
		simStart(local.GetAddr(), "", simCase, simConfig, clean)
	}
}

// NewSingleServer creates a pd server for simulator.
func NewSingleServer(ctx context.Context, simConfig *sc.SimConfig) (*server.Server, testutil.CleanupFunc) {
	err := logutil.SetupLogger(simConfig.ServerConfig.Log, &simConfig.ServerConfig.Logger, &simConfig.ServerConfig.LogProps, simConfig.ServerConfig.Security.RedactInfoLog)
	if err == nil {
		log.ReplaceGlobals(simConfig.ServerConfig.Logger, simConfig.ServerConfig.LogProps)
	} else {
		log.Fatal("setup logger error", zap.Error(err))
	}

	s, err := server.CreateServer(ctx, simConfig.ServerConfig, nil, api.NewHandler)
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

func simStart(pdAddr, statusAddress string, simCase string, simConfig *sc.SimConfig, clean ...testutil.CleanupFunc) {
	start := time.Now()
	driver, err := simulator.NewDriver(pdAddr, statusAddress, simCase, simConfig)
	if err != nil {
		simutil.Logger.Fatal("create driver error", zap.Error(err))
	}

	err = driver.Prepare()
	if err != nil {
		simutil.Logger.Fatal("simulator prepare error", zap.Error(err))
	}
	tickInterval := simConfig.SimTickInterval.Duration

	ctx, cancel := context.WithCancel(context.Background())
	tick := time.NewTicker(tickInterval)
	defer tick.Stop()
	sc := make(chan os.Signal, 1)
	// halt scheduling
	simulator.ChooseToHaltPDSchedule(true)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	simResult := "FAIL"

	go driver.StoresHeartbeat(ctx)
	go driver.RegionsHeartbeat(ctx)
	go driver.StepRegions(ctx)

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

	cancel()
	driver.Stop()
	if len(clean) != 0 && clean[0] != nil {
		clean[0]()
	}

	fmt.Printf("%s [%s] total iteration: %d, time cost: %v\n", simResult, simCase, driver.TickCount(), time.Since(start))
	if analysis.GetTransferCounter().IsValid {
		analysis.GetTransferCounter().PrintResult()
	}

	if simulator.PDHTTPClient != nil {
		simulator.PDHTTPClient.Close()
		simulator.SD.Close()
	}
	if simResult != "OK" {
		os.Exit(1)
	}
}
