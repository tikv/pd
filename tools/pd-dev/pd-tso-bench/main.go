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

package tsobench

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"time"

	"github.com/influxdata/tdigest"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/pflag"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/utils/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	keepaliveTime    = 10 * time.Second
	keepaliveTimeout = 3 * time.Second
)

var (
	wg         sync.WaitGroup
	promServer *httptest.Server
)

func collectMetrics(server *httptest.Server) string {
	time.Sleep(1100 * time.Millisecond)
	res, _ := http.Get(server.URL)
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	return string(body)
}

func Run(ctx context.Context) {
	defer logutil.LogPanic()

	cfg := newConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case pflag.ErrHelp:
		os.Exit(0)
	default:
		log.Fatal("parse cmd flags error", zap.Error(err))
	}

	for i := 0; i < cfg.Count; i++ {
		fmt.Printf("\nStart benchmark #%d, duration: %+vs\n", i, cfg.Duration.Seconds())
		bench(ctx, cfg)
	}
}

func bench(mainCtx context.Context, cfg *config) {
	promServer = httptest.NewServer(promhttp.Handler())

	// Initialize all clients
	fmt.Printf("Create %d client(s) for benchmark\n", cfg.Client)
	pdClients := make([]pd.Client, cfg.Client)
	for idx := range pdClients {
		pdCli, err := createPDClient(mainCtx, cfg)
		if err != nil {
			log.Fatal(fmt.Sprintf("create pd client #%d failed: %v", idx, err))
		}
		pdClients[idx] = pdCli
	}

	ctx, cancel := context.WithCancel(mainCtx)
	// To avoid the first time high latency.
	for idx, pdCli := range pdClients {
		_, _, err := pdCli.GetLocalTS(ctx, cfg.DcLocation)
		if err != nil {
			log.Fatal("get first time tso failed", zap.Int("client-number", idx), zap.Error(err))
		}
	}

	durCh := make(chan time.Duration, 2*(cfg.Concurrency)*(cfg.Client))

	if cfg.EnableFaultInjection {
		fmt.Printf("Enable fault injection, failure rate: %f\n", cfg.FaultInjectionRate)
		wg.Add(cfg.Client)
		for i := 0; i < cfg.Client; i++ {
			go reqWorker(ctx, cfg, pdClients, i, durCh)
		}
	} else {
		wg.Add(cfg.Concurrency * cfg.Client)
		for i := 0; i < cfg.Client; i++ {
			for j := 0; j < cfg.Concurrency; j++ {
				go reqWorker(ctx, cfg, pdClients, i, durCh)
			}
		}
	}

	wg.Add(1)
	go showStats(ctx, cfg.Interval, cfg.Verbose, durCh)

	timer := time.NewTimer(cfg.Duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
	cancel()

	wg.Wait()
	for _, pdCli := range pdClients {
		pdCli.Close()
	}
}

var latencyTDigest = tdigest.New()

func reqWorker(ctx context.Context, cfg *config,
	pdClients []pd.Client, clientIdx int, durCh chan time.Duration) {
	defer wg.Done()

	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		err                    error
		maxRetryTime           = 120
		sleepIntervalOnFailure = 1000 * time.Millisecond
		totalSleepBeforeGetTS  time.Duration
	)
	pdCli := pdClients[clientIdx]

	for {
		if pdCli == nil || (cfg.EnableFaultInjection && shouldInjectFault(cfg)) {
			if pdCli != nil {
				pdCli.Close()
			}
			pdCli, err = createPDClient(ctx, cfg)
			if err != nil {
				log.Error(fmt.Sprintf("re-create pd client #%d failed: %v", clientIdx, err))
				select {
				case <-reqCtx.Done():
				case <-time.After(100 * time.Millisecond):
				}
				continue
			}
			pdClients[clientIdx] = pdCli
		}

		totalSleepBeforeGetTS = 0
		start := time.Now()

		i := 0
		for ; i < maxRetryTime; i++ {
			var ticker *time.Ticker
			if cfg.MaxTSOSendIntervalMilliseconds > 0 {
				sleepBeforeGetTS := time.Duration(rand.Intn(cfg.MaxTSOSendIntervalMilliseconds)) * time.Millisecond
				ticker = time.NewTicker(sleepBeforeGetTS)
				select {
				case <-reqCtx.Done():
				case <-ticker.C:
					totalSleepBeforeGetTS += sleepBeforeGetTS
				}
			}
			_, _, err = pdCli.GetLocalTS(reqCtx, cfg.DcLocation)
			if errors.Cause(err) == context.Canceled {
				ticker.Stop()
				return
			}
			if err == nil {
				ticker.Stop()
				break
			}
			log.Error(fmt.Sprintf("%v", err))
			time.Sleep(sleepIntervalOnFailure)
		}
		if err != nil {
			log.Fatal(fmt.Sprintf("%v", err))
		}
		dur := time.Since(start) - time.Duration(i)*sleepIntervalOnFailure - totalSleepBeforeGetTS

		select {
		case <-reqCtx.Done():
			return
		case durCh <- dur:
		}
	}
}

func createPDClient(ctx context.Context, cfg *config) (pd.Client, error) {
	var (
		pdCli pd.Client
		err   error
	)

	opts := make([]pd.ClientOption, 0)
	opts = append(opts, pd.WithGRPCDialOptions(
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    keepaliveTime,
			Timeout: keepaliveTimeout,
		}),
	))

	if len(cfg.KeyspaceName) > 0 {
		apiCtx := pd.NewAPIContextV2(cfg.KeyspaceName)
		pdCli, err = pd.NewClientWithAPIContext(ctx, apiCtx, []string{cfg.PDAddrs}, pd.SecurityOption{
			CAPath:   cfg.CaPath,
			CertPath: cfg.CertPath,
			KeyPath:  cfg.KeyPath,
		}, opts...)
	} else {
		pdCli, err = pd.NewClientWithKeyspace(ctx, uint32(cfg.KeyspaceID), []string{cfg.PDAddrs}, pd.SecurityOption{
			CAPath:   cfg.CaPath,
			CertPath: cfg.CertPath,
			KeyPath:  cfg.KeyPath,
		}, opts...)
	}
	if err != nil {
		return nil, err
	}

	pdCli.UpdateOption(pd.MaxTSOBatchWaitInterval, cfg.MaxBatchWaitInterval)
	pdCli.UpdateOption(pd.EnableTSOFollowerProxy, cfg.EnableTSOFollowerProxy)
	return pdCli, err
}

func shouldInjectFault(cfg *config) bool {
	return rand.Intn(10000) < int(cfg.FaultInjectionRate*10000)
}
