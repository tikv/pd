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
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/influxdata/tdigest"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
)

const (
	keepaliveTime    = 10 * time.Second
	keepaliveTimeout = 3 * time.Second
)

var (
	pdAddrs                        = flag.String("pd", "127.0.0.1:2379", "pd address")
	clientNumber                   = flag.Int("client", 1, "the number of pd clients involved in each benchmark")
	concurrency                    = flag.Int("c", 1000, "concurrency")
	count                          = flag.Int("count", 1, "the count number that the test will run")
	duration                       = flag.Duration("duration", 60*time.Second, "how many seconds the test will last")
	verbose                        = flag.Bool("v", false, "output statistics info every interval and output metrics info at the end")
	interval                       = flag.Duration("interval", time.Second, "interval to output the statistics")
	caPath                         = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs")
	certPath                       = flag.String("cert", "", "path of file that contains X509 certificate in PEM format")
	keyPath                        = flag.String("key", "", "path of file that contains X509 key in PEM format")
	maxBatchWaitInterval           = flag.Duration("batch-interval", 0, "the max batch wait interval")
	enableTSOFollowerProxy         = flag.Bool("enable-tso-follower-proxy", false, "whether enable the TSO Follower Proxy")
	enableFaultInjection           = flag.Bool("enable-fault-injection", false, "whether enable fault injection")
	faultInjectionRate             = flag.Float64("fault-injection-rate", 0.01, "the failure rate [0.0001, 1]. 0.01 means 1% failure rate")
	maxTSOSendIntervalMilliseconds = flag.Int("max-send-interval-ms", 0, "max tso send interval in milliseconds, 60s by default")
	keyspaceID                     = flag.Uint("keyspace-id", 0, "the id of the keyspace to access")
	keyspaceName                   = flag.String("keyspace-name", "", "the name of the keyspace to access")
	useTSOServerProxy              = flag.Bool("use-tso-server-proxy", false, "whether send tso requests to tso server proxy instead of tso service directly")
	wg                             sync.WaitGroup
)

var promServer *httptest.Server

func collectMetrics(server *httptest.Server) string {
	time.Sleep(1100 * time.Millisecond)
	res, _ := http.Get(server.URL)
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	return string(body)
}

func main() {
	flag.Parse()
	ctx, cancel := context.WithCancel(context.Background())

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		<-sc
		cancel()
	}()

	for i := range *count {
		fmt.Printf("\nStart benchmark #%d, duration: %+vs\n", i, duration.Seconds())
		bench(ctx)
	}
}

func bench(mainCtx context.Context) {
	promServer = httptest.NewServer(promhttp.Handler())

	// Initialize all clients
	fmt.Printf("Create %d client(s) for benchmark\n", *clientNumber)
	pdClients := make([]pd.Client, *clientNumber)
	for idx := range pdClients {
		pdCli, err := createPDClient(mainCtx)
		if err != nil {
			log.Fatal(fmt.Sprintf("create pd client #%d failed: %v", idx, err))
		}
		pdClients[idx] = pdCli
	}

	ctx, cancel := context.WithCancel(mainCtx)
	// To avoid the first time high latency.
	for idx, pdCli := range pdClients {
		_, _, err := pdCli.GetTS(ctx)
		if err != nil {
			log.Fatal("get first time tso failed", zap.Int("client-number", idx), zap.Error(err))
		}
	}

	durCh := make(chan time.Duration, 2*(*concurrency)*(*clientNumber))

	if *enableFaultInjection {
		fmt.Printf("Enable fault injection, failure rate: %f\n", *faultInjectionRate)
		wg.Add(*clientNumber)
		for i := range *clientNumber {
			go reqWorker(ctx, pdClients, i, durCh)
		}
	} else {
		wg.Add((*concurrency) * (*clientNumber))
		for i := range *clientNumber {
			for range *concurrency {
				go reqWorker(ctx, pdClients, i, durCh)
			}
		}
	}

	wg.Add(1)
	go showStats(ctx, durCh)

	timer := time.NewTimer(*duration)
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

var latencyTDigest *tdigest.TDigest = tdigest.New()

func showStats(ctx context.Context, durCh chan time.Duration) {
	defer wg.Done()

	statCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	s := newStats()
	total := newStats()

	fmt.Println()
	for {
		select {
		case <-ticker.C:
			// runtime.GC()
			if *verbose {
				fmt.Println(s.counter())
			}
			total.merge(s)
			s = newStats()
		case d := <-durCh:
			s.update(d)
		case <-statCtx.Done():
			fmt.Println("\nTotal:")
			fmt.Println(total.counter())
			fmt.Println(total.percentage())
			// Calculate the percentiles by using the tDigest algorithm.
			fmt.Printf("P0.5: %.4fms, P0.8: %.4fms, P0.9: %.4fms, P0.99: %.4fms\n\n", latencyTDigest.Quantile(0.5), latencyTDigest.Quantile(0.8), latencyTDigest.Quantile(0.9), latencyTDigest.Quantile(0.99))
			if *verbose {
				fmt.Println(collectMetrics(promServer))
			}
			return
		}
	}
}

const (
	twoDur          = time.Millisecond * 2
	fiveDur         = time.Millisecond * 5
	tenDur          = time.Millisecond * 10
	thirtyDur       = time.Millisecond * 30
	fiftyDur        = time.Millisecond * 50
	oneHundredDur   = time.Millisecond * 100
	twoHundredDur   = time.Millisecond * 200
	fourHundredDur  = time.Millisecond * 400
	eightHundredDur = time.Millisecond * 800
	oneThousandDur  = time.Millisecond * 1000
)

type stats struct {
	maxDur          time.Duration
	minDur          time.Duration
	totalDur        time.Duration
	count           int
	submilliCnt     int
	milliCnt        int
	twoMilliCnt     int
	fiveMilliCnt    int
	tenMSCnt        int
	thirtyCnt       int
	fiftyCnt        int
	oneHundredCnt   int
	twoHundredCnt   int
	fourHundredCnt  int
	eightHundredCnt int
	oneThousandCnt  int
}

func newStats() *stats {
	return &stats{
		minDur: time.Hour,
		maxDur: 0,
	}
}

func (s *stats) update(dur time.Duration) {
	s.count++
	s.totalDur += dur
	latencyTDigest.Add(float64(dur.Nanoseconds())/1e6, 1)

	if dur > s.maxDur {
		s.maxDur = dur
	}
	if dur < s.minDur {
		s.minDur = dur
	}

	if dur > oneThousandDur {
		s.oneThousandCnt++
		return
	}

	if dur > eightHundredDur {
		s.eightHundredCnt++
		return
	}

	if dur > fourHundredDur {
		s.fourHundredCnt++
		return
	}

	if dur > twoHundredDur {
		s.twoHundredCnt++
		return
	}

	if dur > oneHundredDur {
		s.oneHundredCnt++
		return
	}

	if dur > fiftyDur {
		s.fiftyCnt++
		return
	}

	if dur > thirtyDur {
		s.thirtyCnt++
		return
	}

	if dur > tenDur {
		s.tenMSCnt++
		return
	}

	if dur > fiveDur {
		s.fiveMilliCnt++
		return
	}

	if dur > twoDur {
		s.twoMilliCnt++
		return
	}

	if dur > time.Millisecond {
		s.milliCnt++
		return
	}

	s.submilliCnt++
}

func (s *stats) merge(other *stats) {
	if s.maxDur < other.maxDur {
		s.maxDur = other.maxDur
	}
	if s.minDur > other.minDur {
		s.minDur = other.minDur
	}

	s.count += other.count
	s.totalDur += other.totalDur
	s.submilliCnt += other.submilliCnt
	s.milliCnt += other.milliCnt
	s.twoMilliCnt += other.twoMilliCnt
	s.fiveMilliCnt += other.fiveMilliCnt
	s.tenMSCnt += other.tenMSCnt
	s.thirtyCnt += other.thirtyCnt
	s.fiftyCnt += other.fiftyCnt
	s.oneHundredCnt += other.oneHundredCnt
	s.twoHundredCnt += other.twoHundredCnt
	s.fourHundredCnt += other.fourHundredCnt
	s.eightHundredCnt += other.eightHundredCnt
	s.oneThousandCnt += other.oneThousandCnt
}

func (s *stats) counter() string {
	return fmt.Sprintf(
		"count: %d, max: %.4fms, min: %.4fms, avg: %.4fms\n<1ms: %d, >1ms: %d, >2ms: %d, >5ms: %d, >10ms: %d, >30ms: %d, >50ms: %d, >100ms: %d, >200ms: %d, >400ms: %d, >800ms: %d, >1s: %d",
		s.count, float64(s.maxDur.Nanoseconds())/float64(time.Millisecond), float64(s.minDur.Nanoseconds())/float64(time.Millisecond), float64(s.totalDur.Nanoseconds())/float64(s.count)/float64(time.Millisecond),
		s.submilliCnt, s.milliCnt, s.twoMilliCnt, s.fiveMilliCnt, s.tenMSCnt, s.thirtyCnt, s.fiftyCnt, s.oneHundredCnt, s.twoHundredCnt, s.fourHundredCnt,
		s.eightHundredCnt, s.oneThousandCnt)
}

func (s *stats) percentage() string {
	return fmt.Sprintf(
		"count: %d, <1ms: %2.2f%%, >1ms: %2.2f%%, >2ms: %2.2f%%, >5ms: %2.2f%%, >10ms: %2.2f%%, >30ms: %2.2f%%, >50ms: %2.2f%%, >100ms: %2.2f%%, >200ms: %2.2f%%, >400ms: %2.2f%%, >800ms: %2.2f%%, >1s: %2.2f%%", s.count,
		s.calculate(s.submilliCnt), s.calculate(s.milliCnt), s.calculate(s.twoMilliCnt), s.calculate(s.fiveMilliCnt), s.calculate(s.tenMSCnt), s.calculate(s.thirtyCnt), s.calculate(s.fiftyCnt),
		s.calculate(s.oneHundredCnt), s.calculate(s.twoHundredCnt), s.calculate(s.fourHundredCnt), s.calculate(s.eightHundredCnt), s.calculate(s.oneThousandCnt))
}

func (s *stats) calculate(count int) float64 {
	return float64(count) * 100 / float64(s.count)
}

func reqWorker(ctx context.Context, pdClients []pd.Client, clientIdx int, durCh chan time.Duration) {
	defer wg.Done()

	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		err                    error
		maxRetryTime           int           = 120
		sleepIntervalOnFailure time.Duration = 1000 * time.Millisecond
		totalSleepBeforeGetTS  time.Duration
	)
	pdCli := pdClients[clientIdx]

	for {
		if pdCli == nil || (*enableFaultInjection && shouldInjectFault()) {
			if pdCli != nil {
				pdCli.Close()
			}
			pdCli, err = createPDClient(ctx)
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
			if *maxTSOSendIntervalMilliseconds > 0 {
				sleepBeforeGetTS := time.Duration(rand.Intn(*maxTSOSendIntervalMilliseconds)) * time.Millisecond
				if sleepBeforeGetTS > 0 {
					ticker = time.NewTicker(sleepBeforeGetTS)
					select {
					case <-reqCtx.Done():
					case <-ticker.C:
						totalSleepBeforeGetTS += sleepBeforeGetTS
					}
				}
			}
			_, _, err = pdCli.GetTS(reqCtx)
			if errors.Cause(err) == context.Canceled {
				if ticker != nil {
					ticker.Stop()
				}

				return
			}
			if err == nil {
				if ticker != nil {
					ticker.Stop()
				}
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

func createPDClient(ctx context.Context) (pd.Client, error) {
	var (
		pdCli pd.Client
		err   error
	)

	opts := make([]opt.ClientOption, 0)
	if *useTSOServerProxy {
		opts = append(opts, opt.WithTSOServerProxyOption(true))
	}
	opts = append(opts, opt.WithGRPCDialOptions(
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    keepaliveTime,
			Timeout: keepaliveTimeout,
		}),
	))

	if len(*keyspaceName) > 0 {
		apiCtx := pd.NewAPIContextV2(*keyspaceName)
		pdCli, err = pd.NewClientWithAPIContext(ctx, apiCtx,
			caller.TestComponent, []string{*pdAddrs},
			pd.SecurityOption{
				CAPath:   *caPath,
				CertPath: *certPath,
				KeyPath:  *keyPath,
			}, opts...)
	} else {
		pdCli, err = pd.NewClientWithKeyspace(ctx,
			caller.TestComponent,
			uint32(*keyspaceID), []string{*pdAddrs},
			pd.SecurityOption{
				CAPath:   *caPath,
				CertPath: *certPath,
				KeyPath:  *keyPath,
			}, opts...)
	}
	if err != nil {
		return nil, err
	}

	pdCli.UpdateOption(opt.MaxTSOBatchWaitInterval, *maxBatchWaitInterval)
	pdCli.UpdateOption(opt.EnableTSOFollowerProxy, *enableTSOFollowerProxy)
	return pdCli, err
}

func shouldInjectFault() bool {
	return rand.Intn(10000) < int(*faultInjectionRate*10000)
}
