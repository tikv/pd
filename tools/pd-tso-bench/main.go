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
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

var (
	pdAddrs      = flag.String("pd", "127.0.0.1:2379", "pd address")
	clientNumber = flag.Int("client", 1, "the number of pd clients involved in each benchmark")
	concurrency  = flag.Int("c", 1000, "concurrency")
	count        = flag.Int("count", 1, "the count number that the test will run")
	duration     = flag.Duration("duration", 60*time.Second, "how many seconds the test will last")
	dcLocation   = flag.String("dc", "global", "which dc-location this bench will request")
	verbose      = flag.Bool("v", false, "output statistics info every interval and output metrics info at the end")
	interval     = flag.Duration("interval", time.Second, "interval to output the statistics")
	caPath       = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs")
	certPath     = flag.String("cert", "", "path of file that contains X509 certificate in PEM format")
	keyPath      = flag.String("key", "", "path of file that contains X509 key in PEM format")
	wg           sync.WaitGroup
)

var promServer *httptest.Server

func collectMetrics(server *httptest.Server) string {
	time.Sleep(1100 * time.Millisecond)
	res, _ := http.Get(server.URL)
	body, _ := ioutil.ReadAll(res.Body)
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

	for i := 0; i < *count; i++ {
		fmt.Printf("\nStart benchmark #%d, duration: %+vs\n", i, (*duration).Seconds())
		bench(ctx)
	}
}

func bench(mainCtx context.Context) {
	promServer = httptest.NewServer(promhttp.Handler())

	// Initialize all clients
	fmt.Printf("Create %d client(s) for benchmark\n", *clientNumber)
	pdClients := make([]pd.Client, *clientNumber)
	for idx := range pdClients {
		pdCli, err := pd.NewClient([]string{*pdAddrs}, pd.SecurityOption{
			CAPath:   *caPath,
			CertPath: *certPath,
			KeyPath:  *keyPath,
		})
		if err != nil {
			log.Fatal(fmt.Sprintf("create pd client #%d failed: %v", idx, err))
		}
		pdClients[idx] = pdCli
	}

	ctx, cancel := context.WithCancel(mainCtx)
	// To avoid the first time high latency.
	for idx, pdCli := range pdClients {
		_, _, err := pdCli.GetLocalTS(ctx, *dcLocation)
		if err != nil {
			log.Fatal("get first time tso failed", zap.Int("client-number", idx), zap.Error(err))
		}
	}

	durCh := make(chan time.Duration, 2*(*concurrency)*(*clientNumber))

	wg.Add((*concurrency) * (*clientNumber))
	for _, pdCli := range pdClients {
		for i := 0; i < *concurrency; i++ {
			go reqWorker(ctx, pdCli, durCh)
		}
	}

	wg.Add(1)
	go showStats(ctx, durCh)

	time.Sleep(*duration)
	cancel()

	wg.Wait()

	for _, pdCli := range pdClients {
		pdCli.Close()
	}
}

func showStats(ctx context.Context, durCh chan time.Duration) {
	defer wg.Done()

	statCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	ticker := time.NewTicker(*interval)

	s := newStats()
	total := newStats()

	for {
		select {
		case <-ticker.C:
			//runtime.GC()
			if *verbose {
				fmt.Println(s.Counter())
			}
			total.merge(s)
			s = newStats()
		case d := <-durCh:
			s.update(d)
		case <-statCtx.Done():
			fmt.Println("\nTotal:")
			fmt.Println(total.Counter())
			fmt.Println(total.Percentage())
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
	count           int
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
}

func (s *stats) merge(other *stats) {
	if s.maxDur < other.maxDur {
		s.maxDur = other.maxDur
	}
	if s.minDur > other.minDur {
		s.minDur = other.minDur
	}

	s.count += other.count
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

func (s *stats) Counter() string {
	return fmt.Sprintf(
		"count:%d, max:%d, min:%d, >1ms:%d, >2ms:%d, >5ms:%d, >10ms:%d, >30ms:%d >50ms:%d >100ms:%d >200ms:%d >400ms:%d >800ms:%d >1s:%d",
		s.count, s.maxDur.Nanoseconds()/int64(time.Millisecond), s.minDur.Nanoseconds()/int64(time.Millisecond),
		s.milliCnt, s.twoMilliCnt, s.fiveMilliCnt, s.tenMSCnt, s.thirtyCnt, s.fiftyCnt, s.oneHundredCnt, s.twoHundredCnt, s.fourHundredCnt,
		s.eightHundredCnt, s.oneThousandCnt)
}

func (s *stats) Percentage() string {
	return fmt.Sprintf(
		"count:%d, >1ms:%2.2f%%, >2ms:%2.2f%%, >5ms:%2.2f%%, >10ms:%2.2f%%, >30ms:%2.2f%% >50ms:%2.2f%% >100ms:%2.2f%% >200ms:%2.2f%% >400ms:%2.2f%% >800ms:%2.2f%% >1s:%2.2f%%", s.count,
		s.calculate(s.milliCnt), s.calculate(s.twoMilliCnt), s.calculate(s.fiveMilliCnt), s.calculate(s.tenMSCnt), s.calculate(s.thirtyCnt), s.calculate(s.fiftyCnt),
		s.calculate(s.oneHundredCnt), s.calculate(s.twoHundredCnt), s.calculate(s.fourHundredCnt), s.calculate(s.eightHundredCnt), s.calculate(s.oneThousandCnt))
}

func (s *stats) calculate(count int) float64 {
	return float64(count) * 100 / float64(s.count)
}

func reqWorker(ctx context.Context, pdCli pd.Client, durCh chan time.Duration) {
	defer wg.Done()

	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		start := time.Now()
		_, _, err := pdCli.GetLocalTS(reqCtx, *dcLocation)
		if errors.Cause(err) == context.Canceled {
			return
		}

		if err != nil {
			log.Fatal(fmt.Sprintf("%v", err))
		}
		dur := time.Since(start)

		select {
		case <-reqCtx.Done():
			return
		case durCh <- dur:
		}
	}
}
