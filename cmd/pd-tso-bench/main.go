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
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/pingcap/pd/pd-client"
)

var (
	pdAddrs     = flag.String("pd", "127.0.0.1:2379", "pd address")
	concurrency = flag.Int("C", 100, "concurrency")
	num         = flag.Int("N", 1000, "number of request per request worker")
	sleep       = flag.Duration("sleep", time.Millisecond, "sleep time after a request, used to adjust pressure")
)

func main() {
	flag.Parse()
	pdCli, err := pd.NewClient([]string{*pdAddrs})
	if err != nil {
		log.Fatal(err)
	}
	// To avoid the first time high latency.
	_, _, err = pdCli.GetTS()
	if err != nil {
		log.Fatal(err)
	}
	statsCh := make(chan *stats, *concurrency)
	for i := 0; i < *concurrency; i++ {
		go reqWorker(pdCli, statsCh)
	}
	finalStats := newStats(0)
	for i := 0; i < *concurrency; i++ {
		s := <-statsCh
		finalStats.merge(s)
	}
	fmt.Println(finalStats.String())
	pdCli.Close()
}

type stats struct {
	maxDur       time.Duration
	minDur       time.Duration
	count        int
	milliCnt     int
	twoMilliCnt  int
	fiveMilliCnt int
}

func newStats(count int) *stats {
	return &stats{
		minDur: time.Second,
		count:  count,
	}
}

func (s *stats) update(dur time.Duration) {
	if s.minDur == 0 {
		s.minDur = time.Second
	}
	if dur > s.maxDur {
		s.maxDur = dur
	}
	if dur < s.minDur {
		s.minDur = dur
	}
	if dur > time.Millisecond {
		s.milliCnt++
	}
	if dur > time.Millisecond*2 {
		s.twoMilliCnt++
	}
	if dur > time.Millisecond*5 {
		s.fiveMilliCnt++
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
}

func (s *stats) String() string {
	return fmt.Sprintf("\nmax:%v, min:%v, count:%d, >1ms = %d, >2ms = %d, >5ms = %d\n",
		s.maxDur, s.minDur, s.count, s.milliCnt, s.twoMilliCnt, s.fiveMilliCnt)
}

func reqWorker(pdCli pd.Client, statsCh chan *stats) {
	s := newStats(*num)
	for i := 0; i < s.count; i++ {
		start := time.Now()
		_, _, err := pdCli.GetTS()
		if err != nil {
			log.Fatal(err)
		}
		dur := time.Since(start)
		s.update(dur)
		time.Sleep(*sleep)
	}
	statsCh <- s
}
