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
	"sync"
	"time"

	"github.com/pingcap/pd/pd-client"
)

var (
	pdAddrs     = flag.String("pd", "127.0.0.1:2379", "pd address")
	concurrency = flag.Int("C", 100, "concurrency")
	num         = flag.Int("N", 1000, "number of request per request worker")
	sleepFlag   = flag.String("sleep", "1ms", "sleep time after a request, used to adjust pressure")
	sleep       = time.Millisecond
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
	sleep, err = time.ParseDuration(*sleepFlag)
	if err != nil {
		log.Fatal(err)
	}
	wg := new(sync.WaitGroup)
	wg.Add(*concurrency)
	for i := 0; i < *concurrency; i++ {
		go reqWorker(pdCli, wg)
	}
	wg.Wait()
	pdCli.Close()
}

func reqWorker(pdCli pd.Client, wg *sync.WaitGroup) {
	var maxDur time.Duration
	var maxidx int
	var minDur = time.Second
	var (
		milliCnt     int
		twoMilliCnt  int
		fiveMilliCnt int
	)
	for i := 0; i < *num; i++ {
		start := time.Now()
		_, _, err := pdCli.GetTS()
		if err != nil {
			log.Fatal(err)
		}
		dur := time.Since(start)
		if dur > maxDur {
			maxDur = dur
			maxidx = i
		}
		if dur < minDur {
			minDur = dur
		}
		if dur > time.Millisecond {
			milliCnt++
		}
		if dur > time.Millisecond*2 {
			twoMilliCnt++
		}
		if dur > time.Millisecond*5 {
			fiveMilliCnt++
		}
		time.Sleep(sleep)
	}
	fmt.Printf("max:(%v,%d), min:%v, num:%d, >1ms = %d, >2ms = %d, >5ms = %d\n", maxDur, maxidx, minDur, *num, milliCnt, twoMilliCnt, fiveMilliCnt)
	wg.Done()
}
