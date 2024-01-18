// Copyright 2024 TiKV Project Authors.
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

package trend

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func TestLaLbRate(t *testing.T) {
	// [smallWindow.Avg()=5.363636363636363] [bigWindow.Avg()=4.809523809523809]
	laAvg, lbAvg := 5.363636363636363, 4.809523809523809
	res := laLbRate(laAvg, lbAvg, unSensitiveCause)
	log.Info("res", zap.Float64("res", res))

	// [smallWindow.Avg()=8.1] [bigWindow.Avg()=9.741935483870968]
	laAvg, lbAvg = 8.1, 9.741935483870968
	res = laLbRate(laAvg, lbAvg, unSensitiveCause)
	log.Info("res", zap.Float64("res", res))

	// [smallWindow.Avg()=22.952380952380953] [bigWindow.Avg()=16.360655737704917]
	laAvg, lbAvg = 22.952380952380953, 16.360655737704917
	res = laLbRate(laAvg, lbAvg, unSensitiveCause)
	log.Info("res", zap.Float64("res", res))

	// [unSensitive=0.005979197544224765] [base=0.0011958395088449531] [smallWindow.Avg()=0.02655] [bigWindow.Avg()=0.009516666666666666]
	// [base=0.001456156503900156] [smallWindow.Avg()=0.052100000000000014] [bigWindow.Avg()=0.018033333333333335]
	base := 0.001456156503900156
	laAvg, lbAvg, sense := 0.052100000000000014, 0.018033333333333335, base*unSensitiveCause
	res = laLbRate(laAvg, lbAvg, sense)
	res2 := laLbRateSimple(laAvg, lbAvg, sense)
	log.Info("res", zap.Float64("res", res), zap.Float64("res2", res2))
}

func TestWindows(t *testing.T) {
	duration := time.Second * 5
	window := NewSampleWindow("", "test", duration)

	value := 10.0
	now := time.Now()
	println("now:", now.String())
	window.Record(value, now)

	value = 15.0
	now = now.Add(time.Second * 1)
	window.Record(value, now)

	println("window sum:", window.sum)
	average := window.Avg()
	fmt.Println(average)
}

func TestTrendDouble(t *testing.T) {
	trend := NewTrend("test-double", time.Second)

	now := time.Now()
	value := 3 * 1.0
	for i := 0; i < 10; i++ {
		now = now.Add(time.Millisecond * 100)
		trend.Record(1, value, now)
	}

	value *= 2
	for i := 0; i < 10; i++ {
		now = now.Add(time.Millisecond * 100)
		trend.Record(1, value, now)
	}

	result, _ := trend.AvgRate()
	fmt.Println(result)
}

func TestTrendGetAsyncMock(t *testing.T) {
	trend := NewTrend("test-mock", 1*time.Second)

	go func() {
		timer := time.NewTicker(time.Millisecond * 10)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				result, _ := trend.AvgRate()
				log.Info("result", zap.Float64("result", result))
			}
		}
	}()

	log.Info("--------------------- normal ---------------------")
	// fill with normal val
	now := time.Now()
	for i := 0; i < 30; i++ {
		now = now.Add(time.Millisecond * 100)
		time.Sleep(time.Millisecond * 100)
		res := float64(rand.Intn(8)+1) * 0.001
		trend.Record(1, res, now)
	}

	// fill with spike
	log.Info("--------------------- spike111111 ---------------------")
	for i := 0; i < 10; i++ {
		now = now.Add(time.Millisecond * 100)
		time.Sleep(time.Millisecond * 100)
		res := float64(rand.Intn(8)+1) * 0.001
		trend.Record(1, res+4, now)
	}

	log.Info("--------------------- normal ---------------------")
	for i := 0; i < 20; i++ {
		now = now.Add(time.Millisecond * 100)
		time.Sleep(time.Millisecond * 100)
		res := float64(rand.Intn(8)+1) * 0.001
		trend.Record(1, res, now)
	}
}

func TestTrendGetAsync(t *testing.T) {
	trend := NewTrend("test-async", 10*time.Second)
	a := NewAsyncFromEtcd()

	go func() {
		timer := time.NewTicker(time.Second * 1)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				result, _ := trend.AvgRate()
				log.Info("result", zap.Float64("result", result))
			}
		}
	}()

	log.Info("--------------------- normal ---------------------")
	// fill with normal val
	for i := 0; i < 30; i++ {
		time.Sleep(time.Second * 1)
		now := time.Now()
		base, res, _ := a.GetVal("http://127.0.0.1:2384")
		log.Info("!res", zap.Float64("res", res))
		trend.Record(base, res, now)
	}

	// fill with spike
	log.Info("--------------------- spike111111 ---------------------")
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second * 1)
		now := time.Now()
		base, res, _ := a.GetVal("http://127.0.0.1:2384")
		log.Info("!res", zap.Float64("res", res))
		trend.Record(base, float64(res)*10+10, now)
	}

	log.Info("--------------------- normal ---------------------")

	for i := 0; i < 20; i++ {
		time.Sleep(time.Second * 1)
		now := time.Now()
		base, res, _ := a.GetVal("http://127.0.0.1:2384")
		log.Info("!res", zap.Float64("res", res))
		trend.Record(base, res, now)
	}

	println(trend.smallWindow.Avg(), trend.smallWindow.sum, trend.smallWindow.values.Len())
	println(trend.bigWindow.Avg(), trend.bigWindow.sum, trend.bigWindow.values.Len())
}

func TestTrendSpike(t *testing.T) {
	trend := NewTrend("test-spike", time.Second)

	now := time.Now()
	value := 3 * 1.0
	for i := 0; i < 15; i++ {
		now = now.Add(time.Millisecond * 100)
		trend.Record(1, value, now)
	}

	value = 3 * 1000.0
	for i := 0; i < 15; i++ {
		now = now.Add(time.Millisecond * 100)
		trend.Record(1, value, now)
	}

	result, _ := trend.AvgRate()
	println(trend.smallWindow.Avg() > trend.bigWindow.Avg())
	fmt.Println(result)
}

func TestGetAsyncFromEtcd(t *testing.T) {

	a := NewAsyncFromEtcd()
	base, res, _ := a.GetVal("http://127.0.0.1:2384")
	log.Info("!res", zap.Float64("res", res), zap.Float64("base", base))

	time.Sleep(time.Second * 1)
	base, res, _ = a.GetVal("http://127.0.0.1:2384")

	log.Info("!res", zap.Float64("res", res), zap.Float64("base", base))
}
