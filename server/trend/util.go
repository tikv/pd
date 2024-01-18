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
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"go.etcd.io/etcd/pkg/transport"
	"go.uber.org/zap"
)

// AsyncFromEtcd is a trend of async duration from etcd.
type AsyncFromEtcd struct {
	lastMetric map[string]float64
	curMetric  map[string]float64
	subMetric  []float64
}

// NewAsyncFromEtcd creates a new instance of AsyncFromEtcd.
func NewAsyncFromEtcd() *AsyncFromEtcd {
	return &AsyncFromEtcd{
		lastMetric: make(map[string]float64),
		curMetric:  make(map[string]float64),
		subMetric:  make([]float64, 20),
	}
}

// GetVal gets the async duration from etcd.
func (a *AsyncFromEtcd) GetVal(ep string) (float64, float64, error) {
	a.subMetric = a.subMetric[:0]
	res := getMetricsFromEtcd(ep, "etcd_disk_wal_fsync_duration_seconds")

	var sum, cnt float64
	for _, metric := range res.metrics {
		ss := strings.Split(metric, " ")
		if len(ss) != 2 {
			return 0, 0, fmt.Errorf("failed to parse metric %s", metric)
		}

		if ss[0] == "etcd_disk_wal_fsync_duration_seconds_sum" {
			sum, _ = strconv.ParseFloat(ss[1], 64)
			continue
		}
		if ss[0] == "etcd_disk_wal_fsync_duration_seconds_count" {
			cnt, _ = strconv.ParseFloat(ss[1], 64)
			continue
		}

		if val, err := strconv.ParseFloat(ss[1], 64); err == nil {
			a.lastMetric[ss[0]] = a.curMetric[ss[0]]
			a.curMetric[ss[0]] = val
			a.subMetric = append(a.subMetric, a.curMetric[ss[0]]-a.lastMetric[ss[0]])
		} else {
			log.Error("failed to parse metric", zap.Error(err))
			return 0, 0, err
		}
	}

	// expect `sum` and `cnt` are not zero
	if len(a.subMetric) < 3 {
		return 0, 0, fmt.Errorf("failed to get metric, subMetric len is %d less than 3", len(a.subMetric))
	}

	base, val := a.transferTo(sum, cnt, res)
	return base, val, nil
}

// the fsync ms is from `0.0001` to `8192`(2^0-2^13) which is exponential distribution
// to reduce the impact of the largest value, we need to aggregate the data.
// We have two steps:
//  1. consider the all load average value is the base, which can ignore different environment
//     TODO: replace all value with time in server alive
//  2. get a threshold, which is 99% of the total count
//     - compare all array to get the value
func (a *AsyncFromEtcd) transferTo(sum, cnt float64, metric metric) (float64, float64) {
	// uint is seconds
	base := sum / cnt
	count := a.subMetric[len(a.subMetric)-1]
	threshold := count * 0.99
	curSec := 0.0
	// last val is `cnt` and `sum`
	for i := 0; i < len(a.subMetric)-2; i++ {
		// for loop is like log2() to be 0-13
		if a.subMetric[i] >= threshold {
			ss := strings.Split(metric.metrics[i], " ")
			curSec = math.Pow(2.0, float64(i)) * 0.001
			log.Info("getVal", zap.Float64("curSec", curSec), zap.Int("i", i), zap.Float64("metric", math.Pow(2.0, float64(i))), zap.String("metric", ss[0]),
				zap.Float64("base", base), zap.Float64("subMetric[i]", a.subMetric[i]), zap.Float64("count", count),
				zap.Float64("threshold", threshold))
			break
		}
	}
	return base, curSec
}

func getMetricsFromEtcd(ep string, curName string) metric {
	lines, err := fetchMetricsFromEtcd(ep)
	if err != nil {
		log.Info("failed to fetch metrics", zap.Error(err))
		return metric{}
	}
	return parse(lines, curName)
}

func fetchMetricsFromEtcd(ep string) (lines []string, err error) {
	tr, err := transport.NewTimeoutTransport(transport.TLSInfo{}, time.Second, time.Second, time.Second)
	if err != nil {
		return nil, err
	}
	cli := &http.Client{Transport: tr}
	resp, err := cli.Get(fmt.Sprintf("%s/metrics", ep))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, rerr := io.ReadAll(resp.Body)
	if rerr != nil {
		return nil, rerr
	}
	lines = strings.Split(string(b), "\n")
	return lines, nil
}

type metric struct {
	// metrics name
	name string

	metrics []string
}

func (m metric) String() (s string) {
	s += strings.Join(m.metrics, "\n")
	return s
}

func parse(lines []string, curName string) (m metric) {
	for _, line := range lines {
		if strings.HasPrefix(line, "# HELP ") {
			// add previous metric and initialize
			if m.name == curName {
				return
			}
			m = metric{metrics: make([]string, 0)}

			ss := strings.Split(strings.Replace(line, "# HELP ", "", 1), " ")
			m.name = ss[0]
			continue
		}

		if strings.HasPrefix(line, "# TYPE ") {
			continue
		}

		if m.name == curName {
			m.metrics = append(m.metrics, line)
		}
	}

	return
}
