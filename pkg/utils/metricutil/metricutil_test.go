// Copyright 2016 TiKV Project Authors.
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

package metricutil

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/utils/typeutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestCamelCaseToSnakeCase(t *testing.T) {
	re := require.New(t)
	inputs := []struct {
		name    string
		newName string
	}{
		{"a", "a"},
		{"snake", "snake"},
		{"A", "a"},
		{"ID", "id"},
		{"MOTD", "motd"},
		{"Snake", "snake"},
		{"SnakeTest", "snake_test"},
		{"SnakeID", "snake_id"},
		{"SnakeIDGoogle", "snake_id_google"},
		{"GetPDMembers", "get_pd_members"},
		{"LinuxMOTD", "linux_motd"},
		{"OMGWTFBBQ", "omgwtfbbq"},
		{"omg_wtf_bbq", "omg_wtf_bbq"},
		{"Abc", "abc"},
		{"aAbc", "a_abc"},
		{"aBCd", "a_b_cd"},
		{"ABc", "a_bc"},
		{"AbcDef", "abc_def"},
		{"AbcdefghijklmnopqrstuvwxyzAbcdefghijklmnopqrstuvwxyzAbcdefghijklmnopqrstuvwxyz",
			"abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz"},
	}

	for _, input := range inputs {
		re.Equal(input.newName, camelCaseToSnakeCase(input.name))
	}
}

func TestCoverage(_ *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfgs := []*MetricConfig{
		{
			PushJob:     "j1",
			PushAddress: "127.0.0.1:9091",
			PushInterval: typeutil.Duration{
				Duration: time.Hour,
			},
		},
		{
			PushJob:     "j2",
			PushAddress: "127.0.0.1:9091",
			PushInterval: typeutil.Duration{
				Duration: zeroDuration,
			},
		},
	}

	for _, cfg := range cfgs {
		Push(ctx, cfg)
	}
}

func TestPushableGathererDropsTimestampedSamples(t *testing.T) {
	re := require.New(t)
	registry := prometheus.NewRegistry()
	counter := prometheus.NewCounter(prometheus.CounterOpts{Name: "pushed_total", Help: "pushed"})
	registry.MustRegister(counter)
	registry.MustRegister(timestampedCollector{prometheus.NewDesc("scraped_only", "scraped only", nil, nil)})
	mfs, err := registry.Gather()
	re.NoError(err)
	re.Len(mfs, 2)
	// Pushgateway rejects a whole push that carries any timestamp.
	mfs, err = pushableGatherer(registry).Gather()
	re.NoError(err)
	re.Len(mfs, 1)
	re.Equal("pushed_total", mfs[0].GetName())
}

type timestampedCollector struct{ desc *prometheus.Desc }

// Describe implements prometheus.Collector.
func (c timestampedCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

// Collect implements prometheus.Collector.
func (c timestampedCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.NewMetricWithTimestamp(time.Unix(1, 0), prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, 1))
}
