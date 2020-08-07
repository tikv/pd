// Copyright 2020 PingCAP, Inc.
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

package metrics

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"time"

	"testing"

	"github.com/pingcap/check"
	promClient "github.com/prometheus/client_golang/api"
)

const (
	mockDuration                = time.Duration(1e9)
	mockClusterName             = "mock"
	mockTiDBInstanceNamePattern = "%s-tidb-%d"
	mockTiKVInstanceNamePattern = "%s-tikv-%d"

	instanceCount = 3
)

func Test(t *testing.T) {
	check.TestingT(t)
}

var _ = check.Suite(&testPrometheusQuerierSuite{})

type testPrometheusQuerierSuite struct{}

func newPrometheusQuerierWithMockClient(client promClient.Client) *PrometheusQuerier {
	return &PrometheusQuerier{
		endpoint: "http://mock-prometheus:9090",
		client:   client,
	}
}

type normalClient struct {
	mockData map[string]*Response
}

func (c *normalClient) buildMockData() {
	results := make([]Result, 0)
	// Build TiDB Results
	for i := 0; i < instanceCount; i++ {
		results = append(results, Result{
			Value: []interface{}{"0.0", "1"},
			Metric: Metric{
				Instance: fmt.Sprintf(mockTiDBInstanceNamePattern, mockClusterName, i),
				Cluster:  mockClusterName,
			},
		})
	}

	tidbUsageQuery := fmt.Sprintf(tidbSumCPUUsageMetricsPattern, mockClusterName, mockDuration)
	tidbQuotaQuery := fmt.Sprintf(tidbSumCPUQuotaMetricsPattern, mockClusterName)
	response := &Response{
		Status: statusSuccess,
		Data: Data{
			ResultType: "value",
			Result:     results,
		},
	}
	c.mockData[tidbUsageQuery] = response
	c.mockData[tidbQuotaQuery] = response

	results = make([]Result, 0)
	// Build TiKV Results
	for i := 0; i < instanceCount; i++ {
		results = append(results, Result{
			Value: []interface{}{"0.0", "1"},
			Metric: Metric{
				Instance: fmt.Sprintf(mockTiKVInstanceNamePattern, mockClusterName, i),
				Cluster:  mockClusterName,
			},
		})
	}

	tikvUsageQuery := fmt.Sprintf(tikvSumCPUUsageMetricsPattern, mockClusterName, mockDuration)
	tikvQuotaQuery := fmt.Sprintf(tikvSumCPUQuotaMetricsPattern, mockClusterName)
	response = &Response{
		Status: statusSuccess,
		Data: Data{
			ResultType: "value",
			Result:     results,
		},
	}
	c.mockData[tikvUsageQuery] = response
	c.mockData[tikvQuotaQuery] = response
}

func (c *normalClient) URL(_ string, _ map[string]string) *url.URL {
	return nil
}

func (c *normalClient) Do(_ context.Context, req *http.Request) (*http.Response, []byte, promClient.Warnings, error) {
	query := req.URL.Query().Get("query")
	fmt.Println(query)
	respData, _ := c.mockData[query]
	data, err := json.Marshal(respData)
	if err != nil {
		return nil, []byte{}, nil, nil
	}

	response := &http.Response{
		Status:        "200 OK",
		StatusCode:    200,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Body:          ioutil.NopCloser(bytes.NewBufferString(string(data))),
		ContentLength: int64(len(data)),
		Header:        make(http.Header, 0),
	}
	response.Header.Add("Content-Type", "application/json")

	return response, data, nil, nil
}

func (s *testPrometheusQuerierSuite) TestRetrieveCPUMetrics(c *check.C) {
	client := &normalClient{
		mockData: make(map[string]*Response),
	}
	client.buildMockData()
	store := newPrometheusQuerierWithMockClient(client)
	options := NewQueryOptions("mock", TiDB, CPUUsage, []string{"mock-tidb-0", "mock-tidb-1"}, time.Now().Unix(), time.Duration(1e9))
	result, err := store.Query(options)
	c.Assert(err, check.IsNil)
	value, ok := result["mock-tidb-0"]
	c.Assert(ok, check.IsTrue)
	c.Assert(math.Abs(value-1) < 1e-6, check.IsTrue)

	value, ok = result["mock-tidb-2"]
	c.Assert(ok, check.IsFalse)
}
