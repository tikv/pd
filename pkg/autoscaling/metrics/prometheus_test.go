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
	mockResultValue             = 1.0

	instanceCount = 3
)

func Test(t *testing.T) {
	check.TestingT(t)
}

var _ = check.Suite(&testPrometheusQuerierSuite{})

var instanceNameTemplate = map[MemberType]string{
	TiDB: mockTiDBInstanceNamePattern,
	TiKV: mockTiKVInstanceNamePattern,
}

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

func (c *normalClient) buildCPUMockData(member MemberType) {
	instanceNamePattern := instanceNameTemplate[member]
	cpuUsageQuery := fmt.Sprintf(cpuUsagePromQLTemplate[member], mockClusterName, mockDuration)
	cpuQuotaQuery := fmt.Sprintf(cpuQuotaPromQLTemplate[member], mockClusterName)

	results := make([]Result, 0)
	for i := 0; i < instanceCount; i++ {
		results = append(results, Result{
			Value: []interface{}{"0.0", "1"},
			Metric: Metric{
				Instance: fmt.Sprintf(instanceNamePattern, mockClusterName, i),
				Cluster:  mockClusterName,
			},
		})
	}

	response := &Response{
		Status: statusSuccess,
		Data: Data{
			ResultType: "value",
			Result:     results,
		},
	}

	c.mockData[cpuUsageQuery] = response
	c.mockData[cpuQuotaQuery] = response
}

func (c *normalClient) buildMockData() {
	c.buildCPUMockData(TiDB)
	c.buildCPUMockData(TiKV)
}

func makeJSONResponse(promResp *Response) (*http.Response, []byte, error) {
	body, err := json.Marshal(promResp)
	if err != nil {
		return nil, []byte{}, err
	}

	response := &http.Response{
		Status:        "200 OK",
		StatusCode:    200,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Body:          ioutil.NopCloser(bytes.NewBufferString(string(body))),
		ContentLength: int64(len(body)),
		Header:        make(http.Header, 0),
	}
	response.Header.Add("Content-Type", "application/json")

	return response, body, nil
}

func (c *normalClient) URL(_ string, _ map[string]string) *url.URL {
	return nil
}

func (c *normalClient) Do(_ context.Context, req *http.Request) (response *http.Response, body []byte, warnings promClient.Warnings, err error) {
	query := req.URL.Query().Get("query")
	response, body, err = makeJSONResponse(c.mockData[query])
	return
}

func (s *testPrometheusQuerierSuite) TestRetrieveCPUMetrics(c *check.C) {
	client := &normalClient{
		mockData: make(map[string]*Response),
	}
	client.buildMockData()
	querier := newPrometheusQuerierWithMockClient(client)
	options := NewQueryOptions(mockClusterName, TiDB, CPUUsage, []string{"mock-tidb-0", "mock-tidb-1"}, time.Now().Unix(), mockDuration)
	result, err := querier.Query(options)
	c.Assert(err, check.IsNil)
	value, ok := result["mock-tidb-0"]
	c.Assert(ok, check.IsTrue)
	c.Assert(math.Abs(value-mockResultValue) < 1e-6, check.IsTrue)

	// Instance should be filtered
	value, ok = result["mock-tidb-2"]
	c.Assert(ok, check.IsFalse)
}

type emptyResponseClient struct{}

func (c *emptyResponseClient) URL(_ string, _ map[string]string) *url.URL {
	return nil
}

func (c *emptyResponseClient) Do(_ context.Context, req *http.Request) (response *http.Response, body []byte, warnings promClient.Warnings, err error) {
	promResp := &Response{
		Status: statusSuccess,
		Data: Data{
			ResultType: "value",
			Result:     make([]Result, 0),
		},
	}

	response, body, err = makeJSONResponse(promResp)
	return
}

func (s *testPrometheusQuerierSuite) TestEmptyResponse(c *check.C) {
	client := &emptyResponseClient{}
	querier := newPrometheusQuerierWithMockClient(client)
	options := NewQueryOptions(mockClusterName, TiDB, CPUUsage, []string{"mock-tidb-0", "mock-tidb-1"}, time.Now().Unix(), time.Duration(1e9))
	result, err := querier.Query(options)
	c.Assert(result, check.IsNil)
	c.Assert(err, check.NotNil)
}
