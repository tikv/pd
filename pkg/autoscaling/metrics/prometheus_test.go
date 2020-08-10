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

var instanceNameTemplate = map[ComponentType]string{
	TiDB: mockTiDBInstanceNamePattern,
	TiKV: mockTiKVInstanceNamePattern,
}

func generateInstanceNames(component ComponentType) []string {
	names := make([]string, 0, instanceCount)
	pattern := instanceNameTemplate[component]
	for i := 0; i < instanceCount; i++ {
		names = append(names, fmt.Sprintf(pattern, mockClusterName, i))
	}
	return names
}

var instanceNames = map[ComponentType][]string{
	TiDB: generateInstanceNames(TiDB),
	TiKV: generateInstanceNames(TiKV),
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

func (c *normalClient) buildCPUMockData(component ComponentType) {
	instances := instanceNames[component]
	cpuUsageQuery := fmt.Sprintf(cpuUsagePromQLTemplate[component], mockClusterName, mockDuration)
	cpuQuotaQuery := fmt.Sprintf(cpuQuotaPromQLTemplate[component], mockClusterName)

	results := make([]Result, 0)
	for i := 0; i < instanceCount; i++ {
		results = append(results, Result{
			Value: []interface{}{time.Now().Unix(), fmt.Sprintf("%f", mockResultValue)},
			Metric: Metric{
				Instance: instances[i],
				Cluster:  mockClusterName,
			},
		})
	}

	response := &Response{
		Status: statusSuccess,
		Data: Data{
			ResultType: "vector",
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
		Header:        make(http.Header),
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
	metrics := []MetricType{CPUQuota, CPUUsage}
	for component, instances := range instanceNames {
		for _, metric := range metrics {
			options := NewQueryOptions(mockClusterName, component, metric, instances[:len(instances)-1], time.Now().Unix(), mockDuration)
			result, err := querier.Query(options)
			c.Assert(err, check.IsNil)
			for i := 0; i < len(instances)-1; i++ {
				value, ok := result[instances[i]]
				c.Assert(ok, check.IsTrue)
				c.Assert(math.Abs(value-mockResultValue) < 1e-6, check.IsTrue)
			}

			_, ok := result[instances[len(instances)-1]]
			c.Assert(ok, check.IsFalse)
		}
	}
}

type emptyResponseClient struct{}

func (c *emptyResponseClient) URL(_ string, _ map[string]string) *url.URL {
	return nil
}

func (c *emptyResponseClient) Do(_ context.Context, req *http.Request) (response *http.Response, body []byte, warnings promClient.Warnings, err error) {
	promResp := &Response{
		Status: statusSuccess,
		Data: Data{
			ResultType: "vector",
			Result:     make([]Result, 0),
		},
	}

	response, body, err = makeJSONResponse(promResp)
	return
}

func (s *testPrometheusQuerierSuite) TestEmptyResponse(c *check.C) {
	client := &emptyResponseClient{}
	querier := newPrometheusQuerierWithMockClient(client)
	options := NewQueryOptions(mockClusterName, TiDB, CPUUsage, instanceNames[TiDB], time.Now().Unix(), mockDuration)
	result, err := querier.Query(options)
	c.Assert(result, check.IsNil)
	c.Assert(err, check.NotNil)
}

type errorHTTPStatusClient struct{}

func (c *errorHTTPStatusClient) URL(_ string, _ map[string]string) *url.URL {
	return nil
}

func (c *errorHTTPStatusClient) Do(_ context.Context, req *http.Request) (response *http.Response, body []byte, warnings promClient.Warnings, err error) {
	promResp := &Response{}

	response, body, err = makeJSONResponse(promResp)

	response.StatusCode = 500
	response.Status = "500 Internal Server Error"

	return
}

func (s *testPrometheusQuerierSuite) TestErrorHTTPStatus(c *check.C) {
	client := &errorHTTPStatusClient{}
	querier := newPrometheusQuerierWithMockClient(client)
	options := NewQueryOptions(mockClusterName, TiDB, CPUUsage, instanceNames[TiDB], time.Now().Unix(), mockDuration)
	result, err := querier.Query(options)
	c.Assert(result, check.IsNil)
	c.Assert(err, check.NotNil)
}

type errorPrometheusStatusClient struct{}

func (c *errorPrometheusStatusClient) URL(_ string, _ map[string]string) *url.URL {
	return nil
}

func (c *errorPrometheusStatusClient) Do(_ context.Context, req *http.Request) (response *http.Response, body []byte, warnings promClient.Warnings, err error) {
	promResp := &Response{
		Status: "error",
	}

	response, body, err = makeJSONResponse(promResp)
	return
}

func (s *testPrometheusQuerierSuite) TestErrorPrometheusStatus(c *check.C) {
	client := &errorPrometheusStatusClient{}
	querier := newPrometheusQuerierWithMockClient(client)
	options := NewQueryOptions(mockClusterName, TiDB, CPUUsage, instanceNames[TiDB], time.Now().Unix(), time.Duration(1e9))
	result, err := querier.Query(options)
	c.Assert(result, check.IsNil)
	c.Assert(err, check.NotNil)
}
