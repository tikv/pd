// Copyright 2020 TiKV Project Authors.
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

package datasource

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/check"
	promClient "github.com/prometheus/client_golang/api"
	types "github.com/tikv/pd/pkg/autoscaling"
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

var instanceNameTemplate = map[types.ComponentType]string{
	types.TiDB: mockTiDBInstanceNamePattern,
	types.TiKV: mockTiKVInstanceNamePattern,
}

func generateInstanceNames(component types.ComponentType) []string {
	names := make([]string, 0, instanceCount)
	pattern := instanceNameTemplate[component]
	for i := 0; i < instanceCount; i++ {
		names = append(names, fmt.Sprintf(pattern, i))
	}
	return names
}

var instanceNames = map[types.ComponentType][]string{
	types.TiDB: generateInstanceNames(types.TiDB),
	types.TiKV: generateInstanceNames(types.TiKV),
}

type testPrometheusQuerierSuite struct{}

// For building mock data only
type response struct {
	Status string `json:"status"`
	Data   data   `json:"data"`
}

type data struct {
	ResultType string   `json:"resultType"`
	Result     []result `json:"result"`
}

type result struct {
	Metric metric        `json:"metric"`
	Value  []interface{} `json:"value"`
}

type metric struct {
	Cluster  string `json:"cluster,omitempty"`
	Instance string `json:"instance"`
	Job      string `json:"job,omitempty"`
}

type normalClient struct {
	mockData map[string]*response
}

func doURL(ep string, args map[string]string) *url.URL {
	path := ep
	for k, v := range args {
		path = strings.Replace(path, ":"+k, v, -1)
	}
	u := &url.URL{
		Host: "test:9090",
		Path: path,
	}
	return u
}

func (c *normalClient) buildCPUMockData(component types.ComponentType) {
	instances := instanceNames[component]
	cpuUsageQuery := fmt.Sprintf(cpuUsagePromQLTemplate[component], mockDuration)
	cpuQuotaQuery := cpuQuotaPromQLTemplate[component]

	results := make([]result, 0)
	for i := 0; i < instanceCount; i++ {
		results = append(results, result{
			Value: []interface{}{time.Now().Unix(), fmt.Sprintf("%f", mockResultValue)},
			Metric: metric{
				Instance: instances[i],
				Cluster:  mockClusterName,
			},
		})
	}

	response := &response{
		Status: "success",
		Data: data{
			ResultType: "vector",
			Result:     results,
		},
	}

	c.mockData[cpuUsageQuery] = response
	c.mockData[cpuQuotaQuery] = response
}

func (c *normalClient) buildMockData() {
	c.buildCPUMockData(types.TiDB)
	c.buildCPUMockData(types.TiKV)
}

func makeJSONResponse(promResp *response) (*http.Response, []byte, error) {
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

func (c *normalClient) URL(ep string, args map[string]string) *url.URL {
	return doURL(ep, args)
}

func (c *normalClient) Do(_ context.Context, req *http.Request) (response *http.Response, body []byte, warnings promClient.Warnings, err error) {
	req.ParseForm()
	query := req.Form.Get("query")
	response, body, err = makeJSONResponse(c.mockData[query])
	return
}

func (s *testPrometheusQuerierSuite) TestRetrieveCPUMetrics(c *check.C) {
	client := &normalClient{
		mockData: make(map[string]*response),
	}
	client.buildMockData()
	querier := NewPrometheusQuerier(client)
	metrics := []types.MetricType{types.CPUQuota, types.CPUUsage}
	for component, instances := range instanceNames {
		for _, metric := range metrics {
			options := NewQueryOptions(component, metric, instances[:len(instances)-1], time.Now(), mockDuration)
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

func (c *emptyResponseClient) URL(ep string, args map[string]string) *url.URL {
	return doURL(ep, args)
}

func (c *emptyResponseClient) Do(_ context.Context, req *http.Request) (r *http.Response, body []byte, warnings promClient.Warnings, err error) {
	promResp := &response{
		Status: "success",
		Data: data{
			ResultType: "vector",
			Result:     make([]result, 0),
		},
	}

	r, body, err = makeJSONResponse(promResp)
	return
}

func (s *testPrometheusQuerierSuite) TestEmptyResponse(c *check.C) {
	client := &emptyResponseClient{}
	querier := NewPrometheusQuerier(client)
	options := NewQueryOptions(types.TiDB, types.CPUUsage, instanceNames[types.TiDB], time.Now(), mockDuration)
	result, err := querier.Query(options)
	c.Assert(result, check.IsNil)
	c.Assert(err, check.NotNil)
}

type errorHTTPStatusClient struct{}

func (c *errorHTTPStatusClient) URL(ep string, args map[string]string) *url.URL {
	return doURL(ep, args)
}

func (c *errorHTTPStatusClient) Do(_ context.Context, req *http.Request) (r *http.Response, body []byte, warnings promClient.Warnings, err error) {
	promResp := &response{}

	r, body, err = makeJSONResponse(promResp)

	r.StatusCode = 500
	r.Status = "500 Internal Server Error"

	return
}

func (s *testPrometheusQuerierSuite) TestErrorHTTPStatus(c *check.C) {
	client := &errorHTTPStatusClient{}
	querier := NewPrometheusQuerier(client)
	options := NewQueryOptions(types.TiDB, types.CPUUsage, instanceNames[types.TiDB], time.Now(), mockDuration)
	result, err := querier.Query(options)
	c.Assert(result, check.IsNil)
	c.Assert(err, check.NotNil)
}

type errorPrometheusStatusClient struct{}

func (c *errorPrometheusStatusClient) URL(ep string, args map[string]string) *url.URL {
	return doURL(ep, args)
}

func (c *errorPrometheusStatusClient) Do(_ context.Context, req *http.Request) (r *http.Response, body []byte, warnings promClient.Warnings, err error) {
	promResp := &response{
		Status: "error",
	}

	r, body, err = makeJSONResponse(promResp)
	return
}

func (s *testPrometheusQuerierSuite) TestErrorPrometheusStatus(c *check.C) {
	client := &errorPrometheusStatusClient{}
	querier := NewPrometheusQuerier(client)
	options := NewQueryOptions(types.TiDB, types.CPUUsage, instanceNames[types.TiDB], time.Now(), mockDuration)
	result, err := querier.Query(options)
	c.Assert(result, check.IsNil)
	c.Assert(err, check.NotNil)
}
