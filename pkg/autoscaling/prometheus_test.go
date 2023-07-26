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

package autoscaling

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

	. "github.com/pingcap/check"
	promClient "github.com/prometheus/client_golang/api"
)

const (
	mockDuration                = 1 * time.Second
	mockClusterName             = "mock"
	mockTiDBInstanceNamePattern = "%s-tidb-%d"
	mockTiKVInstanceNamePattern = "%s-tikv-%d"
	mockResultValue             = 1.0
	mockKubernetesNamespace     = "mock"

	instanceCount = 3
)

func TestPrometheus(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testPrometheusQuerierSuite{})

var podNameTemplate = map[ComponentType]string{
	TiDB: mockTiDBInstanceNamePattern,
	TiKV: mockTiKVInstanceNamePattern,
}

func generatePodNames(component ComponentType) []string {
	names := make([]string, 0, instanceCount)
	pattern := podNameTemplate[component]
	for i := 0; i < instanceCount; i++ {
		names = append(names, fmt.Sprintf(pattern, mockClusterName, i))
	}
	return names
}

var podNames = map[ComponentType][]string{
	TiDB: generatePodNames(TiDB),
	TiKV: generatePodNames(TiKV),
}

func generateAddresses(component ComponentType) []string {
	pods := podNames[component]
	addresses := make([]string, 0, len(pods))
	for _, pod := range pods {
		addresses = append(addresses, fmt.Sprintf("%s.%s-%s-peer.%s.svc:20080", pod, mockClusterName, component.String(), mockKubernetesNamespace))
	}
	return addresses
}

var podAddresses = map[ComponentType][]string{
	TiDB: generateAddresses(TiDB),
	TiKV: generateAddresses(TiKV),
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
	Cluster             string `json:"cluster,omitempty"`
	Instance            string `json:"instance"`
	Job                 string `json:"job,omitempty"`
	KubernetesNamespace string `json:"kubernetes_namespace"`
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

func (c *normalClient) buildCPUMockData(component ComponentType) {
	pods := podNames[component]
	cpuUsageQuery := fmt.Sprintf(cpuUsagePromQLTemplate[component], mockDuration)
	cpuQuotaQuery := cpuQuotaPromQLTemplate[component]

	results := make([]result, 0)
	for i := 0; i < instanceCount; i++ {
		results = append(results, result{
			Value: []interface{}{time.Now().Unix(), fmt.Sprintf("%f", mockResultValue)},
			Metric: metric{
				Instance:            pods[i],
				Cluster:             mockClusterName,
				KubernetesNamespace: mockKubernetesNamespace,
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
	c.buildCPUMockData(TiDB)
	c.buildCPUMockData(TiKV)
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

func (s *testPrometheusQuerierSuite) TestRetrieveCPUMetrics(c *C) {
	client := &normalClient{
		mockData: make(map[string]*response),
	}
	client.buildMockData()
	querier := NewPrometheusQuerier(client)
	metrics := []MetricType{CPUQuota, CPUUsage}
	for component, addresses := range podAddresses {
		for _, metric := range metrics {
			options := NewQueryOptions(component, metric, addresses[:len(addresses)-1], time.Now(), mockDuration)
			result, err := querier.Query(options)
			c.Assert(err, IsNil)
			for i := 0; i < len(addresses)-1; i++ {
				value, ok := result[addresses[i]]
				c.Assert(ok, IsTrue)
				c.Assert(math.Abs(value-mockResultValue) < 1e-6, IsTrue)
			}

			_, ok := result[addresses[len(addresses)-1]]
			c.Assert(ok, IsFalse)
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

func (s *testPrometheusQuerierSuite) TestEmptyResponse(c *C) {
	client := &emptyResponseClient{}
	querier := NewPrometheusQuerier(client)
	options := NewQueryOptions(TiDB, CPUUsage, podAddresses[TiDB], time.Now(), mockDuration)
	result, err := querier.Query(options)
	c.Assert(result, IsNil)
	c.Assert(err, NotNil)
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

func (s *testPrometheusQuerierSuite) TestErrorHTTPStatus(c *C) {
	client := &errorHTTPStatusClient{}
	querier := NewPrometheusQuerier(client)
	options := NewQueryOptions(TiDB, CPUUsage, podAddresses[TiDB], time.Now(), mockDuration)
	result, err := querier.Query(options)
	c.Assert(result, IsNil)
	c.Assert(err, NotNil)
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

func (s *testPrometheusQuerierSuite) TestErrorPrometheusStatus(c *C) {
	client := &errorPrometheusStatusClient{}
	querier := NewPrometheusQuerier(client)
	options := NewQueryOptions(TiDB, CPUUsage, podAddresses[TiDB], time.Now(), mockDuration)
	result, err := querier.Query(options)
	c.Assert(result, IsNil)
	c.Assert(err, NotNil)
}

func (s *testPrometheusQuerierSuite) TestGetInstanceNameFromAddress(c *C) {
	testcases := []struct {
		address              string
		expectedInstanceName string
	}{
		{
			address:              "test-tikv-0.test-tikv-peer.namespace.svc:20080",
			expectedInstanceName: "test-tikv-0_namespace",
		},
		{
			address:              "test-tikv-0.test-tikv-peer.namespace.svc",
			expectedInstanceName: "test-tikv-0_namespace",
		},
		{
			address:              "tidb-0_10080",
			expectedInstanceName: "",
		},
		{
			address:              "1.2.3.4:2333",
			expectedInstanceName: "",
		},
		{
			address:              "1.2.3.4",
			expectedInstanceName: "",
		},
	}
	for _, testcase := range testcases {
		instanceName, err := getInstanceNameFromAddress(testcase.address)
		if testcase.expectedInstanceName == "" {
			c.Assert(err, NotNil)
		} else {
			c.Assert(instanceName, Equals, testcase.expectedInstanceName)
		}
	}
}

func (s *testPrometheusQuerierSuite) TestGetDurationExpression(c *C) {
	testcases := []struct {
		duration           time.Duration
		expectedExpression string
	}{
		{
			duration:           30 * time.Second,
			expectedExpression: "30s",
		},
		{
			duration:           60 * time.Second,
			expectedExpression: "60s",
		},
		{
			duration:           2 * time.Minute,
			expectedExpression: "120s",
		},
		{
			duration:           90 * time.Second,
			expectedExpression: "90s",
		},
	}

	for _, testcase := range testcases {
		expression := getDurationExpression(testcase.duration)
		c.Assert(expression, Equals, testcase.expectedExpression)
	}
}
