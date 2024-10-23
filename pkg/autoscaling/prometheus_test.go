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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package autoscaling

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	mockDuration                = time.Second
	mockClusterName             = "mock"
	mockTiDBInstanceNamePattern = "%s-tidb-%d"
	mockTiKVInstanceNamePattern = "%s-tikv-%d"
	mockResultValue             = 1.0
	mockKubernetesNamespace     = "mock"

	instanceCount = 3
)

var podNameTemplate = map[ComponentType]string{
	TiDB: mockTiDBInstanceNamePattern,
	TiKV: mockTiKVInstanceNamePattern,
}

func generatePodNames(component ComponentType) []string {
	names := make([]string, 0, instanceCount)
	pattern := podNameTemplate[component]
	for i := range instanceCount {
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
	Metric metric `json:"metric"`
	Value  []any  `json:"value"`
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
		path = strings.ReplaceAll(path, ":"+k, v)
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

	var results []result
	for i := range instanceCount {
		results = append(results, result{
			Value: []any{time.Now().Unix(), fmt.Sprintf("%f", mockResultValue)},
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
		StatusCode:    http.StatusOK,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Body:          io.NopCloser(bytes.NewBufferString(string(body))),
		ContentLength: int64(len(body)),
		Header:        make(http.Header),
	}
	response.Header.Add("Content-Type", "application/json")

	return response, body, nil
}

func (*normalClient) URL(ep string, args map[string]string) *url.URL {
	return doURL(ep, args)
}

func (c *normalClient) Do(_ context.Context, req *http.Request) (response *http.Response, body []byte, err error) {
	req.ParseForm()
	query := req.Form.Get("query")
	response, body, err = makeJSONResponse(c.mockData[query])
	return
}

func TestRetrieveCPUMetrics(t *testing.T) {
	re := require.New(t)
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
			re.NoError(err)
			for i := range len(addresses) - 1 {
				value, ok := result[addresses[i]]
				re.True(ok)
				re.Less(math.Abs(value-mockResultValue), 1e-6)
			}

			_, ok := result[addresses[len(addresses)-1]]
			re.False(ok)
		}
	}
}

type emptyResponseClient struct{}

func (*emptyResponseClient) URL(ep string, args map[string]string) *url.URL {
	return doURL(ep, args)
}

func (*emptyResponseClient) Do(context.Context, *http.Request) (r *http.Response, body []byte, err error) {
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

func TestEmptyResponse(t *testing.T) {
	re := require.New(t)
	client := &emptyResponseClient{}
	querier := NewPrometheusQuerier(client)
	options := NewQueryOptions(TiDB, CPUUsage, podAddresses[TiDB], time.Now(), mockDuration)
	result, err := querier.Query(options)
	re.Nil(result)
	re.Error(err)
}

type errorHTTPStatusClient struct{}

func (*errorHTTPStatusClient) URL(ep string, args map[string]string) *url.URL {
	return doURL(ep, args)
}

func (*errorHTTPStatusClient) Do(context.Context, *http.Request) (r *http.Response, body []byte, err error) {
	promResp := &response{}

	r, body, err = makeJSONResponse(promResp)

	r.StatusCode = http.StatusInternalServerError
	r.Status = "500 Internal Server Error"

	return
}

func TestErrorHTTPStatus(t *testing.T) {
	re := require.New(t)
	client := &errorHTTPStatusClient{}
	querier := NewPrometheusQuerier(client)
	options := NewQueryOptions(TiDB, CPUUsage, podAddresses[TiDB], time.Now(), mockDuration)
	result, err := querier.Query(options)
	re.Nil(result)
	re.Error(err)
}

type errorPrometheusStatusClient struct{}

func (*errorPrometheusStatusClient) URL(ep string, args map[string]string) *url.URL {
	return doURL(ep, args)
}

func (*errorPrometheusStatusClient) Do(_ context.Context, _ *http.Request) (r *http.Response, body []byte, err error) {
	promResp := &response{
		Status: "error",
	}

	r, body, err = makeJSONResponse(promResp)
	return
}

func TestErrorPrometheusStatus(t *testing.T) {
	re := require.New(t)
	client := &errorPrometheusStatusClient{}
	querier := NewPrometheusQuerier(client)
	options := NewQueryOptions(TiDB, CPUUsage, podAddresses[TiDB], time.Now(), mockDuration)
	result, err := querier.Query(options)
	re.Nil(result)
	re.Error(err)
}

func TestGetInstanceNameFromAddress(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
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
			address:              "127.0.0.1:2333",
			expectedInstanceName: "",
		},
		{
			address:              "127.0.0.1",
			expectedInstanceName: "",
		},
	}
	for _, testCase := range testCases {
		instanceName, err := getInstanceNameFromAddress(testCase.address)
		if testCase.expectedInstanceName == "" {
			re.Error(err)
		} else {
			re.Equal(testCase.expectedInstanceName, instanceName)
		}
	}
}

func TestGetDurationExpression(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		duration           time.Duration
		expectedExpression string
	}{
		{
			duration:           30 * time.Second,
			expectedExpression: "30s",
		},
		{
			duration:           time.Minute,
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

	for _, testCase := range testCases {
		expression := getDurationExpression(testCase.duration)
		re.Equal(testCase.expectedExpression, expression)
	}
}
