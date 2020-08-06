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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/pingcap/log"
	"github.com/pkg/errors"
	promClient "github.com/prometheus/client_golang/api"
)

const (
	tikvSumCPUUsageMetricsPattern = `sum(increase(tikv_thread_cpu_seconds_total{cluster="%s"}[%s])) by (instance)`
	tidbSumCPUUsageMetricsPattern = `sum(increase(process_cpu_seconds_total{cluster="%s",job="tidb"}[%s])) by (instance)`
	tikvSumCPUQuotaMetricsPattern = `tikv_server_cpu_cores_quota{cluster="%s"}`
	tidbSumCPUQuotaMetricsPattern = `tidb_server_maxprocs{cluster="%s"}`
	queryPath                     = "/api/v1/query"
	statusSuccess                 = "success"

	httpRequestTimeout = 5
)

// Response is used to marshal the data queried from Prometheus
type Response struct {
	Status string `json:"status"`
	Data   Data   `json:"data"`
}

// Data consists of response data from prometheus
type Data struct {
	ResultType string   `json:"resultType"`
	Result     []Result `json:"result"`
}

// Result consists of value and its labels
type Result struct {
	Metric Metric        `json:"metric"`
	Value  []interface{} `json:"value"`
}

// Metric consists of labels
type Metric struct {
	Cluster  string `json:"cluster,omitempty"`
	Instance string `json:"instance"`
	Job      string `json:"job,omitempty"`
}

// PrometheusStore query metrics from Prometheus
type PrometheusStore struct {
	// Prometheus API Endpoint Address
	endpoint string
	client   promClient.Client
}

// NewPrometheusStore returns a PrometheusStore
func NewPrometheusStore(endpoint string) (*PrometheusStore, error) {
	client, err := promClient.NewClient(promClient.Config{Address: endpoint})
	if err != nil {
		return nil, err
	}

	store := &PrometheusStore{
		endpoint,
		client,
	}

	return store, nil
}

type promQLBuilderFn func(*QueryOptions) (string, error)

var queryBuilderFnMap = map[MetricType]promQLBuilderFn{
	CPUQuota: buildCPUQuotaPromQL,
	CPUUsage: buildCPUUsagePromQL,
}

// Query do the real query on Prometheus and returns metric value for each instance
func (prom *PrometheusStore) Query(options *QueryOptions) (QueryResult, error) {
	builderFn, ok := queryBuilderFnMap[options.metric]
	if !ok {
		return nil, errors.Errorf("unsupported metric type %v", options.metric)
	}

	query, err := builderFn(options)
	if err != nil {
		return nil, err
	}

	resp, err := prom.queryMetricsFromPrometheus(query, options.timestamp)
	if err != nil {
		return nil, err
	}

	result, err := extractInstancesFromResponse(resp, options.instances)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (prom *PrometheusStore) queryMetricsFromPrometheus(query string, timestamp int64) (*Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*httpRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s%s", prom.endpoint, queryPath), nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Add("query", query)
	q.Add("time", fmt.Sprintf("%d", timestamp))
	req.URL.RawQuery = q.Encode()
	r, body, _, err := prom.client.Do(req.Context(), req)
	if err != nil {
		log.Info(err.Error())
		return nil, err
	}

	if r.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("query error, status code: %d", r.StatusCode)
	}

	resp := &Response{}
	err = json.Unmarshal(body, resp)
	if err != nil {
		return nil, err
	}
	if resp.Status != statusSuccess {
		return resp, fmt.Errorf("query error, response status: %v", resp.Status)
	}

	return resp, nil
}

func extractInstancesFromResponse(resp *Response, instances []string) (QueryResult, error) {
	if resp == nil {
		return nil, errors.Errorf("metrics response from Prometheus is empty")
	}

	if len(resp.Data.Result) < 1 {
		return nil, fmt.Errorf("metrics Response returns no info")
	}

	s := map[string]struct{}{}
	for _, instance := range instances {
		s[instance] = struct{}{}
	}

	result := make(QueryResult)

	for _, r := range resp.Data.Result {
		if _, ok := s[r.Metric.Instance]; ok {
			v, err := strconv.ParseFloat(r.Value[1].(string), 64)
			if err != nil {
				return nil, err
			}
			result[r.Metric.Instance] = v
		}
	}

	return result, nil
}

var cpuUsagePromQLTemplate = map[MemberType]string{
	TiDB: tidbSumCPUUsageMetricsPattern,
	TiKV: tikvSumCPUUsageMetricsPattern,
}

var cpuQuotaPromQLTemplate = map[MemberType]string{
	TiDB: tidbSumCPUQuotaMetricsPattern,
	TiKV: tikvSumCPUQuotaMetricsPattern,
}

func buildCPUQuotaPromQL(options *QueryOptions) (string, error) {
	pattern, ok := cpuQuotaPromQLTemplate[options.member]
	if !ok {
		return "", errors.Errorf("unspported member type %v", options.member)
	}

	query := fmt.Sprintf(pattern, options.cluster, options.duration.String())
	return query, nil
}

func buildCPUUsagePromQL(options *QueryOptions) (string, error) {
	pattern, ok := cpuUsagePromQLTemplate[options.member]
	if !ok {
		return "", errors.Errorf("unspported member type %v", options.member)
	}

	query := fmt.Sprintf(pattern, options.cluster)
	return query, nil
}
