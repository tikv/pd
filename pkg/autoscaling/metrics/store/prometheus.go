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
	"k8s.io/apimachinery/pkg/util/sets"
)

const (
	tikvSumStorageMetricsPattern = `sum(tikv_store_size_bytes{cluster="%s", type="%s"}) by (cluster)`
	tikvSumCPUMetricsPattern     = `sum(increase(tikv_thread_cpu_seconds_total{cluster="%s"}[%s])) by (instance)`
	tidbSumCPUMetricsPattern     = `sum(increase(process_cpu_seconds_total{cluster="%s",job="tidb"}[%s])) by (instance)`
	queryPath                    = "/api/v1/query"
	statusSuccess                = "success"

	float64EqualityThreshold = 1e-9
	httpRequestTimeout       = 5
)

// Response is used to marshal the data queried from Prometheus
type Response struct {
	Status string `json:"status"`
	Data   Data   `json:"data"`
}

type Data struct {
	ResultType string   `json:"resultType"`
	Result     []Result `json:"result"`
}

type Result struct {
	Metric Metric        `json:"metric"`
	Value  []interface{} `json:"value"`
}

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

// Query do the real query on Prometheus and returns metric value for each instance
func (prom *PrometheusStore) Query(options *QueryOptions) (QueryResult, error) {
	switch options.metric {
	case CPU:
		return prom.queryCPU(options)
	}

	return nil, errors.Errorf("unsupported metric type %v", options.metric)
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
		return nil, fmt.Errorf("query error, status code:%d", r.StatusCode)
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

	s := sets.String{}
	for _, instance := range instances {
		s.Insert(instance)
	}

	result := make(QueryResult)

	for _, r := range resp.Data.Result {
		if s.Has(r.Metric.Instance) {
			v, err := strconv.ParseFloat(r.Value[1].(string), 64)
			if err != nil {
				return nil, err
			}
			result[r.Metric.Instance] = v
		}
	}

	return result, nil
}

func (prom *PrometheusStore) queryCPU(options *QueryOptions) (QueryResult, error) {
	var query string
	switch options.member {
	case TiDB:
		query = fmt.Sprintf(tidbSumCPUMetricsPattern, options.cluster, options.duration.String())
	case TiKV:
		query = fmt.Sprintf(tikvSumCPUMetricsPattern, options.cluster, options.duration.String())
	default:
		return nil, errors.Errorf("unsupported member type %v", options.member)
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
