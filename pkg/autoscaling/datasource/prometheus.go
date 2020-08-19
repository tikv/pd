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
	"context"
	"fmt"
	"time"

	"github.com/pingcap/log"
	"github.com/pkg/errors"
	promClient "github.com/prometheus/client_golang/api"
	promAPI "github.com/prometheus/client_golang/api/prometheus/v1"
	promModel "github.com/prometheus/common/model"
	types "github.com/tikv/pd/pkg/autoscaling"
	"go.uber.org/zap"
)

const (
	tikvSumCPUUsageMetricsPattern = `sum(increase(tikv_thread_cpu_seconds_total[%s])) by (instance)`
	tidbSumCPUUsageMetricsPattern = `sum(increase(process_cpu_seconds_total{job="tidb"}[%s])) by (instance)`
	tikvCPUQuotaMetricsPattern    = `tikv_server_cpu_cores_quota`
	tidbCPUQuotaMetricsPattern    = `tidb_server_maxprocs`
	instanceLabelName             = "instance"

	httpRequestTimeout = 5 * time.Second
)

// PrometheusQuerier query metrics from Prometheus
type PrometheusQuerier struct {
	api promAPI.API
}

// NewPrometheusQuerier returns a PrometheusQuerier
func NewPrometheusQuerier(client promClient.Client) *PrometheusQuerier {
	return &PrometheusQuerier{
		api: promAPI.NewAPI(client),
	}
}

type promQLBuilderFn func(*QueryOptions) (string, error)

var queryBuilderFnMap = map[types.MetricType]promQLBuilderFn{
	types.CPUQuota: buildCPUQuotaPromQL,
	types.CPUUsage: buildCPUUsagePromQL,
}

// Query do the real query on Prometheus and returns metric value for each instance
func (prom *PrometheusQuerier) Query(options *QueryOptions) (QueryResult, error) {
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

func (prom *PrometheusQuerier) queryMetricsFromPrometheus(query string, timestamp time.Time) (promModel.Value, error) {
	ctx, cancel := context.WithTimeout(context.Background(), httpRequestTimeout)
	defer cancel()

	resp, warnings, err := prom.api.Query(ctx, query, timestamp)

	if err != nil {
		return nil, err
	}

	if warnings != nil && len(warnings) > 0 {
		log.Warn("prometheus query returns with warnings", zap.Strings("warnings", warnings))
	}

	return resp, nil
}

func extractInstancesFromResponse(resp promModel.Value, instances []string) (QueryResult, error) {
	if resp == nil {
		return nil, errors.New("metrics response from Prometheus is empty")
	}

	if resp.Type() != promModel.ValVector {
		return nil, errors.Errorf("expected vector type values, got %s", resp.Type().String())
	}

	vector, ok := resp.(promModel.Vector)

	if !ok {
		return nil, errors.New("type conversion error")
	}

	if len(vector) == 0 {
		return nil, errors.New("no results returned from Prometheus")
	}

	instancesSet := map[string]struct{}{}
	for _, instance := range instances {
		instancesSet[instance] = struct{}{}
	}

	result := make(QueryResult)

	for _, sample := range vector {
		if instance, ok := sample.Metric[instanceLabelName]; ok {
			if _, ok := instancesSet[string(instance)]; ok {
				result[string(instance)] = float64(sample.Value)
			}
		}
	}

	return result, nil
}

var cpuUsagePromQLTemplate = map[types.ComponentType]string{
	types.TiDB: tidbSumCPUUsageMetricsPattern,
	types.TiKV: tikvSumCPUUsageMetricsPattern,
}

var cpuQuotaPromQLTemplate = map[types.ComponentType]string{
	types.TiDB: tidbCPUQuotaMetricsPattern,
	types.TiKV: tikvCPUQuotaMetricsPattern,
}

func buildCPUQuotaPromQL(options *QueryOptions) (string, error) {
	pattern, ok := cpuQuotaPromQLTemplate[options.component]
	if !ok {
		return "", errors.Errorf("unsupported component type %v", options.component)
	}

	query := pattern
	return query, nil
}

func buildCPUUsagePromQL(options *QueryOptions) (string, error) {
	pattern, ok := cpuUsagePromQLTemplate[options.component]
	if !ok {
		return "", errors.Errorf("unsupported component type %v", options.component)
	}

	query := fmt.Sprintf(pattern, options.duration.String())
	return query, nil
}
