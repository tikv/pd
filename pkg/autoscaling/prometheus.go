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
	"context"
	"fmt"
	"math"
	"time"

	"github.com/pingcap/log"
	promClient "github.com/prometheus/client_golang/api"
	promAPI "github.com/prometheus/client_golang/api/prometheus/v1"
	promModel "github.com/prometheus/common/model"
	"github.com/tikv/pd/pkg/errs"
	"go.uber.org/zap"
)

const (
	tikvSumCPUUsageMetricsPattern = `sum(increase(tikv_thread_cpu_seconds_total[%s])) by (instance, kubernetes_namespace)`
	tidbSumCPUUsageMetricsPattern = `sum(increase(process_cpu_seconds_total{component="tidb"}[%s])) by (instance, kubernetes_namespace)`
	tikvCPUQuotaMetricsPattern    = `tikv_server_cpu_cores_quota`
	tidbCPUQuotaMetricsPattern    = `tidb_server_maxprocs`
	instanceLabelKey              = "instance"
	namespaceLabelKey             = "kubernetes_namespace"

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

var queryBuilderFnMap = map[MetricType]promQLBuilderFn{
	CPUQuota: buildCPUQuotaPromQL,
	CPUUsage: buildCPUUsagePromQL,
}

// Query do the real query on Prometheus and returns metric value for each instance
func (prom *PrometheusQuerier) Query(options *QueryOptions) (QueryResult, error) {
	builderFn, ok := queryBuilderFnMap[options.metric]
	if !ok {
		return nil, errs.ErrUnsupportedMetricsType.FastGenByArgs(options.metric)
	}

	query, err := builderFn(options)
	if err != nil {
		return nil, err
	}

	resp, err := prom.queryMetricsFromPrometheus(query, options.timestamp)
	if err != nil {
		return nil, err
	}

	result, err := extractInstancesFromResponse(resp)
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
		return nil, errs.ErrPrometheusQuery.Wrap(err).FastGenWithCause()
	}

	if warnings != nil && len(warnings) > 0 {
		log.Warn("prometheus query returns with warnings", zap.Strings("warnings", warnings))
	}

	return resp, nil
}

func extractInstancesFromResponse(resp promModel.Value) (QueryResult, error) {
	if resp == nil {
		return nil, errs.ErrEmptyMetricsResponse.FastGenByArgs()
	}

	if resp.Type() != promModel.ValVector {
		return nil, errs.ErrUnexpectedType.FastGenByArgs(resp.Type().String())
	}

	vector, ok := resp.(promModel.Vector)

	if !ok {
		return nil, errs.ErrTypeConversion.FastGenByArgs()
	}

	if len(vector) == 0 {
		return nil, errs.ErrEmptyMetricsResult.FastGenByArgs("query metrics duration must be at least twice the Prometheus scrape interval")
	}

	var (
		instanceName string
		namespace    string
	)

	result := make(QueryResult)

	for _, sample := range vector {
		instanceLabel, ok := sample.Metric[instanceLabelKey]
		if ok {
			instanceName = string(instanceLabel)
		}
		namespaceLabel, ok := sample.Metric[namespaceLabelKey]
		if ok {
			namespace = string(namespaceLabel)
		}

		instanceFullName := buildInstanceIdentifier(instanceName, namespace)
		result[instanceFullName] = float64(sample.Value)
	}

	return result, nil
}

var cpuUsagePromQLTemplate = map[ComponentType]string{
	TiDB: tidbSumCPUUsageMetricsPattern,
	TiKV: tikvSumCPUUsageMetricsPattern,
}

var cpuQuotaPromQLTemplate = map[ComponentType]string{
	TiDB: tidbCPUQuotaMetricsPattern,
	TiKV: tikvCPUQuotaMetricsPattern,
}

func buildCPUQuotaPromQL(options *QueryOptions) (string, error) {
	pattern, ok := cpuQuotaPromQLTemplate[options.component]
	if !ok {
		return "", errs.ErrUnsupportedComponentType.FastGenByArgs(options.component)
	}

	query := pattern
	return query, nil
}

func buildCPUUsagePromQL(options *QueryOptions) (string, error) {
	pattern, ok := cpuUsagePromQLTemplate[options.component]
	if !ok {
		return "", errs.ErrUnsupportedComponentType.FastGenByArgs(options.component)
	}

	query := fmt.Sprintf(pattern, getDurationExpression(options.duration))
	return query, nil
}

func buildInstanceIdentifier(podName string, namespace string) string {
	return fmt.Sprintf("%s_%s", podName, namespace)
}

func getDurationExpression(duration time.Duration) string {
	// Prometheus only accept single unit duration like 10s, 2m
	// and the time.Duration.String() method returns the duration like 2m0s, 2m30s,
	// so we need to express the duration in seconds like 120s
	seconds := int64(math.Floor(duration.Seconds()))
	return fmt.Sprintf("%ds", seconds)
}
