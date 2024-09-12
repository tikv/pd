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
	"net"
	"strings"
	"time"

	"github.com/pingcap/errors"
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
	instanceLabelName             = "instance"
	namespaceLabelName            = "kubernetes_namespace"
	addressFormat                 = "pod-name.peer-svc.namespace.svc:port"

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

	result, err := extractInstancesFromResponse(resp, options.addresses)
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
		return nil, errs.ErrPrometheusQuery.Wrap(err)
	}

	if len(warnings) > 0 {
		log.Warn("prometheus query returns with warnings", zap.Strings("warnings", warnings))
	}

	return resp, nil
}

func extractInstancesFromResponse(resp promModel.Value, addresses []string) (QueryResult, error) {
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

	instancesSet := map[string]string{}
	for _, addr := range addresses {
		instanceName, err := getInstanceNameFromAddress(addr)
		if err == nil {
			instancesSet[instanceName] = addr
		}
	}

	result := make(QueryResult)

	for _, sample := range vector {
		podName, ok := sample.Metric[instanceLabelName]
		if !ok {
			continue
		}

		namespace, ok := sample.Metric[namespaceLabelName]
		if !ok {
			continue
		}

		instanceName := buildInstanceIdentifier(string(podName), string(namespace))

		if addr, ok := instancesSet[instanceName]; ok {
			result[addr] = float64(sample.Value)
		}
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

// this function assumes that addr is already a valid resolvable address
// returns in format "podname_namespace"
func getInstanceNameFromAddress(addr string) (string, error) {
	// In K8s, a StatefulSet pod address is composed of pod-name.peer-svc.namespace.svc:port
	// Extract the hostname part without port
	hostname := addr
	portColonIdx := strings.LastIndex(addr, ":")
	if portColonIdx >= 0 {
		hostname = addr[:portColonIdx]
	}

	// Just to make sure it is not an IP address
	ip := net.ParseIP(hostname)
	if ip != nil {
		// Hostname is an IP address, return the whole address
		return "", errors.Errorf("address %s is an ip address", addr)
	}

	parts := strings.Split(hostname, ".")
	if len(parts) < 4 {
		return "", errors.Errorf("address %s does not match the expected format %s", addr, addressFormat)
	}

	podName, namespace := parts[0], parts[2]

	return buildInstanceIdentifier(podName, namespace), nil
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
