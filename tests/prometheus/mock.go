// Copyright 2021 TiKV Project Authors.
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

package prometheus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/tikv/pd/pkg/autoscaling"
)

const (
	mockDuration                = 60 * time.Second
	mockClusterName             = "mock"
	mockTiDBInstanceNamePattern = "%s-tidb-%d"
	mockTiKVInstanceNamePattern = "%s-tikv-%d"
	mockResultValue             = 1.0
	mockKubernetesNamespace     = "mock"

	instanceCount = 3
)

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

func getComponentType(query string) autoscaling.ComponentType {
	if strings.Contains(query, autoscaling.TiKV.String()) {
		return autoscaling.TiKV
	}

	if strings.Contains(query, autoscaling.TiDB.String()) {
		return autoscaling.TiDB
	}

	return 2
}

func buildCPUMockData(component autoscaling.ComponentType) response {
	pods := podNames[component]

	var results []result
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

	resp := response{
		Status: "success",
		Data: data{
			ResultType: "vector",
			Result:     results,
		},
	}

	return resp
}

var podNameTemplate = map[autoscaling.ComponentType]string{
	autoscaling.TiDB: mockTiDBInstanceNamePattern,
	autoscaling.TiKV: mockTiKVInstanceNamePattern,
}

func generatePodNames(component autoscaling.ComponentType) []string {
	names := make([]string, 0, instanceCount)
	pattern := podNameTemplate[component]
	for i := 0; i < instanceCount; i++ {
		names = append(names, fmt.Sprintf(pattern, mockClusterName, i))
	}
	return names
}

var podNames = map[autoscaling.ComponentType][]string{
	autoscaling.TiDB: generatePodNames(autoscaling.TiDB),
	autoscaling.TiKV: generatePodNames(autoscaling.TiKV),
}

func makeJSONResponse(promResp *response) (*http.Response, []byte, error) {
	body, err := json.Marshal(promResp)
	if err != nil {
		return nil, []byte{}, err
	}

	resp := &http.Response{
		Status:        "200 OK",
		StatusCode:    200,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Body:          io.NopCloser(bytes.NewBufferString(string(body))),
		ContentLength: int64(len(body)),
		Header:        make(http.Header),
	}
	resp.Header.Add("Content-Type", "application/json")

	return resp, body, nil
}
