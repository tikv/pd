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
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tikv/pd/pkg/utils/etcdutil"
)

// Strategy within a HTTP request provides rules and resources to help make decision for auto scaling.
type Strategy struct {
	Rules     []*Rule     `json:"rules"`
	Resources []*Resource `json:"resources"`
}

// Rule is a set of constraints for a kind of component.
type Rule struct {
	Component   string       `json:"component"`
	CPURule     *CPURule     `json:"cpu_rule,omitempty"`
	StorageRule *StorageRule `json:"storage_rule,omitempty"`
}

// CPURule is the constraints about CPU.
type CPURule struct {
	MaxThreshold  float64  `json:"max_threshold"`
	MinThreshold  float64  `json:"min_threshold"`
	ResourceTypes []string `json:"resource_types"`
}

// StorageRule is the constraints about storage.
type StorageRule struct {
	MinThreshold  float64  `json:"min_threshold"`
	ResourceTypes []string `json:"resource_types"`
}

// Resource represents a kind of resource set including CPU, memory, storage.
type Resource struct {
	ResourceType string `json:"resource_type"`
	// The basic unit of CPU is milli-core.
	CPU uint64 `json:"cpu"`
	// The basic unit of memory is byte.
	Memory uint64 `json:"memory"`
	// The basic unit of storage is byte.
	Storage uint64  `json:"storage"`
	Count   *uint64 `json:"count,omitempty"`
}

// Plan is the final result of auto scaling, which indicates how to scale in or scale out.
type Plan struct {
	Component    string            `json:"component"`
	Count        uint64            `json:"count"`
	ResourceType string            `json:"resource_type"`
	Labels       map[string]string `json:"labels"`
}

// ComponentType distinguishes different kinds of components.
type ComponentType int

const (
	// TiKV indicates the TiKV component
	TiKV ComponentType = iota
	// TiDB indicates the TiDB component
	TiDB
)

func (c ComponentType) String() string {
	switch c {
	case TiKV:
		return "tikv"
	case TiDB:
		return "tidb"
	default:
		return "unknown"
	}
}

// MetricType distinguishes different kinds of metrics
type MetricType int

const (
	// CPUUsage is used cpu time in the duration
	CPUUsage MetricType = iota
	// CPUQuota is cpu cores quota for each instance
	CPUQuota
)

func (c MetricType) String() string {
	switch c {
	case CPUUsage:
		return "cpu_usage"
	case CPUQuota:
		return "cpu_quota"
	default:
		return "unknown"
	}
}

type instance struct {
	id      uint64
	address string
}

// TiDBInfo record the detail tidb info
type TiDBInfo struct {
	Version        *string           `json:"version,omitempty"`
	StartTimestamp *int64            `json:"start_timestamp,omitempty"`
	Labels         map[string]string `json:"labels"`
	GitHash        *string           `json:"git_hash,omitempty"`
	Address        string
}

// GetLabelValue returns a label's value (if exists).
func (t *TiDBInfo) getLabelValue(key string) string {
	for k, v := range t.getLabels() {
		if strings.EqualFold(k, key) {
			return v
		}
	}
	return ""
}

// GetLabels returns the labels of the tidb.
func (t *TiDBInfo) getLabels() map[string]string {
	return t.Labels
}

// GetTiDB get TiDB info which registered in PD by address
func GetTiDB(etcdClient *clientv3.Client, address string) (*TiDBInfo, error) {
	key := fmt.Sprintf("/topology/tidb/%s/info", address)
	resp, err := etcdutil.EtcdKVGet(etcdClient, key)
	if err != nil {
		return nil, err
	}
	if resp.Count < 1 {
		err := fmt.Errorf("resp loaded for tidb [%s] is empty", address)
		return nil, err
	}
	tidb := &TiDBInfo{}
	err = json.Unmarshal(resp.Kvs[0].Value, tidb)
	if err != nil {
		return nil, err
	}
	tidb.Address = address
	return tidb, nil
}

const (
	tidbTTLPatternStr = "/topology/tidb/.+/ttl"
	tidbInfoPrefix    = "/topology/tidb/"
)

// GetTiDBs list TiDB register in PD
func GetTiDBs(etcdClient *clientv3.Client) ([]*TiDBInfo, error) {
	tidbTTLPattern, err := regexp.Compile(tidbTTLPatternStr)
	if err != nil {
		return nil, err
	}
	resps, err := etcdutil.EtcdKVGet(etcdClient, tidbInfoPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	tidbs := make([]*TiDBInfo, 0, resps.Count)
	for _, resp := range resps.Kvs {
		key := string(resp.Key)
		if tidbTTLPattern.MatchString(key) {
			address := key[len(tidbInfoPrefix) : len(key)-len("/ttl")]
			// In order to avoid make "aaa/bbb" in "/topology/tidb/aaa/bbb/ttl" stored as tidb address
			if !strings.Contains(address, "/") {
				tidbs = append(tidbs, &TiDBInfo{
					Address: address,
				})
			}
		}
	}
	return tidbs, nil
}
