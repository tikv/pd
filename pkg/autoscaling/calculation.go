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
	"math"
	"strings"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	promClient "github.com/prometheus/client_golang/api"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	groupLabelKey                  = "group"
	autoScalingGroupLabelKeyPrefix = "pd-auto-scaling-"
	millicores                     = 1000
)

// TODO: adjust the value or make it configurable.
var (
	// MetricsTimeDuration is used to get the metrics of a certain time period.
	// This must be long enough to cover at least 2 scrape intervals
	// Or you will get nothing when querying CPU usage
	MetricsTimeDuration = 60 * time.Second
	// MaxScaleOutStep is used to indicate the maxium number of instance for scaling out operations at once.
	MaxScaleOutStep uint64 = 1
	// MaxScaleInStep is used to indicate the maxium number of instance for scaling in operations at once.
	MaxScaleInStep uint64 = 1
)

func calculate(rc *cluster.RaftCluster, cfg *config.PDServerConfig, strategy *Strategy) []*Plan {
	var plans []*Plan

	client, err := promClient.NewClient(promClient.Config{
		Address: cfg.MetricStorage,
	})
	if err != nil {
		log.Error("error initializing Prometheus client", zap.String("metric-storage", cfg.MetricStorage), zap.Error(err))
		return nil
	}
	querier := NewPrometheusQuerier(client)

	if tikvPlans := getPlans(rc, querier, strategy, TiKV); tikvPlans != nil {
		plans = append(plans, tikvPlans...)
	}
	if tidbPlans := getPlans(rc, querier, strategy, TiDB); tidbPlans != nil {
		plans = append(plans, tidbPlans...)
	}
	return plans
}

func getPlans(rc *cluster.RaftCluster, querier Querier, strategy *Strategy, component ComponentType) []*Plan {
	var instances []instance
	if component == TiKV {
		instances = filterTiKVInstances(rc)
	} else {
		instances = getTiDBInstances(rc.GetEtcdClient())
	}

	if len(instances) == 0 {
		return nil
	}

	now := time.Now()
	totalCPUUseTime, err := getTotalCPUUseTime(querier, component, instances, now, MetricsTimeDuration)
	if err != nil {
		log.Error("cannot get total CPU used time", zap.Error(err))
		return nil
	}

	currentQuota, err := getTotalCPUQuota(querier, component, instances, now)
	if err != nil {
		log.Error("cannot get total CPU quota", zap.Error(err))
		return nil
	}

	totalCPUTime := float64(currentQuota) / millicores * MetricsTimeDuration.Seconds()
	usage := totalCPUUseTime / totalCPUTime
	maxThreshold, minThreshold := getCPUThresholdByComponent(strategy, component)

	// TODO: add metrics to show why it triggers scale in/out.
	if usage > maxThreshold {
		scaleOutQuota := (totalCPUUseTime - totalCPUTime*maxThreshold) / MetricsTimeDuration.Seconds()
		return calculateScaleOutPlan(rc, strategy, component, scaleOutQuota, currentQuota, instances)
	}
	if usage < minThreshold {
		scaleInQuota := (totalCPUTime*minThreshold - totalCPUUseTime) / MetricsTimeDuration.Seconds()
		return calculateScaleInPlan(rc, strategy, component, scaleInQuota, instances)
	}
	return nil
}

func filterTiKVInstances(informer core.StoreSetInformer) []instance {
	var instances []instance
	stores := informer.GetStores()
	for _, store := range stores {
		if store.GetState() == metapb.StoreState_Up {
			instances = append(instances, instance{id: store.GetID(), address: store.GetAddress()})
		}
	}
	return instances
}

func getTiDBInstances(etcdClient *clientv3.Client) []instance {
	infos, err := GetTiDBs(etcdClient)
	if err != nil {
		// TODO: error handling
		return []instance{}
	}
	instances := make([]instance, 0, len(infos))
	for _, info := range infos {
		instances = append(instances, instance{address: info.Address})
	}
	return instances
}

func getAddresses(instances []instance) []string {
	names := make([]string, 0, len(instances))
	for _, inst := range instances {
		names = append(names, inst.address)
	}
	return names
}

// TODO: suppport other metrics storage
// get total CPU use time (in seconds) through Prometheus.
func getTotalCPUUseTime(querier Querier, component ComponentType, instances []instance, timestamp time.Time, duration time.Duration) (float64, error) {
	result, err := querier.Query(NewQueryOptions(component, CPUUsage, getAddresses(instances), timestamp, duration))
	if err != nil {
		return 0.0, err
	}

	sum := 0.0
	for _, value := range result {
		sum += value
	}

	return sum, nil
}

// TODO: suppport other metrics storage
// get total CPU quota (in millicores) through Prometheus.
func getTotalCPUQuota(querier Querier, component ComponentType, instances []instance, timestamp time.Time) (uint64, error) {
	result, err := querier.Query(NewQueryOptions(component, CPUQuota, getAddresses(instances), timestamp, 0))
	if err != nil {
		return 0, err
	}

	sum := 0.0
	for _, value := range result {
		sum += value
	}

	quota := uint64(math.Floor(sum * float64(millicores)))

	return quota, nil
}

func getCPUThresholdByComponent(strategy *Strategy, component ComponentType) (maxThreshold float64, minThreshold float64) {
	for _, rule := range strategy.Rules {
		if rule.Component == component.String() {
			return rule.CPURule.MaxThreshold, rule.CPURule.MinThreshold
		}
	}
	return 0, 0
}

func getResourcesByComponent(strategy *Strategy, component ComponentType) []*Resource {
	var resTyp []string
	var resources []*Resource
	for _, rule := range strategy.Rules {
		if rule.Component == component.String() {
			resTyp = rule.CPURule.ResourceTypes
		}
	}
	for _, res := range strategy.Resources {
		for _, typ := range resTyp {
			if res.ResourceType == typ {
				resources = append(resources, res)
			}
		}
	}
	return resources
}

func calculateScaleOutPlan(rc *cluster.RaftCluster, strategy *Strategy, component ComponentType, scaleOutQuota float64, currentQuota uint64, instances []instance) []*Plan {
	groups := getScaledGroupsByComponent(rc, component, instances)
	group := findBestGroupToScaleOut(rc, strategy, scaleOutQuota, groups, component)

	resCPU := float64(getCPUByResourceType(strategy, group.ResourceType))
	resCount := getCountByResourceType(strategy, group.ResourceType)
	scaleOutCount := typeutil.MinUint64(uint64(math.Ceil(scaleOutQuota/resCPU)), MaxScaleOutStep)

	// A new group created
	if len(groups) == 0 {
		if group.Count+scaleOutCount <= resCount {
			group.Count += scaleOutCount
			return []*Plan{&group}
		}
		return nil
	}

	// update the existed group
	for i, g := range groups {
		if g.ResourceType == group.ResourceType && group.Count+scaleOutCount <= resCount {
			group.Count += scaleOutCount
			groups[i] = &group
		}
	}
	return groups
}

func calculateScaleInPlan(rc *cluster.RaftCluster, strategy *Strategy, component ComponentType, scaleInQuota float64, instances []instance) []*Plan {
	groups := getScaledGroupsByComponent(rc, component, instances)
	if len(groups) == 0 {
		return nil
	}
	group := findBestGroupToScaleIn(rc, strategy, scaleInQuota, groups)
	resCPU := float64(getCPUByResourceType(strategy, group.ResourceType))
	scaleInCount := typeutil.MinUint64(uint64(math.Ceil(scaleInQuota/resCPU)), MaxScaleInStep)
	for i, g := range groups {
		if g.ResourceType == group.ResourceType {
			if group.Count > scaleInCount {
				group.Count -= scaleInCount
				groups[i] = &group
			} else {
				groups = append(groups[:i], groups[i+1:]...)
			}
		}
	}
	return groups
}

func getCPUByResourceType(strategy *Strategy, resourceType string) uint64 {
	for _, res := range strategy.Resources {
		if res.ResourceType == resourceType {
			return res.CPU
		}
	}
	return 0
}

func getCountByResourceType(strategy *Strategy, resourceType string) uint64 {
	for _, res := range strategy.Resources {
		if res.ResourceType == resourceType {
			return res.Count
		}
	}
	return 0
}

func getScaledGroupsByComponent(rc *cluster.RaftCluster, component ComponentType, healthyInstances []instance) []*Plan {
	switch component {
	case TiKV:
		return getScaledTiKVGroups(rc, healthyInstances)
	case TiDB:
		return getScaledTiDBGroups(rc.GetEtcdClient(), healthyInstances)
	default:
		return nil
	}
}

func getScaledTiKVGroups(informer core.StoreSetInformer, healthyInstances []instance) []*Plan {
	planMap := make(map[string]map[string]struct{}, len(healthyInstances))
	for _, instance := range healthyInstances {
		store := informer.GetStore(instance.id)
		if store == nil {
			log.Warn("inconsistency between health instances and store status, exit auto-scaling calculation",
				zap.Uint64("store-id", instance.id))
			return nil
		}
		v := store.GetLabelValue(groupLabelKey)
		buildPlanMap(planMap, v, instance.address)
	}
	return buildPlans(planMap, TiKV)
}

func getScaledTiDBGroups(etcdClient *clientv3.Client, healthyInstances []instance) []*Plan {
	planMap := make(map[string]map[string]struct{}, len(healthyInstances))
	for _, instance := range healthyInstances {
		tidb, err := GetTiDB(etcdClient, instance.address)
		if err != nil {
			// TODO: error handling
			return nil
		}
		if tidb == nil {
			log.Warn("inconsistency between health instances and tidb status, exit auto-scaling calculation",
				zap.String("tidb-address", instance.address))
			return nil
		}
		v := tidb.getLabelValue(groupLabelKey)
		buildPlanMap(planMap, v, instance.address)
	}
	return buildPlans(planMap, TiDB)
}

func buildPlanMap(planMap map[string]map[string]struct{}, groupName, address string) {
	if len(groupName) > len(autoScalingGroupLabelKeyPrefix) &&
		strings.HasPrefix(groupName, autoScalingGroupLabelKeyPrefix) {
		if component, ok := planMap[groupName]; ok {
			component[address] = struct{}{}
		} else {
			planMap[groupName] = map[string]struct{}{
				address: {},
			}
		}
	}
}

func buildPlans(planMap map[string]map[string]struct{}, componentType ComponentType) []*Plan {
	var plans []*Plan
	for groupLabel, groupInstances := range planMap {
		plans = append(plans, &Plan{
			Component: componentType.String(),
			Count:     uint64(len(groupInstances)),
			Labels: []*metapb.StoreLabel{
				{
					Key:   groupLabelKey,
					Value: groupLabel,
				},
			},
		})
	}
	return plans
}

// TODO: implement heterogeneous logic
func findBestGroupToScaleIn(rc *cluster.RaftCluster, strategy *Strategy, scaleInQuota float64, groups []*Plan) Plan {
	return *groups[0]
}

// TODO: implement heterogeneous logic
func findBestGroupToScaleOut(rc *cluster.RaftCluster, strategy *Strategy, scaleOutQuota float64, groups []*Plan, component ComponentType) Plan {
	if len(groups) != 0 {
		return *groups[0]
	}

	resources := getResourcesByComponent(strategy, component)
	group := Plan{
		Component:    component.String(),
		Count:        0,
		ResourceType: resources[0].ResourceType,
	}
	return group
}
