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
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	promClient "github.com/prometheus/client_golang/api"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	groupLabelKey                  = "group"
	autoScalingGroupLabelKeyPrefix = "pd-auto-scaling"
	resourceTypeLabelKey           = "resource-type"
	milliCores                     = 1000
)

// TODO: adjust the value or make it configurable.
var (
	// MetricsTimeDuration is used to get the metrics of a certain time period.
	// This must be long enough to cover at least 2 scrape intervals
	// Or you will get nothing when querying CPU usage
	MetricsTimeDuration = time.Minute
	// MaxScaleOutStep is used to indicate the maximum number of instance for scaling out operations at once.
	MaxScaleOutStep uint64 = 1
	// MaxScaleInStep is used to indicate the maximum number of instance for scaling in operations at once.
	MaxScaleInStep uint64 = 1
)

func calculate(rc *cluster.RaftCluster, cfg *config.PDServerConfig, strategy *Strategy) []*Plan {
	var plans []*Plan

	client, err := promClient.NewClient(promClient.Config{
		Address: cfg.MetricStorage,
	})
	if err != nil {
		log.Error("error initializing Prometheus client", zap.String("metric-storage", cfg.MetricStorage), errs.ZapError(errs.ErrPrometheusCreateClient, err))
		return nil
	}
	querier := NewPrometheusQuerier(client)

	components := map[ComponentType]struct{}{}
	for _, rule := range strategy.Rules {
		switch rule.Component {
		case "tidb":
			components[TiDB] = struct{}{}
		case "tikv":
			components[TiKV] = struct{}{}
		}
	}

	for comp := range components {
		if compPlans := getPlans(rc, querier, strategy, comp); compPlans != nil {
			plans = append(plans, compPlans...)
		}
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
		log.Error("cannot get total CPU used time", errs.ZapError(err))
		return nil
	}

	currentQuota, err := getTotalCPUQuota(querier, component, instances, now)
	if err != nil || currentQuota == 0 {
		log.Error("cannot get total CPU quota", errs.ZapError(err))
		return nil
	}

	totalCPUTime := float64(currentQuota) / milliCores * MetricsTimeDuration.Seconds()
	usage := totalCPUUseTime / totalCPUTime
	maxThreshold, minThreshold := getCPUThresholdByComponent(strategy, component)

	groups, err := getScaledGroupsByComponent(rc, component, instances)
	if err != nil {
		// TODO: error handling
		return nil
	}

	// TODO: add metrics to show why it triggers scale in/out.
	if usage > maxThreshold {
		scaleOutQuota := (totalCPUUseTime - totalCPUTime*maxThreshold) / MetricsTimeDuration.Seconds()
		return calculateScaleOutPlan(strategy, component, scaleOutQuota, groups)
	}

	if usage < minThreshold {
		scaleInQuota := (totalCPUTime*minThreshold - totalCPUUseTime) / MetricsTimeDuration.Seconds()
		return calculateScaleInPlan(strategy, scaleInQuota, groups)
	}

	return groups
}

func filterTiKVInstances(informer core.StoreSetInformer) []instance {
	var instances []instance
	stores := informer.GetStores()
	for _, store := range stores {
		if store.IsUp() {
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

// TODO: support other metrics storage
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

// TODO: support other metrics storage
// get total CPU quota (in milliCores) through Prometheus.
func getTotalCPUQuota(querier Querier, component ComponentType, instances []instance, timestamp time.Time) (uint64, error) {
	result, err := querier.Query(NewQueryOptions(component, CPUQuota, getAddresses(instances), timestamp, 0))
	if err != nil {
		return 0, err
	}

	sum := 0.0
	for _, value := range result {
		sum += value
	}

	quota := uint64(math.Floor(sum * float64(milliCores)))

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

func calculateScaleOutPlan(strategy *Strategy, component ComponentType, scaleOutQuota float64, groups []*Plan) []*Plan {
	group := findBestGroupToScaleOut(strategy, groups, component)

	resCPU := float64(getCPUByResourceType(strategy, group.ResourceType))
	if math.Abs(resCPU) <= 1e-6 {
		log.Error("resource CPU is zero, exiting calculation")
		return nil
	}
	resCount := getCountByResourceType(strategy, group.ResourceType)
	scaleOutCount := typeutil.MinUint64(uint64(math.Ceil(scaleOutQuota/resCPU)), MaxScaleOutStep)

	// A new group created
	if len(groups) == 0 {
		if resCount == nil || group.Count+scaleOutCount <= *resCount {
			group.Count += scaleOutCount
			return []*Plan{&group}
		}
		return nil
	}

	// update the existed group
	for i, g := range groups {
		if g.ResourceType == group.ResourceType {
			if resCount == nil || group.Count+scaleOutCount <= *resCount {
				group.Count += scaleOutCount
				groups[i] = &group
			} else {
				group.Count = *resCount
				groups[i] = &group
			}
		}
	}
	return groups
}

func calculateScaleInPlan(strategy *Strategy, scaleInQuota float64, groups []*Plan) []*Plan {
	if len(groups) == 0 {
		return nil
	}
	group := findBestGroupToScaleIn(strategy, scaleInQuota, groups)
	resCPU := float64(getCPUByResourceType(strategy, group.ResourceType))
	if math.Abs(resCPU) <= 1e-6 {
		log.Error("resource CPU is zero, exiting calculation")
		return nil
	}
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

func getCountByResourceType(strategy *Strategy, resourceType string) *uint64 {
	var zero uint64 = 0
	for _, res := range strategy.Resources {
		if res.ResourceType == resourceType {
			return res.Count
		}
	}
	return &zero
}

func getScaledGroupsByComponent(rc *cluster.RaftCluster, component ComponentType, healthyInstances []instance) ([]*Plan, error) {
	switch component {
	case TiKV:
		return getScaledTiKVGroups(rc, healthyInstances)
	case TiDB:
		return getScaledTiDBGroups(rc.GetEtcdClient(), healthyInstances)
	default:
		return nil, errors.Errorf("unknown component type %s", component.String())
	}
}

func getScaledTiKVGroups(informer core.StoreSetInformer, healthyInstances []instance) ([]*Plan, error) {
	planMap := make(map[string]map[string]struct{}, len(healthyInstances))
	resourceTypeMap := make(map[string]string)
	for _, instance := range healthyInstances {
		store := informer.GetStore(instance.id)
		if store == nil {
			log.Warn("inconsistency between health instances and store status, exit auto-scaling calculation",
				zap.Uint64("store-id", instance.id))
			return nil, errors.New("inconsistent healthy instances")
		}

		groupName := store.GetLabelValue(groupLabelKey)
		if !isAutoScaledGroup(groupName) {
			continue
		}

		buildPlanMap(planMap, groupName, instance.address)

		if _, ok := resourceTypeMap[groupName]; !ok {
			resourceType := store.GetLabelValue(resourceTypeLabelKey)
			if resourceType == "" {
				log.Warn("store is in auto-scaled group but has no resource type label, exit auto-scaling calculation", zap.Uint64("store-id", instance.id))
				return nil, errors.New("missing resource type label")
			}
			resourceTypeMap[groupName] = resourceType
		}
	}
	return buildPlans(planMap, resourceTypeMap, TiKV), nil
}

func getScaledTiDBGroups(etcdClient *clientv3.Client, healthyInstances []instance) ([]*Plan, error) {
	planMap := make(map[string]map[string]struct{}, len(healthyInstances))
	resourceTypeMap := make(map[string]string)
	for _, instance := range healthyInstances {
		tidb, err := GetTiDB(etcdClient, instance.address)
		if err != nil {
			// TODO: error handling
			return nil, err
		}
		if tidb == nil {
			log.Warn("inconsistency between health instances and tidb status, exit auto-scaling calculation",
				zap.String("tidb-address", instance.address))
			return nil, errors.New("inconsistent healthy instances")
		}

		groupName := tidb.getLabelValue(groupLabelKey)
		if !isAutoScaledGroup(groupName) {
			continue
		}

		buildPlanMap(planMap, groupName, instance.address)
		resourceType := tidb.getLabelValue(resourceTypeLabelKey)
		if _, ok := resourceTypeMap[groupName]; !ok {
			if resourceType == "" {
				log.Warn("tidb is in auto-scaled group but has no resource type label, exit auto-scaling calculation", zap.String("tidb-address", instance.address))
				return nil, errors.New("missing resource type label")
			}
			resourceTypeMap[groupName] = resourceType
		}
	}
	return buildPlans(planMap, resourceTypeMap, TiDB), nil
}

func isAutoScaledGroup(groupName string) bool {
	return len(groupName) > len(autoScalingGroupLabelKeyPrefix) && strings.HasPrefix(groupName, autoScalingGroupLabelKeyPrefix)
}

func buildPlanMap(planMap map[string]map[string]struct{}, groupName, address string) {
	if component, ok := planMap[groupName]; ok {
		component[address] = struct{}{}
	} else {
		planMap[groupName] = map[string]struct{}{
			address: {},
		}
	}
}

func buildPlans(planMap map[string]map[string]struct{}, resourceTypeMap map[string]string, componentType ComponentType) []*Plan {
	plans := make([]*Plan, 0, len(planMap))
	for groupName, groupInstances := range planMap {
		resourceType := resourceTypeMap[groupName]
		plans = append(plans, &Plan{
			Component:    componentType.String(),
			Count:        uint64(len(groupInstances)),
			ResourceType: resourceType,
			Labels: map[string]string{
				groupLabelKey:        groupName,
				resourceTypeLabelKey: resourceType,
			},
		})
	}
	return plans
}

// TODO: implement heterogeneous logic and take cluster information into consideration.
func findBestGroupToScaleIn(_ *Strategy, _ float64, groups []*Plan) Plan {
	return *groups[0]
}

// TODO: implement heterogeneous logic and take cluster information into consideration.
func findBestGroupToScaleOut(strategy *Strategy, groups []*Plan, component ComponentType) Plan {
	if len(groups) != 0 {
		return *groups[0]
	}

	resources := getResourcesByComponent(strategy, component)
	group := Plan{
		Component:    component.String(),
		Count:        0,
		ResourceType: resources[0].ResourceType,
		Labels: map[string]string{
			// TODO: we need to make this label not duplicated when we implement the heterogeneous logic.
			groupLabelKey:        fmt.Sprintf("%s-%s", autoScalingGroupLabelKeyPrefix, component.String()),
			resourceTypeLabelKey: resources[0].ResourceType,
		},
	}

	// TODO: we can provide different senerios by using options and remove this kind of special judgement.
	if component == TiKV {
		group.Labels[filter.SpecialUseKey] = filter.SpecialUseHotRegion
	}

	return group
}
