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
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	promClient "github.com/prometheus/client_golang/api"
	"github.com/tikv/pd/server/cluster"
	"go.uber.org/zap"
)

const (
	DefaultTimeout                 = 5 * time.Second
	prometheusAddressKey           = "/topology/prometheus"
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
	MetricsTimeDuration = 60 * time.Second
	// MaxScaleOutStep is used to indicate the maximum number of instance for scaling out operations at once.
	MaxScaleOutStep uint64 = 1
	// MaxScaleInStep is used to indicate the maximum number of instance for scaling in operations at once.
	MaxScaleInStep uint64 = 1
)

func calculate(rc *cluster.RaftCluster, strategy *Strategy) ([]*Plan, error) {
	var (
		plans       []*Plan
		groups      []*Plan
		instances   []instance
		resourceMap map[string]uint64
	)
	// init prometheus client
	address, err := getPrometheusAddress(rc)
	if err != nil {
		return nil, err
	}

	log.Debug(fmt.Sprintf("prometheus address: %s", address))

	client, err := promClient.NewClient(promClient.Config{Address: address})
	if err != nil {
		return nil, err
	}
	querier := NewPrometheusQuerier(client)

	for _, rule := range strategy.Rules {
		switch rule.Component {
		case TiKV.String():
			// get heterogeneous groups
			instances, err = getInstancesByComponent(rc, TiKV)
			if err != nil {
				return nil, err
			}
			resourceMap, err = getResourceMapByComponent(rc, instances, TiKV)
			if err != nil {
				return nil, err
			}
			groups = getHeterogeneousGroupsByComponent(resourceMap, TiKV)
			log.Debug(fmt.Sprintf("heterogeneous tikv groups: %v", groups))
			// get tikv plans
			tikvPlans, err := getTiKVPlans(rc, querier, instances, strategy, resourceMap)
			if err != nil {
				return nil, err
			}
			if tikvPlans != nil {
				log.Info(fmt.Sprintf("autoscale tikv plans: %v", tikvPlans))
			}
			// merge plans
			plans = mergePlans(tikvPlans, groups)
		case TiDB.String():
			// get heterogeneous groups
			instances, err = getInstancesByComponent(rc, TiDB)
			if err != nil {
				return nil, err
			}
			resourceMap, err = getResourceMapByComponent(rc, instances, TiDB)
			if err != nil {
				return nil, err
			}
			groups = getHeterogeneousGroupsByComponent(resourceMap, TiDB)
			log.Debug(fmt.Sprintf("heterogeneous tidb groups: %v", groups))
			// get tidb plans
			tidbPlans, err := getTiDBPlans(querier, instances, strategy, resourceMap)
			if err != nil {
				return nil, err
			}
			if tidbPlans != nil {
				log.Info(fmt.Sprintf("autoscale tidb plans: %v", tidbPlans))
			}
			// merge plans
			plans = mergePlans(tidbPlans, groups)
		}
	}

	return plans, nil
}

func getTiKVPlans(rc *cluster.RaftCluster, querier Querier, instances []instance, strategy *Strategy, resourceMap map[string]uint64) ([]*Plan, error) {
	plans, err := getTiKVStoragePlans(rc, instances, strategy, resourceMap)
	if err != nil {
		return nil, err
	}
	if plans != nil {
		return plans, nil
	}

	plans, err = getCPUPlans(querier, instances, strategy, resourceMap, TiKV)
	if err != nil {
		return nil, err
	}

	return plans, nil
}

func getTiKVStoragePlans(rc *cluster.RaftCluster, instances []instance, strategy *Strategy, resourceMap map[string]uint64) ([]*Plan, error) {
	if strategy.Rules[0].StorageRule == nil || len(instances) == 0 {
		return nil, nil
	}

	// get total storage used size and total storage capacity
	totalStorageUsedSize, totalStorageCapacity, err := getTotalStorageInfo(rc, instances)
	if err != nil {
		return nil, err
	}

	// calculate storage usage
	storageUsage := totalStorageUsedSize / totalStorageCapacity
	storageMaxThreshold, storageMinThreshold := getStorageThresholdByComponent(strategy, TiKV)
	storageUsageTarget := (storageMaxThreshold + storageMinThreshold) / 2

	log.Debug(fmt.Sprintf("autoscale: get storage usage information compeleted. totalStorageUsedSize: %f, totalStorageCapacity: %f, storageUsage: %f, storageMaxThreshold: %f , storageMinThreshold: %f",
		totalStorageUsedSize, totalStorageCapacity, storageUsage, storageMaxThreshold, storageMinThreshold))

	if storageUsage > storageMaxThreshold {
		// generate homogeneous tikv plan
		resources := getStorageResourcesByComponent(strategy, TiKV)
		homogeneousTiKVCount := getCountByResourceType(resources, homogeneousTiKVResourceType)

		if resourceMap[homogeneousTiKVResourceType] == strategy.NodeCount || (homogeneousTiKVCount != nil && resourceMap[homogeneousTiKVResourceType] > *homogeneousTiKVCount) {
			// homogeneous instance number reaches k8s node number or the resource limit,
			// can not scale out homogeneous instance any more
			log.Warn(fmt.Sprintf("autoscale: can not scale out homogeneous instance, homogeneous instance number: %d, k8s node number: %d, resource limit: %v",
				resourceMap[homogeneousTiKVResourceType], strategy.NodeCount, homogeneousTiKVCount))
			return nil, nil
		}

		storageScaleSize := totalStorageUsedSize/storageUsageTarget - totalStorageCapacity
		storeStorageSize := getStorageByResourceType(resources, homogeneousTiKVResourceType)
		scaleOutCount := uint64(storageScaleSize)/storeStorageSize + 1

		return getHomogeneousScaleOutPlans(scaleOutCount, uint64(len(instances)), strategy.NodeCount, resources, resourceMap, TiKV), nil
	}

	return nil, nil
}

func getPrometheusAddress(rc *cluster.RaftCluster) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	resp, err := rc.GetEtcdClient().Get(ctx, prometheusAddressKey)
	if err != nil {
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", errors.New(fmt.Sprintf("length of the response values of the key %s is 0", prometheusAddressKey))
	}

	address := &Address{}
	err = json.Unmarshal(resp.Kvs[0].Value, address)
	if err != nil {
		return "", err
	}

	return address.String(), nil
}

func getTiDBPlans(querier Querier, instances []instance, strategy *Strategy, resourceMap map[string]uint64) ([]*Plan, error) {
	return getCPUPlans(querier, instances, strategy, resourceMap, TiDB)
}

func getCPUPlans(querier Querier, instances []instance, strategy *Strategy, resourceMap map[string]uint64, component ComponentType) ([]*Plan, error) {
	if strategy.Rules[0].CPURule == nil {
		return nil, nil
	}

	now := time.Now()
	// get cpu used times
	cpuUsedTimes, err := querier.Query(NewQueryOptions(component, CPUUsage, now, MetricsTimeDuration))
	if err != nil {
		return nil, err
	}
	// get cpu quotas
	cpuQuotas, err := querier.Query(NewQueryOptions(component, CPUQuota, now, MetricsTimeDuration))
	if err != nil {
		return nil, err
	}

	log.Debug(fmt.Sprintf("autoscale: get cpu usage information completed. component: %s, cpuUsedTimes: %v, cpuQuotas: %v",
		component.String(), cpuUsedTimes, cpuQuotas))

	var (
		totalCPUUsedTime float64
		totalCPUQuota    float64
		cpuUsageLowNum   uint64
	)

	// get cpu threshold
	cpuMaxThreshold, cpuMinThreshold := getCPUThresholdByComponent(strategy, component)
	cpuUsageHighMap := make(map[float64]float64)
	for instanceName, cpuUsedTime := range cpuUsedTimes {
		cpuQuota, ok := cpuQuotas[instanceName]
		if !ok {
			// if corresponding cpu quota does not exist, consider this instance is in the low usage status,
			// this could be useful to avoid scaling out incorrectly after scaling in heterogeneous groups
			cpuUsageLowNum++
			continue
		}
		cpuUsedTime /= MetricsTimeDuration.Seconds()
		totalCPUUsedTime += cpuUsedTime
		totalCPUQuota += cpuQuota
		cpuUsage := cpuUsedTime / cpuQuota

		if cpuUsage > cpuMaxThreshold {
			cpuUsageHighMap[cpuUsage] = cpuQuota
			continue
		}
		if cpuUsage < cpuMinThreshold {
			cpuUsageLowNum++
		}
	}

	totalInstanceCount := uint64(len(instances))
	totalCPUUsage := totalCPUUsedTime / totalCPUQuota
	cpuUsageTarget := (cpuMaxThreshold + cpuMinThreshold) / 2
	resources := getCPUResourcesByComponent(strategy, component)

	log.Debug(fmt.Sprintf("autoscale: calculate total cpu usage infomation completed. component: %s, totalInstanceCount: %d, totalCPUUsage: %f, cpuUsageHighMap: %v, cpuUsageLowNum: %d",
		component.String(), totalInstanceCount, totalCPUUsage, cpuUsageHighMap, cpuUsageLowNum))

	if totalCPUUsage > cpuMaxThreshold {
		// get homogeneous plans
		homogeneousResourceType := getHomogeneousResourceType(component)
		homogeneousCount := getCountByResourceType(resources, homogeneousResourceType)

		if resourceMap[homogeneousResourceType] >= strategy.NodeCount || (homogeneousCount != nil && resourceMap[homogeneousResourceType] >= *homogeneousCount) {
			// homogeneous instance number reaches k8s node number or the resource limit,
			// can not scale out homogeneous instance any more
			log.Warn(fmt.Sprintf("autoscale: can not scale out homogeneous instance, component: %s, homogeneous instance number: %d, k8s node number: %d, resource limit: %v",
				component.String(), resourceMap[homogeneousResourceType], strategy.NodeCount, homogeneousCount))
			return nil, nil
		}

		homogeneousCPUSize := getCPUByResourceType(resources, homogeneousResourceType)
		cpuScaleOutSize := totalCPUUsedTime/cpuUsageTarget - totalCPUQuota
		scaleOutCount := uint64(cpuScaleOutSize/float64(homogeneousCPUSize)) + 1

		return getHomogeneousScaleOutPlans(scaleOutCount, totalInstanceCount, strategy.NodeCount, resources, resourceMap, component), nil
	}

	if len(cpuUsageHighMap) > 0 && totalInstanceCount < strategy.NodeCount {
		// generate heterogeneous scale out plans
		cpuScaleOutSize := 0.0
		for cpuUsage, cpuQuota := range cpuUsageHighMap {
			cpuScaleOutSize += (cpuUsage - cpuUsageTarget) * cpuQuota
		}

		availableCount := strategy.NodeCount - totalInstanceCount
		return getHeterogeneousScaleOutPlans(cpuScaleOutSize, cpuUsageTarget, availableCount, resourceMap, resources, component), nil
	}

	if cpuUsageLowNum == totalInstanceCount {
		// generate heterogeneous scale in plans
		return getHeterogeneousScaleInPlans(resourceMap, component), nil
	}

	return nil, nil
}

func getHeterogeneousScaleOutPlans(cpuScaleOutSize float64, cpuUsageTarget float64, availableCount uint64, resourceMap map[string]uint64, resources []*Resource, component ComponentType) []*Plan {
	var plans []*Plan
	// sort resources by cpu desc
	sortResourcesByCPUDesc(resources)

	for _, resource := range resources {
		if cpuScaleOutSize <= 0 || availableCount <= 0 {
			break
		}

		if resource.ResourceType != homogeneousTiKVResourceType && resource.ResourceType != homogeneousTiDBResourceType {
			scaleOutCount := uint64(cpuScaleOutSize/float64(resource.CPU)/cpuUsageTarget) + 1
			if scaleOutCount > availableCount {
				// not enough k8s nodes to scale out, reduce the scale out count
				scaleOutCount = availableCount
				availableCount = 0
			}

			existsCount, ok := resourceMap[resource.ResourceType]
			if ok {
				// this resource type exists
				count := scaleOutCount + existsCount
				if resource.Count == nil || count <= *resource.Count {
					// unlimited resource count or enough resource count left
					plans = append(plans, NewPlan(component, scaleOutCount, resource.ResourceType))
					log.Debug(fmt.Sprintf("autoscale: get heterogeneous scale out plans completed. component: %s, plans: %+v", component, plans))

					return plans
				}

				// not enough count left, use as much as possible
				scaleOutCount = *resource.Count - existsCount
				availableCount -= scaleOutCount
				count = *resource.Count
				cpuScaleOutSize -= float64(resource.CPU * scaleOutCount)

				plans = append(plans, NewPlan(component, count, resource.ResourceType))
				continue
			}

			// this resource type does not exist
			if resource.Count == nil || scaleOutCount <= *resource.Count {
				// unlimited resource count or enough resource count left
				plans = append(plans, NewPlan(component, scaleOutCount, resource.ResourceType))
				log.Debug(fmt.Sprintf("autoscale: get heterogeneous scale out plans completed. component: %s, plans: %+v", component, plans))

				return plans
			}

			if *resource.Count > 0 {
				// not enough count left, use as much as possible
				availableCount -= *resource.Count
				cpuScaleOutSize -= float64(resource.CPU * *resource.Count)

				plans = append(plans, NewPlan(TiKV, *resource.Count, resource.ResourceType))
			}
		}
	}

	log.Debug(fmt.Sprintf("autoscale: get heterogeneous scale out plans completed. component: %s, plans: %+v", component, plans))
	return plans
}

func getHeterogeneousScaleInPlans(resourceMap map[string]uint64, component ComponentType) []*Plan {
	var plans []*Plan

	for resourceType, resourceCount := range resourceMap {
		if resourceType != homogeneousTiKVResourceType && resourceType != homogeneousTiDBResourceType {
			plans = append(plans, NewPlan(component, resourceCount-1, resourceType))
			log.Debug(fmt.Sprintf("autoscale: get heterogeneous scale in plans completed. component: %s, plans: %+v", component, plans))

			return plans
		}
	}

	log.Debug(fmt.Sprintf("autoscale: get heterogeneous scale in plans completed. component: %s, plans: %+v", component, plans))

	return plans
}

func getInstancesByComponent(rc *cluster.RaftCluster, component ComponentType) ([]instance, error) {
	var (
		err       error
		instances []instance
	)

	switch component {
	case TiKV:
		instances = getTiKVInstances(rc)
	case TiDB:
		instances, err = getTiDBInstances(rc)
	default:
		return nil, errors.Errorf("unknown component type %s", component.String())
	}

	return instances, err
}

func getTiKVInstances(rc *cluster.RaftCluster) []instance {
	var instances []instance

	stores := rc.GetStores()
	for _, store := range stores {
		if store.GetState() == metapb.StoreState_Up {
			instances = append(instances, instance{id: store.GetID(), address: store.GetAddress()})
		}
	}
	return instances
}

func getTiDBInstances(rc *cluster.RaftCluster) ([]instance, error) {
	infos, err := GetTiDBs(rc.GetEtcdClient())
	if err != nil {
		return nil, err
	}

	instances := make([]instance, 0, len(infos))
	for _, info := range infos {
		instances = append(instances, instance{address: info.Address})
	}

	return instances, nil
}

func getHeterogeneousGroupsByComponent(resourceMap map[string]uint64, component ComponentType) []*Plan {
	var groups []*Plan

	for resourceType, count := range resourceMap {
		if resourceType != homogeneousTiKVResourceType && resourceType != homogeneousTiDBResourceType {
			groups = append(groups, NewPlan(component, count, resourceType))
		}
	}

	return groups
}

func getTotalStorageInfo(rc *cluster.RaftCluster, healthyInstances []instance) (float64, float64, error) {
	var (
		totalStorageUsedSize uint64
		totalStorageCapacity uint64
	)

	for _, healthyInstance := range healthyInstances {
		store := rc.GetStore(healthyInstance.id)
		if store == nil {
			log.Warn("inconsistency between health instances and store status, exit auto-scaling calculation",
				zap.Uint64("store-id", healthyInstance.id))
			return 0, 0, errors.New(fmt.Sprintf("inconsistent healthy instance, instance id: %d", healthyInstance.id))
		}

		groupName := store.GetLabelValue(groupLabelKey)
		totalStorageUsedSize += store.GetUsedSize()
		if !isAutoScaledGroup(groupName) {
			totalStorageCapacity += store.GetCapacity()
		}
	}

	return float64(totalStorageUsedSize), float64(totalStorageCapacity), nil
}

func getCPUResourcesByComponent(strategy *Strategy, component ComponentType) []*Resource {
	var resources []*Resource

	for _, rule := range strategy.Rules {
		if rule.Component == component.String() {
			for _, resourceType := range rule.CPURule.ResourceTypes {
				resource := getResourceByResourceType(strategy, resourceType)
				if resource != nil {
					resources = append(resources, resource)
				}
			}

			return resources
		}
	}

	return resources
}

func getStorageResourcesByComponent(strategy *Strategy, component ComponentType) []*Resource {
	var resources []*Resource

	for _, rule := range strategy.Rules {
		if rule.Component == component.String() {
			for _, resourceType := range rule.StorageRule.ResourceTypes {
				resource := getResourceByResourceType(strategy, resourceType)
				if resource != nil {
					resources = append(resources, resource)
				}
			}

			return resources
		}
	}

	return resources
}

func getResourceByResourceType(strategy *Strategy, resourceType string) *Resource {
	for _, resource := range strategy.Resources {
		if resource.ResourceType == resourceType {
			return resource
		}
	}

	return nil
}

func getCPUByResourceType(resources []*Resource, resourceType string) uint64 {
	for _, resource := range resources {
		if resource.ResourceType == resourceType {
			return resource.CPU
		}
	}

	return 0
}

func getStorageByResourceType(resources []*Resource, resourceType string) uint64 {
	for _, resource := range resources {
		if resource.ResourceType == resourceType {
			return resource.Storage
		}
	}

	return 0
}

func getCountByResourceType(resources []*Resource, resourceType string) *uint64 {
	for _, resource := range resources {
		if resource.ResourceType == resourceType {
			return resource.Count
		}
	}

	return nil
}

func getCPUThresholdByComponent(strategy *Strategy, component ComponentType) (maxThreshold float64, minThreshold float64) {
	for _, rule := range strategy.Rules {
		if rule.Component == component.String() {
			return rule.CPURule.MaxThreshold, rule.CPURule.MinThreshold
		}
	}
	return 0, 0
}

func getStorageThresholdByComponent(strategy *Strategy, component ComponentType) (maxThreshold float64, minThreshold float64) {
	for _, rule := range strategy.Rules {
		if rule.Component == component.String() {
			return rule.StorageRule.MaxThreshold, rule.StorageRule.MinThreshold
		}
	}
	return 0, 0
}

func getHomogeneousResourceType(component ComponentType) string {
	switch component {
	case TiKV:
		return homogeneousTiKVResourceType
	case TiDB:
		return homogeneousTiDBResourceType
	default:
		return ""
	}
}

func sortResourcesByCPUAsc(resources []*Resource) {
	for i := len(resources) - 1; i > 0; i-- {
		sorted := true
		for j := 0; j < i; j++ {
			if resources[j].CPU > resources[j+1].CPU {
				sorted = false
				tmp := resources[j]
				resources[j] = resources[j+1]
				resources[j+1] = tmp
			}
		}

		if sorted {
			break
		}
	}
}

func sortResourcesByCPUDesc(resources []*Resource) {
	for i := len(resources) - 1; i > 0; i-- {
		sorted := true
		for j := 0; j < i; j++ {
			if resources[j].CPU < resources[j+1].CPU {
				sorted = false
				tmp := resources[j]
				resources[j] = resources[j+1]
				resources[j+1] = tmp
			}
		}

		if sorted {
			break
		}
	}
}

func getResourceMapByComponent(rc *cluster.RaftCluster, healthyInstances []instance, component ComponentType) (map[string]uint64, error) {
	var (
		err         error
		resourceMap map[string]uint64
	)

	switch component {
	case TiKV:
		resourceMap, err = getTiKVResourceMap(rc, healthyInstances)
		if err != nil {
			return nil, err
		}
	case TiDB:
		resourceMap, err = getTiDBResourceMap(rc, healthyInstances)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.Errorf("unknown component type %s", component.String())
	}

	return resourceMap, nil
}

func getTiKVResourceMap(rc *cluster.RaftCluster, healthyInstances []instance) (map[string]uint64, error) {
	resourceMap := make(map[string]uint64)

	for _, healthyInstance := range healthyInstances {
		store := rc.GetStore(healthyInstance.id)
		if store == nil {
			log.Warn("inconsistency between health instances and store status, exit auto-scaling calculation",
				zap.Uint64("store-id", healthyInstance.id))
			return nil, errors.New(fmt.Sprintf("inconsistent healthy instance, instance id: %d", healthyInstance.id))
		}

		groupName := store.GetLabelValue(groupLabelKey)
		if isAutoScaledGroup(groupName) {
			resourceType := store.GetLabelValue(resourceTypeLabelKey)
			resourceMap[resourceType]++
			continue
		}

		resourceMap[homogeneousTiKVResourceType]++
	}

	return resourceMap, nil
}

func getTiDBResourceMap(rc *cluster.RaftCluster, healthyInstances []instance) (map[string]uint64, error) {
	resourceMap := make(map[string]uint64)

	for _, healthyInstance := range healthyInstances {
		tidbInfo, err := GetTiDB(rc.GetEtcdClient(), healthyInstance.address)
		if err != nil {
			return nil, err
		}

		groupName := tidbInfo.getLabelValue(groupLabelKey)
		if isAutoScaledGroup(groupName) {
			resourceType := tidbInfo.getLabelValue(resourceTypeLabelKey)
			resourceMap[resourceType]++
			continue
		}

		resourceMap[homogeneousTiDBResourceType]++
	}

	return resourceMap, nil
}

func isAutoScaledGroup(groupName string) bool {
	return len(groupName) > len(autoScalingGroupLabelKeyPrefix) && strings.HasPrefix(groupName, autoScalingGroupLabelKeyPrefix)
}

func getHomogeneousScaleOutPlans(scaleOutCount, totalInstanceCount, nodeCount uint64, resources []*Resource, resourceMap map[string]uint64, component ComponentType) []*Plan {
	var (
		plans        []*Plan
		scaleInPlans []*Plan
		remainCount  uint64
	)

	// sort resources by cpu to minimize the impact when need to scale in heterogeneous instances
	sortResourcesByCPUAsc(resources)

	homogeneousResourceType := getHomogeneousResourceType(component)
	homogeneousCount := getCountByResourceType(resources, homogeneousResourceType)
	count := resourceMap[homogeneousResourceType] + scaleOutCount

	if count > nodeCount {
		// not enough k8s nodes to scale out, set count to node count
		scaleOutCount = nodeCount - resourceMap[homogeneousResourceType]
		count = nodeCount
	}

	if homogeneousCount != nil && count > *homogeneousCount {
		// limited by homogeneous resource count, scale out as much as possible
		scaleOutCount = *homogeneousCount - resourceMap[homogeneousResourceType]
		count = *homogeneousCount
	}

	if totalInstanceCount+scaleOutCount > nodeCount {
		scaleInCount := totalInstanceCount + scaleOutCount - nodeCount
		log.Debug(fmt.Sprintf("autoscale: there are not enough k8s nodes to scale out, need to scale in some heterogeneous instances. scaleOutCount: %d, totalInstanceCount: %d, nodeCount: %d, scaleInCount: %d",
			scaleOutCount, totalInstanceCount, nodeCount, scaleInCount))

		remainCount, scaleInPlans = getScaleInPlansForHomogeneous(scaleInCount, resources, resourceMap, component)
		count -= remainCount

		if scaleInPlans != nil {
			plans = append(plans, scaleInPlans...)
		}
	}

	log.Debug(fmt.Sprintf("autoscale: get homogeneous plans competed. component: %s, homogeneousCount: %d, scaleOutCount: %d, count: %d, remainCount: %d",
		component.String(), homogeneousCount, scaleOutCount, count, remainCount))
	// there are enough k8s nodes to scale out or all heterogeneous instances are scaled in, scale out as much as possible
	return append(plans, NewPlan(component, count, homogeneousResourceType))
}

func getScaleInPlansForHomogeneous(scaleInCount uint64, resources []*Resource, resourceMap map[string]uint64, component ComponentType) (uint64, []*Plan) {
	var plans []*Plan

	for _, resource := range resources {
		if resource.ResourceType != homogeneousTiKVResourceType {
			resourceInstanceCount, ok := resourceMap[resource.ResourceType]
			if ok {
				// this resource type exists, try to scale in
				if scaleInCount <= resourceInstanceCount {
					// scaling in this resource type is enough
					scaleInPlan := NewPlan(TiKV, resourceInstanceCount-scaleInCount, resource.ResourceType)

					return 0, append(plans, scaleInPlan)
				}

				// scaling in this resource type is not enough, need to scale in all instances of this resource type
				scaleInPlan := NewPlan(TiKV, 0, resource.ResourceType)
				plans = append(plans, scaleInPlan)
				scaleInCount -= resourceInstanceCount
			}
		}
	}

	return scaleInCount, plans
}

func mergePlans(plans, groups []*Plan) []*Plan {
	var mergedPlans []*Plan

	// merge homogeneous plans
	for _, plan := range plans {
		groupExists := false
		for _, group := range groups {
			if plan.ResourceType == group.ResourceType {
				groupExists = true
			}
		}

		if !groupExists && plan.Count > 0 {
			mergedPlans = append(mergedPlans, plan.Clone())
		}
	}

	// merge heterogeneous plans
	for _, group := range groups {
		cloned := group.Clone()

		for _, plan := range plans {
			if group.ResourceType == plan.ResourceType {
				cloned.Count = plan.Count
				break
			}
		}

		if cloned.Count > 0 {
			mergedPlans = append(mergedPlans, cloned)
		}
	}

	return mergedPlans
}
