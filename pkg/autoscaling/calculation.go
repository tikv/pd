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
	"math"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	promClient "github.com/prometheus/client_golang/api"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/core"
	"go.uber.org/zap"
)

const (
	DefaultTimeout                 = 5 * time.Second
	prometheusAddressKey           = "/topology/prometheus"
	prometheusPort                 = 9090
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

func calculate(rc *cluster.RaftCluster, strategy *Strategy) []*Plan {
	var plans []*Plan

	prometheusAddress, err := getPrometheusAddress(rc)
	if err != nil {
		log.Error("error getting prometheus address", errs.ZapError(err))
	}

	client, err := promClient.NewClient(promClient.Config{Address: prometheusAddress})
	if err != nil {
		log.Error("error initializing Prometheus client", zap.String("prometheusAddress", prometheusAddress), errs.ZapError(errs.ErrPrometheusCreateClient, err))
		return nil
	}
	querier := NewPrometheusQuerier(client)

	for _, rule := range strategy.Rules {
		switch rule.Component {
		case TiKV.String():
			tikvPlans, err := getTiKVPlans(rc, querier, strategy)
			if err != nil {
				log.Error("error getting tikv plans", errs.ZapError(err))
				return nil
			}

			plans = append(plans, tikvPlans...)
		case TiDB.String():
			tidbPlans, err := getTiDBPlans(rc, querier, strategy)
			if err != nil {
				log.Error("error getting tidb plans", errs.ZapError(err))
				return nil
			}

			plans = append(plans, tidbPlans...)
		}
	}

	return plans
}

func getTiKVPlans(rc *cluster.RaftCluster, querier Querier, strategy *Strategy) ([]*Plan, error) {
	var (
		plans        []*Plan
		storageCount uint64
	)

	instances := getTiKVInstances(rc)
	if len(instances) == 0 {
		return nil, nil
	}

	storagePlan, err := getTiKVStoragePlan(rc, instances, strategy)
	if err != nil {
		return nil, err
	}
	if storagePlan != nil {
		storageCount = storagePlan.Count - uint64(len(instances))
		plans = append(plans, storagePlan)
	}

	cpuPlans, err := getCPUPlans(rc, querier, instances, strategy, TiKV, storageCount)
	if err != nil {
		return nil, err
	}
	if cpuPlans != nil {
		plans = append(plans, cpuPlans...)
	}

	return plans, nil
}

func getTiKVStoragePlan(rc *cluster.RaftCluster, instances []instance, strategy *Strategy) (*Plan, error) {
	if len(instances) == 0 {
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
	if storageUsage > storageMaxThreshold {
		// generate homogeneous tikv plan
		resources := getCPUResourcesByComponent(strategy, TiKV)
		storageScaleSize := totalStorageUsedSize/storageUsageTarget - totalStorageCapacity
		storeStorageSize := getStorageByResourceType(resources, homogeneousTiKVResourceType)
		count := uint64(storageScaleSize)/storeStorageSize + 1 + uint64(len(instances))

		return NewPlan(TiKV, count, homogeneousTiKVResourceType), nil
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

	prometheusAddress := &PrometheusAddress{}
	err = json.Unmarshal(resp.Kvs[0].Value, prometheusAddress)
	if err != nil {
		return "", err
	}

	return prometheusAddress.String(), nil
}

func getTiDBPlans(rc *cluster.RaftCluster, querier Querier, strategy *Strategy) ([]*Plan, error) {
	instances, err := getTiDBInstances(rc)
	if err != nil {
		return nil, err
	}

	return getCPUPlans(rc, querier, instances, strategy, TiDB, 0)
}

func getCPUPlans(rc *cluster.RaftCluster, querier Querier, instances []instance, strategy *Strategy, component ComponentType, storageCount uint64) ([]*Plan, error) {
	var plans []*Plan

	now := time.Now()
	// get cpu used times
	cpuUsedTimes, err := querier.Query(NewQueryOptions(component, CPUUsage, getAddresses(instances), now, MetricsTimeDuration))
	if err != nil {
		return nil, err
	}
	// get total cpu quota
	currentQuota, err := getTotalCPUQuota(querier, component, instances, now)
	if err != nil {
		return nil, err
	}

	var (
		totalCPUUsedTime float64
		cpuUsageLowNum   int
		count            uint64
	)

	// get cpu threshold
	cpuMaxThreshold, cpuMinThreshold := getCPUThresholdByComponent(strategy, component)
	cpuUsageHighMap := make(map[string][]float64)
	for resourceType, cpuUsedTime := range cpuUsedTimes {
		totalCPUUsedTime += cpuUsedTime
		cpuUsage := cpuUsedTime / MetricsTimeDuration.Seconds()

		if cpuUsage > cpuMaxThreshold {
			cpuUsageHighMap[resourceType] = append(cpuUsageHighMap[resourceType], cpuUsage)
			continue
		}
		if cpuUsage < cpuMinThreshold {
			cpuUsageLowNum++
		}
	}
	resources := getCPUResourcesByComponent(strategy, component)
	totalCPUQuota := float64(currentQuota) / float64(milliCores) * MetricsTimeDuration.Seconds()
	homogeneousResourceType := getHomogeneousResourceType(component)
	homogeneousCPUSize := getCPUByResourceType(resources, homogeneousResourceType)

	if component == TiKV && storageCount > 0 {
		// add quota from storage plan
		totalCPUQuota += float64(storageCount * homogeneousCPUSize)
		count = storageCount
	}

	totalCPUUsage := totalCPUUsedTime / totalCPUQuota
	cpuUsageTarget := (cpuMaxThreshold + cpuMinThreshold) / 2
	if totalCPUUsage > cpuMaxThreshold {
		// generate homogeneous scale out plan
		cpuScaleOutSize := (totalCPUUsedTime/cpuUsageTarget - totalCPUQuota) * milliCores
		count += uint64(cpuScaleOutSize/float64(homogeneousCPUSize)) + 1 + uint64(len(instances))

		return append(plans, NewPlan(component, count, homogeneousResourceType)), nil
	}

	// get scaled groups
	groups, err := getScaledGroupsByComponent(rc, instances, component)
	if err != nil {
		return nil, err
	}

	if len(cpuUsageHighMap) > 0 {
		// generate heterogeneous scale out plans
		cpuScaleOutSize := 0.0
		for resourceType, cpuUsageList := range cpuUsageHighMap {
			resourceCPUSize := getCPUByResourceType(resources, resourceType)
			for _, cpuUsage := range cpuUsageList {
				cpuScaleOutSize += (cpuUsage - cpuUsageTarget) * float64(resourceCPUSize) * milliCores
			}
		}

		return getHeterogeneousScaleOutPlans(cpuScaleOutSize, cpuUsageTarget, groups, resources), nil
	}

	if cpuUsageLowNum == len(instances) {
		// generate heterogeneous scale in plans
		return getHeterogeneousScaleInPlans(groups), nil
	}

	return nil, nil
}

func getHeterogeneousScaleOutPlans(cpuScaleOutSize float64, cpuUsageTarget float64, groups []*Plan, resources []*Resource) []*Plan {
	plans := deepCopyPlans(groups)
	// sort resources by cpu desc
	sortResourcesByCPUDesc(resources)

	for _, resource := range resources {
		if resource.ResourceType == homogeneousTiKVResourceType || resource.ResourceType == homogeneousTiDBResourceType {
			// jump over homogeneous resource type
			continue
		}

		group := getScaledGroupByResourceType(groups, resource.ResourceType)
		scaleOutCount := uint64(cpuScaleOutSize/float64(resource.CPU)/cpuUsageTarget) + 1

		if group == nil {
			// scaled group does not exist
			if resource.Count == nil || *resource.Count >= scaleOutCount {
				// unlimited resource count or enough resource count left
				return append(plans, NewPlan(TiKV, scaleOutCount, resource.ResourceType))
			}
			if *resource.Count > 0 {
				// not enough count left, use as much as it left
				plans = append(plans, NewPlan(TiKV, *resource.Count, resource.ResourceType))
				cpuScaleOutSize -= float64(resource.CPU) * cpuUsageTarget * float64(*resource.Count)
				continue
			}
		}

		// scaled group exists
		plan := getScaledTiKVGroupByResourceType(plans, resource.ResourceType)

		if resource.Count == nil || *resource.Count >= scaleOutCount+plan.Count {
			// unlimited resource count or enough count left
			plan.Count += scaleOutCount

			return plans
		}
		if *resource.Count > plan.Count {
			// not enough count left, use as much as it left
			cpuScaleOutSize -= float64(resource.CPU) * cpuUsageTarget * float64(*resource.Count-plan.Count)
			plan.Count = *resource.Count
		}
	}

	return plans
}

func getHeterogeneousScaleInPlans(groups []*Plan) []*Plan {
	plans := deepCopyPlans(groups)

	for i, plan := range plans {
		plan.Count -= MaxScaleInStep

		if plan.Count <= 0 {
			plans = append(plans[:i], plans[i+1:]...)
		}

		return plans
	}

	return plans
}

func deepCopyPlans(plans []*Plan) []*Plan {
	var newPlans []*Plan

	for _, plan := range plans {
		newPlans = append(newPlans, plan.Clone())
	}

	return newPlans
}

func getScaledGroupByResourceType(groups []*Plan, resourceType string) *Plan {
	for _, group := range groups {
		if group.ResourceType == resourceType {
			return group
		}
	}

	return nil
}

func getScaledTiKVGroupByResourceType(groups []*Plan, resourceType string) *Plan {
	for _, group := range groups {
		if group.ResourceType == resourceType {
			return group
		}
	}

	return nil
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

func getAddresses(instances []instance) []string {
	names := make([]string, 0, len(instances))
	for _, inst := range instances {
		names = append(names, inst.address)
	}
	return names
}

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

func getTotalStorageInfo(informer core.StoreSetInformer, healthyInstances []instance) (float64, float64, error) {
	var (
		totalStorageUsedSize uint64
		totalStorageCapacity uint64
	)

	for _, healthyInstance := range healthyInstances {
		store := informer.GetStore(healthyInstance.id)
		if store == nil {
			log.Warn("inconsistency between health instances and store status, exit auto-scaling calculation",
				zap.Uint64("store-id", healthyInstance.id))
			return 0, 0, errors.New(fmt.Sprintf("inconsistent healthy instance, instance id: %d", healthyInstance.id))
		}

		groupName := store.GetLabelValue(groupLabelKey)
		totalStorageUsedSize += store.GetUsedSize()
		if !isAutoScaledGroup(groupName) {
			// TODO: find out the unit of the size
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

func getScaledGroupsByComponent(rc *cluster.RaftCluster, healthyInstances []instance, component ComponentType) ([]*Plan, error) {
	var (
		err         error
		resourceMap map[string]uint64
		groups      []*Plan
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

	for resourceType, count := range resourceMap {
		groups = append(groups, NewPlan(TiKV, count, resourceType))
	}

	return groups, nil
}

func sortResourcesByCPUDesc(resources []*Resource) {
	for i := len(resources) - 1; i > 0; i-- {
		sorted := true
		j := 0
		for ; j < i; j++ {
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
		if !isAutoScaledGroup(groupName) {
			continue
		}

		resourceType := store.GetLabelValue(resourceTypeLabelKey)
		resourceMap[resourceType]++
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
		if !isAutoScaledGroup(groupName) {
			continue
		}

		resourceType := tidbInfo.getLabelValue(resourceTypeLabelKey)
		resourceMap[resourceType]++
	}

	return resourceMap, nil
}

func isAutoScaledGroup(groupName string) bool {
	return len(groupName) > len(autoScalingGroupLabelKeyPrefix) && strings.HasPrefix(groupName, autoScalingGroupLabelKeyPrefix)
}
