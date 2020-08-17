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
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/core"
	"go.uber.org/zap"
)

const (
	groupLabelKey                  = "group"
	autoScalingGroupLabelKeyPrefix = "pd-auto-scaling-"
)

// TODO: adjust the value or make it configurable.
var (
	// MetricsTimeDuration is used to get the metrics of a certain time period.
	MetricsTimeDuration = 5 * time.Second
	// MaxScaleOutStep is used to indicate the maxium number of instance for scaling out operations at once.
	MaxScaleOutStep uint64 = 1
	// MaxScaleInStep is used to indicate the maxium number of instance for scaling in operations at once.
	MaxScaleInStep uint64 = 1
)

func calculate(rc *cluster.RaftCluster, strategy *Strategy) []*Plan {
	var plans []*Plan
	var afterQuota uint64
	if tikvPlans, tikvAfterQuota := getPlans(rc, strategy, TiKV); tikvPlans != nil {
		plans = append(plans, tikvPlans...)
		afterQuota += tikvAfterQuota
	}
	if tidbPlans, tidbAfterQuota := getPlans(rc, strategy, TiDB); tidbPlans != nil {
		plans = append(plans, tidbPlans...)
		afterQuota += tidbAfterQuota
	}
	if exceedMaxCPUQuota(strategy, afterQuota) {
		return nil
	}
	return plans
}

func getPlans(rc *cluster.RaftCluster, strategy *Strategy, component ComponentType) ([]*Plan, uint64) {
	var instances []instance
	if component == TiKV {
		instances = filterTiKVInstances(rc)
	} else {
		instances = getTiDBInstances()
	}

	if len(instances) == 0 {
		return nil, 0
	}

	totalCPUUseTime := getTotalCPUUseTime(component, instances, MetricsTimeDuration)
	currentQuota := getTotalCPUQuota(component, instances)
	totalCPUTime := float64(currentQuota) * MetricsTimeDuration.Seconds()
	usage := totalCPUUseTime / totalCPUTime
	maxThreshold, minThreshold := getCPUThresholdByComponent(strategy, component)

	// TODO: add metrics to show why it triggers scale in/out.
	if usage > maxThreshold {
		scaleOutQuota := (totalCPUUseTime - totalCPUTime*maxThreshold) / MetricsTimeDuration.Seconds()
		return calculateScaleOutPlan(rc, strategy, component, scaleOutQuota, currentQuota, instances)
	}
	if usage < minThreshold {
		scaleInQuota := (totalCPUTime*minThreshold - totalCPUUseTime) / MetricsTimeDuration.Seconds()
		return calculateScaleInPlan(rc, strategy, component, scaleInQuota, instances), 0
	}
	return nil, 0
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

// TODO: get TiDB instances
func getTiDBInstances() []instance {
	return []instance{}
}

// TODO: get total CPU use time through Prometheus.
func getTotalCPUUseTime(component ComponentType, instances []instance, duration time.Duration) float64 {
	return 1.0
}

// TODO: get total CPU quota through Prometheus.
func getTotalCPUQuota(component ComponentType, instances []instance) uint64 {
	return 1
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

func calculateScaleOutPlan(rc *cluster.RaftCluster, strategy *Strategy, component ComponentType, scaleOutQuota float64, currentQuota uint64, instances []instance) ([]*Plan, uint64) {
	groups := getScaledGroupsByComponent(rc, component, instances)
	group := findBestGroupToScaleOut(rc, strategy, scaleOutQuota, groups, component)

	resCPU := float64(getCPUByResourceType(strategy, group.ResourceType))
	scaleOutCount := typeutil.MinUint64(uint64(math.Ceil(scaleOutQuota/resCPU)), MaxScaleOutStep)
	increasedQuota := getCPUByResourceType(strategy, group.ResourceType) * scaleOutCount
	afterQuota := currentQuota + increasedQuota

	// A new group created
	if len(groups) == 0 {
		return []*Plan{&group}, afterQuota
	}

	// update the existed group
	for i, g := range groups {
		if g.ResourceType == group.ResourceType {
			group.Count += scaleOutCount
			groups[i] = &group
		}
	}
	return groups, afterQuota
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

func exceedMaxCPUQuota(strategy *Strategy, totalCPUQuota uint64) bool {
	return totalCPUQuota > strategy.MaxCPUQuota
}

func getCPUByResourceType(strategy *Strategy, resourceType string) uint64 {
	for _, res := range strategy.Resources {
		if res.ResourceType == resourceType {
			return res.CPU
		}
	}
	return 0
}

// TODO: get the scaled groups
func getScaledGroupsByComponent(rc *cluster.RaftCluster, component ComponentType, healthyInstances []instance) []*Plan {
	switch component {
	case TiKV:
		return getScaledTiKVGroups(rc, healthyInstances)
	case TiDB:
		// TODO: support search TiDB Group
		return []*Plan{}
	default:
		return nil
	}
}

func getScaledTiKVGroups(informer core.StoreSetInformer, healthyInstances []instance) []*Plan {
	var plans []*Plan
	planMap := make(map[string]map[uint64]struct{}, len(healthyInstances))
	for _, instance := range healthyInstances {
		store := informer.GetStore(instance.id)
		if store == nil {
			log.Warn("inconsistency between health instances and store status, exit auto-scaling calculation",
				zap.Uint64("store-id", instance.id))
			return nil
		}
		v := store.GetLabelValue(groupLabelKey)
		if len(v) > len(autoScalingGroupLabelKeyPrefix) &&
			v[:len(autoScalingGroupLabelKeyPrefix)] == autoScalingGroupLabelKeyPrefix {
			if stores, ok := planMap[v]; ok {
				stores[instance.id] = struct{}{}
			} else {
				planMap[v] = map[uint64]struct{}{
					instance.id: {},
				}
			}
		}
	}
	for groupLabel, groupInstances := range planMap {
		plans = append(plans, &Plan{
			Component: TiKV.String(),
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
