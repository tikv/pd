package checker

import (
	"bytes"
	log "github.com/sirupsen/logrus"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/anti"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
)

type AntiRuleChecker struct {
	cluster         opt.Cluster
	antiRuleManager *anti.AntiRuleManager
	name            string
}

// NewAntiChecker creates a checker instance.
func NewAntiChecker(cluster opt.Cluster, antiRuleManager *anti.AntiRuleManager) *AntiRuleChecker {
	return &AntiRuleChecker{
		cluster:         cluster,
		antiRuleManager: antiRuleManager,
		name:            "anti-affinity-checker",
	}
}

//Check checks the anti rules that fits given region
func (ac *AntiRuleChecker) Check(region *core.RegionInfo) *operator.Operator {
	//anti rule only affects and checks leader region
	if region.GetID() != region.GetLeader().GetId() {
		return nil
	}

	startKey, endKey := region.GetStartKey(), region.GetEndKey()
	var ruleFit anti.AntiRule
	rules := ac.antiRuleManager.GetAntiRules()
	for _, rule := range rules {
		if bytes.Compare(rule.StartKey, startKey) <= 0 && bytes.Compare(rule.EndKey, endKey) >= 0 {
			ruleFit = rule
			/*
				TODO: we only handle the first rule that fits the region's key range(only for testing convenience),
				      other rules should be handle, will support in later days
			*/
			break
		}
	}

	storeScore := ac.antiRuleManager.GetAntiScoreByRuleID(ruleFit.ID)
	if len(storeScore) < 1 {
		return nil
	}
	var minScore, minStoreID, maxScore uint64
	//pick up a record as the initial record
	for ID, score := range storeScore {
		minScore = score
		maxScore = score
		minStoreID = ID

		break
	}
	for ID, score := range storeScore {
		if score > maxScore {
			maxScore = score
			continue
		}
		if score < minScore {
			minScore = score
			minStoreID = ID
		}
	}
	//avoid transfer leader repeatedly
	if maxScore-minScore < 1 {
		return nil
	}

	currStore := region.GetLeader().StoreId

	//transfer leader to the store with min score
	op, err := operator.CreateTransferLeaderOperator("anti rule", ac.cluster, region, 1, minStoreID, operator.OpLeader)

	//update antiScoreMap(decr source store, incr target store)
	if err != nil {
		log.Errorf("create anti rule transferLeader op failed: %s", err.Error())
		return nil
	}
	if err := ac.antiRuleManager.DecrAntiScore(ruleFit.ID, currStore); err != nil {
		return nil
	}
	if err := ac.antiRuleManager.IncrAntiScore(ruleFit.ID, minStoreID); err != nil {
		return nil
	}

	return op
}
