package anti

import (
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"sync"
)

// AntiRuleManager is responsible for the lifecycle of all anti-affinity Rules.
type AntiRuleManager struct {
	sync.RWMutex
	initialized bool
	antiRules []AntiRule
	//antiRuleID -> storeID -> affinityScore
	antiScore map[uint64]map[uint64]uint64
}

// NewAntiRuleManager creates a AntiRuleManager instance.
func NewAntiRuleManager() *AntiRuleManager {
	return &AntiRuleManager{}
}

func (m *AntiRuleManager) GetAntiScoreByRuleID(ruleID uint64) map[uint64]uint64 {
	m.RLock()
	defer m.RUnlock()
	return m.antiScore[ruleID]
}

func (m *AntiRuleManager) IncrAntiScore(ruleID, storeID uint64) error {
	m.Lock()
	defer m.Unlock()
	if store, ok := m.antiScore[ruleID]; ok {
		if _, ok := store[storeID]; ok {
			m.antiScore[ruleID][storeID]++
			return nil
		}
	}
	return errors.Errorf("incr failed, unable to get ruleID(%d) or storeID(%d) in antiScore map", ruleID, storeID)
}

func (m *AntiRuleManager) DecrAntiScore(ruleID, storeID uint64) error {
	m.Lock()
	defer m.Unlock()
	if store, ok := m.antiScore[ruleID]; ok {
		if _, ok := store[storeID]; ok {
			m.antiScore[ruleID][storeID]--
			return nil
		}
	}
	return errors.Errorf("decr failed, unable to get ruleID(%d) or storeID(%d) in antiScore map", ruleID, storeID)
}

// GetAntiRules returns the all the anti rules.
func (m *AntiRuleManager) GetAntiRules() []AntiRule {
	m.RLock()
	defer m.RUnlock()
	return m.getAntiRules()
}

// SetAntiRule inserts an anti Rule.
func (m *AntiRuleManager) SetAntiRule(antiRule *AntiRule) {
	m.Lock()
	defer m.Unlock()
	m.setAntiRule(antiRule)
}

// getAntiRules returns all the AntiRule.
func (m *AntiRuleManager) getAntiRules() []AntiRule {
	m.RLock()
	defer m.RUnlock()
	return m.antiRules
}

// setAntiRule inserts or updates a AntiRule.
func (m *AntiRuleManager) setAntiRule(antiRule *AntiRule) {
	m.Lock()
	defer m.Unlock()
	m.antiRules = append(m.antiRules, *antiRule)
	log.Info("placement antiRule updated", zap.String("antiRule", fmt.Sprint(antiRule)))

}
