package anti

import (
	"fmt"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"sync"
)

// AntiRuleManager is responsible for the lifecycle of all anti-affinity Rules.
// It is thread safe.
type AntiRuleManager struct {
	sync.RWMutex
	initialized bool
	//antiRuleConfig *antiRuleConfig
	antiRules []AntiRule
}

// NewAntiRuleManager creates a AntiRuleManager instance.
func NewAntiRuleManager() *AntiRuleManager {
	return &AntiRuleManager{}
}


// GetRule returns the all the anti rules
func (m *AntiRuleManager) GetAntiRules() []AntiRule {
	m.RLock()
	defer m.RUnlock()
	return m.getAntiRules()
}

// SetRule inserts or updates a Rule.
func (m *AntiRuleManager) SetAntiRule(antiRule *AntiRule) {
	m.Lock()
	defer m.Unlock()
	m.setAntiRule(antiRule)
}

// getAntiRule returns the AntiRule with the same (group, id).
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



/*

// Initialize loads antiRules from storage. If Placement Rules feature is never enabled, it creates default antiRule that is
// compatible with previous configuration.
func (m *AntiRuleManager) Initialize() error {
	m.Lock()
	defer m.Unlock()
	if m.initialized {
		return nil
	}

	m.initialized = true
	return nil
}

func (m *AntiRuleManager) loadRules() error {

	return nil
}

// DeleteAntiRule removes a Rule.
func (m *AntiRuleManager) DeleteAntiRule(group, id string) error {
	m.Lock()
	defer m.Unlock()
	p := m.beginPatch()
	p.deleteAntiRule(group, id)
	if err := m.tryCommitPatch(p); err != nil {
		return err
	}
	log.Info("placement rule is removed", zap.String("group", group), zap.String("id", id))
	return nil
}


// DeleteRule removes a AntiRule.
func (m *AntiRuleManager) DeleteRule(group, id string) error {
	m.Lock()
	defer m.Unlock()
	p := m.beginPatch()
	p.deleteAntiRule(group, id)
	if err := m.tryCommitPatch(p); err != nil {
		return err
	}
	log.Info("placement antiRule is removed", zap.String("group", group), zap.String("id", id))
	return nil
}

// GetSplitKeys returns all split keys in the range (start, end).
func (m *AntiRuleManager) GetSplitKeys(start, end []byte) [][]byte {
	m.RLock()
	defer m.RUnlock()
	return m.antiRuleList.getSplitKeys(start, end)
}

// GetAllRules returns sorted all antiRules.
func (m *AntiRuleManager) GetAllRules() []*AntiRule {
	m.RLock()
	defer m.RUnlock()
	antiRules := make([]*AntiRule, 0, len(m.antiRuleConfig.antiRules))
	for _, r := range m.antiRuleConfig.antiRules {
		antiRules = append(antiRules, r)
	}
	sortRules(antiRules)
	return antiRules
}

// GetRulesByGroup returns sorted antiRules of a group.
func (m *AntiRuleManager) GetRulesByGroup(group string) []*AntiRule {
	m.RLock()
	defer m.RUnlock()
	var antiRules []*AntiRule
	for _, r := range m.antiRuleConfig.antiRules {
		if r.GroupID == group {
			antiRules = append(antiRules, r)
		}
	}
	sortRules(antiRules)
	return antiRules
}

// GetRulesByKey returns sorted antiRules that affects a key.
func (m *AntiRuleManager) GetRulesByKey(key []byte) []*AntiRule {
	m.RLock()
	defer m.RUnlock()
	return m.antiRuleList.getRulesByKey(key)
}

// GetRulesForApplyRegion returns the antiRules list that should be applied to a region.
func (m *AntiRuleManager) GetRulesForApplyRegion(region *core.RegionInfo) []*AntiRule {
	m.RLock()
	defer m.RUnlock()
	return m.antiRuleList.getRulesForApplyRegion(region.GetStartKey(), region.GetEndKey())
}

// FitRegion fits a region to the antiRules it matches.
func (m *AntiRuleManager) FitRegion(stores StoreSet, region *core.RegionInfo) *RegionFit {
	antiRules := m.GetRulesForApplyRegion(region)
	return FitRegion(stores, region, antiRules)
}

func (m *AntiRuleManager) beginPatch() *antiRuleConfigPatch {
	return m.antiRuleConfig.beginPatch()
}

func (m *AntiRuleManager) tryCommitPatch(patch *antiRuleConfigPatch) error {
	patch.adjust()

	antiRuleList, err := buildRuleList(patch)
	if err != nil {
		return err
	}

	patch.trim()

	// save updates
	err = m.savePatch(patch.mut)
	if err != nil {
		return err
	}

	// update in-memory state
	patch.commit()
	m.antiRuleList = antiRuleList
	return nil
}

func (m *AntiRuleManager) savePatch(p *antiRuleConfig) error {
	// TODO: it is not completely safe
	// 1. in case that half of antiRules applied, error.. we have to cancel persisted antiRules
	// but that may fail too, causing memory/disk inconsistency
	// either rely a transaction API, or clients to request again until success
	// 2. in case that PD is suddenly down in the loop, inconsistency again
	// now we can only rely clients to request again
	var err error
	for key, r := range p.antiRules {
		if r == nil {
			r = &AntiRule{GroupID: key[0], ID: key[1]}
			err = m.store.DeleteRule(r.StoreKey())
		} else {
			err = m.store.SaveRule(r.StoreKey(), r)
		}
		if err != nil {
			return err
		}
	}
	for id, g := range p.groups {
		if g.isDefault() {
			err = m.store.DeleteRuleGroup(id)
		} else {
			err = m.store.SaveRuleGroup(id, g)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// SetRules inserts or updates lots of Rules at once.
func (m *AntiRuleManager) SetAntiRules(antiRules []*AntiRule) error {
	m.Lock()
	defer m.Unlock()
	p := m.beginPatch()
	for _, r := range antiRules {
		if err := m.adjustAntiRule(r); err != nil {
			return err
		}
		p.setAntiRule(r)
	}
	if err := m.tryCommitPatch(p); err != nil {
		return err
	}

	log.Info("placement antiRules updated", zap.String("antiRules", fmt.Sprint(antiRules)))
	return nil
}

// RuleOpType indicates the operation type
type RuleOpType string

const (
	// RuleOpAdd a placement antiRule, only need to specify the field *AntiRule
	RuleOpAdd RuleOpType = "add"
	// RuleOpDel a placement antiRule, only need to specify the field `GroupID`, `ID`, `MatchID`
	RuleOpDel RuleOpType = "del"
)

// RuleOp is for batching placement antiRule actions. The action type is
// distinguished by the field `Action`.
type RuleOp struct {
	*AntiRule                   // information of the placement antiRule to add/delete
	Action           RuleOpType `json:"action"` // the operation type
	DeleteByIDPrefix bool       `json:"delete_by_id_prefix"` // if action == delete, delete by the prefix of id
}

func (r RuleOp) String() string {
	b, _ := json.Marshal(r)
	return string(b)
}

// Batch executes a series of actions at once.
func (m *AntiRuleManager) Batch(todo []RuleOp) error {
	for _, t := range todo {
		switch t.Action {
		case RuleOpAdd:
			err := m.adjustAntiRule(t.AntiRule)
			if err != nil {
				return err
			}
		}
	}

	m.Lock()
	defer m.Unlock()

	patch := m.beginPatch()
	for _, t := range todo {
		switch t.Action {
		case RuleOpAdd:
			patch.setAntiRule(t.AntiRule)
		case RuleOpDel:
			if !t.DeleteByIDPrefix {
				patch.deleteAntiRule(t.GroupID, t.ID)
			} else {
				m.antiRuleConfig.iterateAntiRules(func(r *AntiRule) {
					if r.GroupID == t.GroupID && strings.HasPrefix(r.ID, t.ID) {
						patch.deleteAntiRule(r.GroupID, r.ID)
					}
				})
			}
		}
	}

	if err := m.tryCommitPatch(patch); err != nil {
		return err
	}

	log.Info("placement antiRules updated", zap.String("batch", fmt.Sprint(todo)))
	return nil
}

// GetRuleGroup returns a AntiRuleGroup configuration.
func (m *AntiRuleManager) GetRuleGroup(id string) *AntiRuleGroup {
	m.RLock()
	defer m.RUnlock()
	return m.antiRuleConfig.groups[id]
}

// GetRuleGroups returns all AntiRuleGroup configuration.
func (m *AntiRuleManager) GetRuleGroups() []*AntiRuleGroup {
	m.RLock()
	defer m.RUnlock()
	var groups []*AntiRuleGroup
	for _, g := range m.antiRuleConfig.groups {
		groups = append(groups, g)
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Index < groups[j].Index ||
			(groups[i].Index == groups[j].Index && groups[i].ID < groups[j].ID)
	})
	return groups
}

// SetRuleGroup updates a AntiRuleGroup.
func (m *AntiRuleManager) SetRuleGroup(group *AntiRuleGroup) error {
	m.Lock()
	defer m.Unlock()
	p := m.beginPatch()
	p.setGroup(group)
	if err := m.tryCommitPatch(p); err != nil {
		return err
	}
	log.Info("group config updated", zap.String("group", fmt.Sprint(group)))
	return nil
}

// DeleteRuleGroup removes a AntiRuleGroup.
func (m *AntiRuleManager) DeleteRuleGroup(id string) error {
	m.Lock()
	defer m.Unlock()
	p := m.beginPatch()
	p.deleteGroup(id)
	if err := m.tryCommitPatch(p); err != nil {
		return err
	}
	log.Info("group config reset", zap.String("group", id))
	return nil
}

// GetAllGroupBundles returns all antiRules and groups configuration. Rules are
// grouped by groups.
func (m *AntiRuleManager) GetAllGroupBundles() []GroupBundle {
	m.RLock()
	defer m.RUnlock()
	var bundles []GroupBundle
	for _, g := range m.antiRuleConfig.groups {
		bundles = append(bundles, GroupBundle{
			ID:       g.ID,
			Index:    g.Index,
			Override: g.Override,
		})
	}
	for _, r := range m.antiRuleConfig.antiRules {
		for i := range bundles {
			if bundles[i].ID == r.GroupID {
				bundles[i].AntiRules = append(bundles[i].AntiRules, r)
			}
		}
	}
	sort.Slice(bundles, func(i, j int) bool {
		return bundles[i].Index < bundles[j].Index ||
			(bundles[i].Index == bundles[j].Index && bundles[i].ID < bundles[j].ID)
	})
	for _, b := range bundles {
		sortRules(b.AntiRules)
	}
	return bundles
}

// GetGroupBundle returns a group and all antiRules belong to it.
func (m *AntiRuleManager) GetGroupBundle(id string) (b GroupBundle) {
	m.RLock()
	defer m.RUnlock()
	b.ID = id
	if g := m.antiRuleConfig.groups[id]; g != nil {
		b.Index, b.Override = g.Index, g.Override
		for _, r := range m.antiRuleConfig.antiRules {
			if r.GroupID == id {
				b.AntiRules = append(b.AntiRules, r)
			}
		}
		sortRules(b.AntiRules)
	}
	return
}

// SetAllGroupBundles resets configuration. If override is true, all old configurations are dropped.
func (m *AntiRuleManager) SetAllGroupBundles(groups []GroupBundle, override bool) error {
	m.Lock()
	defer m.Unlock()
	p := m.beginPatch()
	matchID := func(a string) bool {
		for _, g := range groups {
			if g.ID == a {
				return true
			}
		}
		return false
	}
	for k := range m.antiRuleConfig.antiRules {
		if override || matchID(k[0]) {
			p.deleteAntiRule(k[0], k[1])
		}
	}
	for id := range m.antiRuleConfig.groups {
		if override || matchID(id) {
			p.deleteGroup(id)
		}
	}
	for _, g := range groups {
		p.setGroup(&AntiRuleGroup{
			ID:       g.ID,
			Index:    g.Index,
			Override: g.Override,
		})
		for _, r := range g.AntiRules {
			if err := m.adjustAntiRule(r); err != nil {
				return err
			}
			p.setAntiRule(r)
		}
	}
	if err := m.tryCommitPatch(p); err != nil {
		return err
	}
	log.Info("full config reset", zap.String("config", fmt.Sprint(groups)))
	return nil
}

// SetGroupBundle resets a Group and all antiRules belong to it. All old antiRules
// belong to the Group are dropped.
func (m *AntiRuleManager) SetGroupBundle(group GroupBundle) error {
	m.Lock()
	defer m.Unlock()
	p := m.beginPatch()
	if _, ok := m.antiRuleConfig.groups[group.ID]; ok {
		for k := range m.antiRuleConfig.antiRules {
			if k[0] == group.ID {
				p.deleteAntiRule(k[0], k[1])
			}
		}
	}
	p.setGroup(&AntiRuleGroup{
		ID:       group.ID,
		Index:    group.Index,
		Override: group.Override,
	})
	for _, r := range group.AntiRules {
		if err := m.adjustAntiRule(r); err != nil {
			return err
		}
		p.setAntiRule(r)
	}
	if err := m.tryCommitPatch(p); err != nil {
		return err
	}
	log.Info("group is reset", zap.String("group", fmt.Sprint(group)))
	return nil
}

// DeleteGroupBundle removes a Group and all antiRules belong to it. If `regex` is
// true, `id` is a regexp expression.
func (m *AntiRuleManager) DeleteGroupBundle(id string, regex bool) error {
	m.Lock()
	defer m.Unlock()
	matchID := func(a string) bool { return a == id }
	if regex {
		r, err := regexp.Compile(id)
		if err != nil {
			return err
		}
		matchID = func(a string) bool { return r.MatchString(a) }
	}

	p := m.beginPatch()
	for k := range m.antiRuleConfig.antiRules {
		if matchID(k[0]) {
			p.deleteAntiRule(k[0], k[1])
		}
	}
	for _, g := range m.antiRuleConfig.groups {
		if matchID(g.ID) {
			p.deleteGroup(g.ID)
		}
	}
	if err := m.tryCommitPatch(p); err != nil {
		return err
	}
	log.Info("groups are removed", zap.String("id", id), zap.Bool("regexp", regex))
	return nil
}

// IsInitialized returns whether the antiRule manager is initialized.
func (m *AntiRuleManager) IsInitialized() bool {
	m.RLock()
	defer m.RUnlock()
	return m.initialized
}
*/
