// Copyright 2022 TiKV Project Authors.
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

package keyspace

import (
	"bytes"
	"container/heap"
	"encoding/hex"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/google/btree"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/codec"
	coreconstant "github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
)

const (
	// namePattern is a regex that specifies acceptable characters of the keyspace name.
	// Valid name must be non-empty and 64 characters or fewer and consist only of letters (a-z, A-Z),
	// numbers (0-9), hyphens (-), and underscores (_).
	// currently, we enforce this rule to keyspace_name.
	namePattern = "^[-A-Za-z0-9_]{1,20}$"
)

var (
	errNoAvailableMetaServiceGroups = errors.New("no available meta-service groups")

	// ErrUnknownMetaServiceGroup is returned when the specified meta-service group does not exist.
	ErrUnknownMetaServiceGroup = errors.New("unknown meta-service group")
	// ErrInvalidAssignmentCount is returned when the patched assignment count is negative.
	ErrInvalidAssignmentCount = errors.New("assignment count must be non-negative")
	// ErrMetaServiceGroupDisabled is returned when assigning a keyspace to a
	// disabled meta-service group, which is not eligible for assignment.
	ErrMetaServiceGroupDisabled = errors.New("meta-service group is disabled")
	// ErrGroupHasAssignedKeyspaces is returned when deleting a meta-service group
	// that still has keyspaces assigned to it. It is exported so HTTP handlers can
	// map it to a 400 Bad Request via errors.Is.
	ErrGroupHasAssignedKeyspaces = errors.New("cannot delete meta-service group with assigned keyspaces")

	// stateTransitionTable lists all allowed next state for the given current state.
	// Note that transit from any state to itself is allowed for idempotence.
	stateTransitionTable = map[keyspacepb.KeyspaceState][]keyspacepb.KeyspaceState{
		keyspacepb.KeyspaceState_ENABLED:   {keyspacepb.KeyspaceState_ENABLED, keyspacepb.KeyspaceState_DISABLED},
		keyspacepb.KeyspaceState_DISABLED:  {keyspacepb.KeyspaceState_DISABLED, keyspacepb.KeyspaceState_ENABLED, keyspacepb.KeyspaceState_ARCHIVED},
		keyspacepb.KeyspaceState_ARCHIVED:  {keyspacepb.KeyspaceState_ARCHIVED, keyspacepb.KeyspaceState_TOMBSTONE},
		keyspacepb.KeyspaceState_TOMBSTONE: {keyspacepb.KeyspaceState_TOMBSTONE},
	}
	// Only keyspaces in the state specified by allowChangeConfig are allowed to change their config.
	allowChangeConfig = []keyspacepb.KeyspaceState{keyspacepb.KeyspaceState_ENABLED, keyspacepb.KeyspaceState_DISABLED}
)

// validateID check if keyspace falls within the acceptable range.
// It throws errIllegalID when input id is our of range,
// or if it collides with reserved id.
func validateID(id uint32) error {
	if id > constant.MaxValidKeyspaceID {
		return errors.Errorf("illegal keyspace id %d, larger than spaceID Max %d", id, constant.MaxValidKeyspaceID)
	}
	if isProtectedKeyspaceID(id) {
		return errors.Errorf("illegal keyspace id %d, collides with a protected keyspace id", id)
	}
	return nil
}

// validateName check if user provided name is legal.
// It throws errIllegalName when name contains illegal character,
// or if it collides with reserved name.
func validateName(name string) error {
	isValid, err := regexp.MatchString(namePattern, name)
	if err != nil {
		return err
	}
	if !isValid {
		return errors.Errorf("illegal keyspace name %s, should contain only alphanumerical and underline", name)
	}
	if isProtectedKeyspaceName(name) {
		return errors.Errorf("illegal keyspace name %s, collides with a protected keyspace name", name)
	}
	return nil
}

// MaskKeyspaceID is used to hash the spaceID inside the lockGroup.
// A simple mask is applied to spaceID to use its last byte as map key,
// limiting the maximum map length to 256.
// Since keyspaceID is sequentially allocated, this can also reduce the chance
// of collision when comparing with random hashes.
func MaskKeyspaceID(id uint32) uint32 {
	return id & 0xFF
}

// RegionBound represents the region boundary of the given keyspace.
// For a keyspace with id ['a', 'b', 'c'], it has four boundaries:
//
//	Lower bound for raw mode: ['r', 'a', 'b', 'c']
//	Upper bound for raw mode: ['r', 'a', 'b', 'c + 1']
//	Lower bound for txn mode: ['x', 'a', 'b', 'c']
//	Upper bound for txn mode: ['x', 'a', 'b', 'c + 1']
//	For the max valid keyspace ID, the upper bound advances the mode byte as an exclusive fencepost.
//
// From which it shares the lower bound with keyspace with id ['a', 'b', 'c-1'].
// And shares upper bound with keyspace with id ['a', 'b', 'c + 1'].
// These repeated bound will not cause any problem, as repetitive bound will be ignored during rangeListBuild,
// but provides guard against hole in keyspace allocations should it occur.
type RegionBound struct {
	RawLeftBound  []byte
	RawRightBound []byte
	TxnLeftBound  []byte
	TxnRightBound []byte
}

// MakeRegionBound constructs the correct region boundaries of the given keyspace.
func MakeRegionBound(id uint32) *RegionBound {
	rawLeftBound := codec.EncodeKeyspaceBoundary(codec.RawKeyspaceModePrefix, id)
	rawRightBound := codec.EncodeKeyspaceBoundary(codec.RawKeyspaceModePrefix, id+1)
	txnLeftBound := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, id)
	txnRightBound := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, id+1)
	return &RegionBound{
		RawLeftBound:  rawLeftBound[:],
		RawRightBound: rawRightBound[:],
		TxnLeftBound:  txnLeftBound[:],
		TxnRightBound: txnRightBound[:],
	}
}

// MakeKeyRanges encodes keyspace ID to correct LabelRule data with the specified
// region bound. Used by tests and pd-ctl.
func MakeKeyRanges(id uint32, keyType string) []any {
	if keyType == coreconstant.Raw.String() {
		return buildKeyRanges(id, rawRegionBound)
	}
	return buildKeyRanges(id, txnRegionBound)
}

func buildKeyRanges(id uint32, boundType regionBoundType) []any {
	regionBound := MakeRegionBound(id)
	if boundType == txnRegionBound {
		return []any{
			map[string]any{
				"start_key": hex.EncodeToString(regionBound.TxnLeftBound),
				"end_key":   hex.EncodeToString(regionBound.TxnRightBound),
			},
		}
	}
	return []any{
		map[string]any{
			"start_key": hex.EncodeToString(regionBound.RawLeftBound),
			"end_key":   hex.EncodeToString(regionBound.RawRightBound),
		},
	}
}

// getRegionLabelID returns the region label id of the target keyspace.
func getRegionLabelID(id uint32) string {
	return constant.RegionLabelIDPrefix + strconv.FormatUint(uint64(id), endpoint.SpaceIDBase)
}

// MakeTxnLabelRule makes the label rule for the given keyspace id, only for test
func MakeTxnLabelRule(id uint32) *labeler.LabelRule {
	return buildLabelRule(id, txnRegionBound)
}

func buildLabelRule(id uint32, boundType regionBoundType) *labeler.LabelRule {
	return &labeler.LabelRule{
		ID:    getRegionLabelID(id),
		Index: 0,
		Labels: []labeler.RegionLabel{
			{
				Key:   constant.RegionLabelKey,
				Value: strconv.FormatUint(uint64(id), endpoint.SpaceIDBase),
			},
		},
		RuleType: labeler.KeyRange,
		Data:     buildKeyRanges(id, boundType),
	}
}

// ParseKeyspaceIDFromLabelRule parses the keyspace ID from the label rule.
// It will return the keyspace ID and a boolean indicating whether the label
// rule is a keyspace label rule.
func ParseKeyspaceIDFromLabelRule(rule *labeler.LabelRule) (uint32, bool) {
	// Validate the ID matches the expected format "keyspaces/<id>".
	if rule == nil {
		return 0, false
	}
	idText, ok := strings.CutPrefix(rule.ID, constant.RegionLabelIDPrefix)
	if !ok {
		return 0, false
	}
	// Retrieve the keyspace ID.
	keyspaceID, err := strconv.ParseUint(
		idText,
		endpoint.SpaceIDBase, 32,
	)
	hasLeadingZero := len(idText) > 1 && idText[0] == '0'
	if err != nil || keyspaceID > uint64(constant.MaxValidKeyspaceID) || hasLeadingZero {
		return 0, false
	}
	// Double check the keyspace ID from the label rule.
	var (
		idFromLabel  uint64
		foundIDLabel bool
	)
	for _, label := range rule.Labels {
		if label.Key == constant.RegionLabelKey {
			foundIDLabel = true
			idFromLabel, err = strconv.ParseUint(label.Value, endpoint.SpaceIDBase, 32)
			if err != nil {
				return 0, false
			}
			break
		}
	}
	if !foundIDLabel || keyspaceID != idFromLabel {
		return 0, false
	}
	return uint32(keyspaceID), true
}

// indexedHeap is a heap with index.
type indexedHeap struct {
	items []*endpoint.KeyspaceGroup
	// keyspace group id -> position in items
	index map[uint32]int
}

func newIndexedHeap(hint int) *indexedHeap {
	return &indexedHeap{
		items: make([]*endpoint.KeyspaceGroup, 0, hint),
		index: map[uint32]int{},
	}
}

// Implementing heap.Interface.
func (hp *indexedHeap) Len() int {
	return len(hp.items)
}

// Implementing heap.Interface.
func (hp *indexedHeap) Less(i, j int) bool {
	// Gives the keyspace group with the least number of keyspaces first
	return len(hp.items[j].Keyspaces) > len(hp.items[i].Keyspaces)
}

// Swap swaps the items at the given indices.
// Implementing heap.Interface.
func (hp *indexedHeap) Swap(i, j int) {
	lid := hp.items[i].ID
	rid := hp.items[j].ID
	hp.items[i], hp.items[j] = hp.items[j], hp.items[i]
	hp.index[lid] = j
	hp.index[rid] = i
}

// Push adds an item to the heap.
// Implementing heap.Interface.
func (hp *indexedHeap) Push(x any) {
	item := x.(*endpoint.KeyspaceGroup)
	hp.index[item.ID] = hp.Len()
	hp.items = append(hp.items, item)
}

// Pop removes the top item and returns it.
// Implementing heap.Interface.
func (hp *indexedHeap) Pop() any {
	l := hp.Len()
	item := hp.items[l-1]
	hp.items[l-1] = nil // avoid memory leak
	hp.items = hp.items[:l-1]
	delete(hp.index, item.ID)
	return item
}

// Top returns the top item.
func (hp *indexedHeap) Top() *endpoint.KeyspaceGroup {
	if hp.Len() <= 0 {
		return nil
	}
	return hp.items[0]
}

// Get returns item with the given ID.
func (hp *indexedHeap) Get(id uint32) *endpoint.KeyspaceGroup {
	idx, ok := hp.index[id]
	if !ok {
		return nil
	}
	item := hp.items[idx]
	return item
}

// GetAll returns all the items.
func (hp *indexedHeap) GetAll() []*endpoint.KeyspaceGroup {
	all := make([]*endpoint.KeyspaceGroup, len(hp.items))
	copy(all, hp.items)
	return all
}

// Put inserts item or updates the old item if it exists.
func (hp *indexedHeap) Put(item *endpoint.KeyspaceGroup) (isUpdate bool) {
	if idx, ok := hp.index[item.ID]; ok {
		hp.items[idx] = item
		heap.Fix(hp, idx)
		return true
	}
	heap.Push(hp, item)
	return false
}

// Remove deletes item by ID and returns it.
func (hp *indexedHeap) Remove(id uint32) *endpoint.KeyspaceGroup {
	if idx, ok := hp.index[id]; ok {
		item := heap.Remove(hp, idx)
		return item.(*endpoint.KeyspaceGroup)
	}
	return nil
}

// GetBootstrapKeyspaceID returns the Keyspace ID used for bootstrapping.
// Classic: constant.DefaultKeyspaceID
// NextGen: constant.SystemKeyspaceID
func GetBootstrapKeyspaceID() uint32 {
	if kerneltype.IsNextGen() {
		return constant.SystemKeyspaceID
	}
	return constant.DefaultKeyspaceID
}

// GetBootstrapKeyspaceName returns the Keyspace Name used for bootstrapping.
// Classic: constant.DefaultKeyspaceName
// NextGen: constant.SystemKeyspaceName
func GetBootstrapKeyspaceName() string {
	if kerneltype.IsNextGen() {
		return constant.SystemKeyspaceName
	}
	return constant.DefaultKeyspaceName
}

func newModifyProtectedKeyspaceError() error {
	if kerneltype.IsNextGen() {
		return errs.ErrModifyReservedKeyspace
	}
	return errs.ErrModifyDefaultKeyspace
}

func isProtectedKeyspaceID(id uint32) bool {
	if kerneltype.IsNextGen() {
		return id == constant.SystemKeyspaceID
	}
	return id == constant.DefaultKeyspaceID
}

func isProtectedKeyspaceName(name string) bool {
	if kerneltype.IsNextGen() {
		return name == constant.SystemKeyspaceName
	}
	return name == constant.DefaultKeyspaceName
}

// regionBoundType represents a keyspace region boundary's mode, raw or txn.
type regionBoundType int

const (
	// rawRegionBound represents the raw keyspace, which is used for KV operations without transaction.
	rawRegionBound regionBoundType = iota
	// txnRegionBound represents the txn keyspace, which is used for KV operations with transaction.
	txnRegionBound
)

// String returns the string representation of the regionBoundType.
func (t regionBoundType) String() string {
	if t == rawRegionBound {
		return "raw"
	}
	return "txn"
}

// bounds returns the left and right boundary of the given RegionBound for this key type.
func (t regionBoundType) bounds(b *RegionBound) (lo, hi []byte) {
	if t == rawRegionBound {
		return b.RawLeftBound, b.RawRightBound
	}
	return b.TxnLeftBound, b.TxnRightBound
}

// keyTypeToRegionBoundType converts the cluster's key type to the corresponding
// keyspace key type (raw or txn).
// ref rfc: https://github.com/tikv/rfcs/blob/master/text/0069-api-v2.md
func keyTypeToRegionBoundType(keyType coreconstant.KeyType) regionBoundType {
	if keyType == coreconstant.Raw {
		return rawRegionBound
	}
	return txnRegionBound
}

func keyTypeStringToRegionBoundType(keyType string) regionBoundType {
	if keyType == coreconstant.Raw.String() {
		return rawRegionBound
	}
	return txnRegionBound
}

// ExtractKeyspaceID extracts the keyspace ID and region bound type (raw or
// txn) from a region key. ok is false when key is not a memcomparable-encoded
// keyspace key, in which case id and bound are left at their zero value.
// The key format is: [mode_prefix][keyspace_id_3bytes][...], where mode_prefix
// is 'x' for txn and 'r' for raw. An empty key belongs to the max txn keyspace.
func ExtractKeyspaceID(key []byte) (id uint32, bound regionBoundType, ok bool) {
	// Empty key represents the start of the entire key space (no keyspace).
	if len(key) == 0 {
		return constant.MaxValidKeyspaceID, txnRegionBound, true
	}

	_, decoded, err := codec.DecodeBytes(key)
	if err != nil {
		return 0, 0, false
	}
	mode, id, parsed := codec.ParseKeyspacePrefix(decoded)
	if !parsed {
		return 0, 0, false
	}
	switch mode {
	case codec.RawKeyspaceModePrefix:
		return id, rawRegionBound, true
	case codec.TxnKeyspaceModePrefix:
		return id, txnRegionBound, true
	default:
		return 0, 0, false
	}
}

// Checker is an interface to check keyspace existence.
type Checker interface {
	// GetKeyspaceIDInRange returns the keyspace IDs in the range [start, end].
	// It returns the keyspace IDs by desc and a boolean indicating whether there is any keyspace in the range.
	GetKeyspaceIDInRange(start, end uint32, limit int) ([]uint32, bool)
	// KeyspaceExist returns whether the keyspace ID exists.
	KeyspaceExist(keyspaceID uint32) bool
}

// RegionSpansMultipleKeyspaces checks whether the region [startKey, endKey)
// (endKey exclusive) crosses a keyspace boundary. It returns false when nil
// checker is passed.
//
// The decision, in order:
//   - both keys carry no keyspace prefix (or endKey is absent): not spanning;
//   - exactly one key carries no keyspace prefix: conservatively spanning, since
//     the boundary cannot be determined;
//   - same keyspace ID and same mode (raw/txn): not spanning;
//   - raw startKey with txn endKey: spanning (crosses the raw/txn boundary);
//   - endKey absent (region runs past every later keyspace): spanning iff the
//     start keyspace still exists;
//   - endKey sits exactly on startKey's keyspace right bound: not spanning;
//   - otherwise: spanning iff the start or the end keyspace still exists.
//
// The last rule only looks at the two end keyspaces; keyspaces that lie strictly
// between them are not inspected. An absent endKey is treated as +inf rather than
// keyspace MaxValidKeyspaceID, so the result does not depend on the checker
// implementation's handling of that sentinel ID.
func RegionSpansMultipleKeyspaces(startKey, endKey []byte, checker Checker) bool {
	if checker == nil {
		return false
	}
	var startKeyspaceID uint32
	var startKT regionBoundType
	var startOK bool
	if len(startKey) == 0 {
		startKeyspaceID, startKT, startOK = constant.StartKeyspaceID, rawRegionBound, true
	} else {
		startKeyspaceID, startKT, startOK = ExtractKeyspaceID(startKey)
	}

	endKeyspaceID, endKT, endOK := ExtractKeyspaceID(endKey)

	// If both keys have no recognizable keyspace ID (or the end key is simply
	// absent), the region carries no keyspace boundary information at all, so it
	// does not span multiple keyspaces.
	if !startOK && (!endOK || len(endKey) == 0) {
		return false
	}

	// If exactly one side has an unknown key type, conservatively consider it spans multiple keyspaces to avoid potential data corruption.
	// This can happen when the key is not in the expected format, or when there is a hole in keyspace allocation.
	// For example, if startKey has valid keyspace ID but endKey is invalid, we cannot determine the keyspace boundary,
	// thus we consider it spans multiple keyspaces to be safe.
	if !startOK || !endOK {
		return true
	}

	// If the keyspace ids are same and key types are same, it does not span multiple keyspaces even if the key is invalid.
	if startKeyspaceID == endKeyspaceID && startKT == endKT {
		return false
	}
	// If startKey is raw key and endKey is txn key, it must span multiple keyspaces, because raw key usually the rightmost key and the txn the smallest key.
	// So it must cross the boundary between raw keyspace and txn keyspace, which means it spans multiple keyspaces.
	// such as this ['r200','x100'], it may cross keyspace (200, MaxValidKeyspaceID]
	if startKT == rawRegionBound && endKT == txnRegionBound {
		return true
	}

	// An absent endKey means the region runs past every later keyspace (+inf).
	// There is no end keyspace to check, so it spans a boundary iff it starts
	// inside an existing keyspace. Reaching here, startKey is a txn keyspace key.
	if len(endKey) == 0 {
		return checker.KeyspaceExist(startKeyspaceID)
	}

	// If end keyspace ID is exactly start keyspace + 1,
	// check if endKey is at the exact boundary (right bound of startKeyspace)
	// If yes, the region is [startKey, rightBound of startKeyspace) which is within one keyspace.
	if endKeyspaceID == startKeyspaceID+1 {
		startBound := MakeRegionBound(startKeyspaceID)
		// it means the region is [startKey, rightBound of startKeyspace)
		// which is still within one keyspace
		if string(endKey) == string(startBound.TxnRightBound) || string(endKey) == string(startBound.RawRightBound) {
			return false
		}
	}
	// Check the keyspace existence of startKeyspaceID and endKeyspaceID.
	//  If both of them do not exist, we consider it does not span multiple keyspaces.
	startExist := checker.KeyspaceExist(startKeyspaceID)
	endExist := checker.KeyspaceExist(endKeyspaceID)
	return startExist || endExist
}

const scanLimit = 10

// resolveKeyspaceID reports the keyspace ID that key should be treated as
// when restricting a keyspace ID range search to boundType's key space. If
// key encodes a keyspace ID of exactly boundType, that ID is returned.
// Otherwise key is classified by which side of boundType's key block it
// falls on: constant.StartKeyspaceID if key sorts before the entire block
// (e.g. a raw key when boundType is txn), or constant.MaxValidKeyspaceID if
// it sorts at or after the block (e.g. a txn key when boundType is raw, or a
// classic non-keyspace key that happens to sort past it). This keeps the
// resulting [startID, endID] window correct even when a query mixes a real
// boundType key with a key from another mode or a foreign, non-keyspace
// encoding, instead of falling back to a role-based default (0 for a start
// key, MaxValidKeyspaceID for an end key) regardless of where key actually
// sorts.
//
// ok is false only when key cannot be classified at all, i.e. it is not
// memcomparable-encoded; the caller should keep its own conservative default
// in that case.
func resolveKeyspaceID(key []byte, boundType regionBoundType) (id uint32, ok bool) {
	if ksID, kt, extractOK := ExtractKeyspaceID(key); extractOK {
		if kt == boundType {
			return ksID, true
		}
		// key is a real keyspace key of the other mode. The raw ('r') key
		// block always sorts before the txn ('x') key block.
		if boundType == txnRegionBound {
			return constant.StartKeyspaceID, true
		}
		return constant.MaxValidKeyspaceID, true
	}
	_, decoded, err := codec.DecodeBytes(key)
	if err != nil || len(decoded) == 0 {
		return 0, false
	}
	targetPrefix := codec.TxnKeyspaceModePrefix
	if boundType == rawRegionBound {
		targetPrefix = codec.RawKeyspaceModePrefix
	}
	if decoded[0] < targetPrefix {
		return constant.StartKeyspaceID, true
	}
	return constant.MaxValidKeyspaceID, true
}

// GetKeyspaceSplitKeys returns the keys at which the region [startKey, endKey)
// must be split so that no region spans more than one keyspace. keyType is the
// cluster-wide keyspace API mode (raw or txn); only that mode's keyspace
// boundaries are considered. It returns nil when no split is needed.
func GetKeyspaceSplitKeys(startKey, endKey []byte, keyType coreconstant.KeyType, checker Checker) [][]byte {
	if checker == nil {
		return nil
	}
	boundType := keyTypeToRegionBoundType(keyType)

	// A start key that is empty or not a keyspace key of this mode means the
	// region begins before any keyspace; an absent or foreign end key means it
	// runs to the end of this mode's keyspace space. See resolveKeyspaceID for
	// how a key of the other mode, or a non-keyspace key, is classified.
	startID := constant.StartKeyspaceID
	if len(startKey) != 0 {
		if id, ok := resolveKeyspaceID(startKey, boundType); ok {
			startID = id
		}
	}
	endID := constant.MaxValidKeyspaceID
	if len(endKey) != 0 {
		if id, ok := resolveKeyspaceID(endKey, boundType); ok {
			endID = id
		}
	}
	if startID >= endID {
		return nil
	}

	// No keyspace ID can lie strictly between startID and endID, so the only
	// possible split point is the boundary they share. Resolve it with two
	// point lookups instead of a range scan, which would otherwise trigger a
	// keyspace ID range backfill for a window that can hold no keyspace of
	// its own.
	if endID == startID+1 {
		// endID may be the MaxValidKeyspaceID sentinel used above to mean "runs
		// to the end of this mode's keyspace space" (an absent or foreign end
		// key), not a real keyspace ID any Checker implementation was ever
		// asked to track. Treat it as always present instead of asking the
		// checker, so the result doesn't depend on how a given implementation
		// happens to answer for that ID - the same reason
		// RegionSpansMultipleKeyspaces treats an absent endKey as +inf rather
		// than routing it through KeyspaceExist(MaxValidKeyspaceID).
		endExist := endID == constant.MaxValidKeyspaceID || checker.KeyspaceExist(endID)
		if !checker.KeyspaceExist(startID) && !endExist {
			return nil
		}
		_, boundary := boundType.bounds(MakeRegionBound(startID))
		if keyutil.Between(startKey, endKey, boundary) {
			return [][]byte{boundary}
		}
		return nil
	}

	ids, ok := checker.GetKeyspaceIDInRange(startID, endID, scanLimit)
	if !ok || len(ids) == 0 {
		return nil
	}
	var splitKeys [][]byte
	for _, id := range ids {
		lo, hi := boundType.bounds(MakeRegionBound(id))
		if keyutil.Between(startKey, endKey, lo) {
			splitKeys = append(splitKeys, lo)
		}
		if keyutil.Between(startKey, endKey, hi) {
			splitKeys = append(splitKeys, hi)
		}
	}
	if len(splitKeys) == 0 {
		return nil
	}
	slices.SortFunc(splitKeys, bytes.Compare)
	return slices.CompactFunc(splitKeys, bytes.Equal)
}

type keyspaceItem struct {
	keyspaceID uint32
	name       string
	state      keyspacepb.KeyspaceState
}

// Less compares two keyspaceItem.
func (s *keyspaceItem) Less(than keyspaceItem) bool {
	return s.keyspaceID < than.keyspaceID
}

// Cache is a cache for keyspace information, which is used to quickly determine keyspace existence and get keyspace name by ID.
type Cache struct {
	syncutil.RWMutex
	tree *btree.BTreeG[keyspaceItem]
}

// NewCache creates a new Cache.
func NewCache() *Cache {
	return &Cache{
		tree: btree.NewG(2, func(i, j keyspaceItem) bool {
			return i.Less(j)
		}),
	}
}

func (s *Cache) getKeyspaceByID(keyspaceID uint32) (keyspaceItem, bool) {
	s.RLock()
	defer s.RUnlock()
	item, found := s.tree.Get(keyspaceItem{keyspaceID: keyspaceID})
	return item, found
}

// Save saves the keyspace information to the cache. It will replace the old information if the keyspace ID already exists.
func (s *Cache) Save(keyspaceID uint32, name string, state keyspacepb.KeyspaceState) {
	s.Lock()
	defer s.Unlock()
	item := keyspaceItem{keyspaceID: keyspaceID, name: name, state: state}
	s.tree.ReplaceOrInsert(item)
}

// DeleteKeyspace deletes a keyspace by ID.
func (s *Cache) DeleteKeyspace(keyspaceID uint32) {
	s.Lock()
	defer s.Unlock()
	s.tree.Delete(keyspaceItem{keyspaceID: keyspaceID})
}

// clearAll empties the cache in place. It must be used instead of replacing
// the *Cache pointer held by a caller, since the pointer itself may be read
// concurrently without synchronization.
func (s *Cache) clearAll() {
	s.Lock()
	defer s.Unlock()
	s.tree = btree.NewG(2, func(i, j keyspaceItem) bool {
		return i.Less(j)
	})
}

func (s *Cache) scanAllKeyspaces(f func(keyspaceID uint32, name string, state keyspacepb.KeyspaceState) bool) {
	s.RLock()
	defer s.RUnlock()
	s.tree.Ascend(func(i keyspaceItem) bool {
		return f(i.keyspaceID, i.name, i.state)
	})
}

// KeyspaceExist checks if a keyspace exists by ID.
func (s *Cache) KeyspaceExist(id uint32) bool {
	s.RLock()
	defer s.RUnlock()
	item, found := s.tree.Get(keyspaceItem{keyspaceID: id})
	if found && item.state == keyspacepb.KeyspaceState_TOMBSTONE {
		return false
	}
	return found
}

// GetKeyspaceIDInRange returns the keyspace IDs in the range [start, end].
func (s *Cache) GetKeyspaceIDInRange(start, end uint32, limit int) ([]uint32, bool) {
	s.RLock()
	defer s.RUnlock()
	ret := make([]uint32, 0, max(limit, 0))
	found := false
	s.tree.DescendLessOrEqual(keyspaceItem{keyspaceID: end}, func(item keyspaceItem) bool {
		// The tree is scanned in descending order, so once an item falls below
		// start, every later item does too; nothing further can match.
		if item.keyspaceID < start {
			return false
		}
		if item.state == keyspacepb.KeyspaceState_TOMBSTONE {
			return true
		}
		ret = append(ret, item.keyspaceID)
		found = true
		return limit <= 0 || len(ret) < limit
	})
	return ret, found
}

// NewKeyspaceMeta creates a KeyspaceMeta from the given marshaled protobuf data.
func NewKeyspaceMeta(data string) (*keyspacepb.KeyspaceMeta, error) {
	meta := &keyspacepb.KeyspaceMeta{}
	if err := proto.Unmarshal([]byte(data), meta); err != nil {
		return nil, errs.ErrProtoUnmarshal.Wrap(err).GenWithStackByCause()
	}
	return meta, nil
}

// IgnoreMetaServiceGroup removes the meta-service-group fields from the config.
// Exported for tests.
func IgnoreMetaServiceGroup(m map[string]string) map[string]string {
	c := make(map[string]string, len(m))
	for k, v := range m {
		if k != MetaServiceGroupIDKey && k != MetaServiceGroupAddressesKey {
			c[k] = v
		}
	}
	return c
}
