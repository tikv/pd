// Copyright 2026 TiKV Project Authors.
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

package regionmeta

import (
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"
)

const (
	maxResponseBytes = 8 * 1024 * 1024
	sortBufferBytes  = 8 * 1024 * 1024
	mergeFanIn       = 8
)

// Status is the conclusion of a completed region meta check.
type Status string

const (
	// StatusConsistent means no difference remains after confirmation.
	StatusConsistent Status = "consistent"
	// StatusInconsistent means at least one stable difference was confirmed.
	StatusInconsistent Status = "inconsistent"
	// StatusIncomplete means the observations are insufficient for a conclusion.
	StatusIncomplete Status = "incomplete"
)

// Outcome contains the process-relevant result of a completed check.
type Outcome struct {
	Status Status
}

// Config controls one region meta check.
type Config struct {
	Endpoints             []string
	TLSConfig             *tls.Config
	AuthorizationFile     string
	BatchSize             int
	Interval              time.Duration
	Timeout               time.Duration
	MaxRuntime            time.Duration
	Retries               int
	ScanRetries           int
	ConfirmLimit          int
	ConfirmationDelay     time.Duration
	WorkDir               string
	MaxTemporaryDiskBytes int64
	MaxOutputBytes        int64
	Output                string
}

// DefaultConfig returns conservative production defaults.
func DefaultConfig() Config {
	return Config{
		BatchSize:             128,
		Interval:              50 * time.Millisecond,
		Timeout:               10 * time.Second,
		MaxRuntime:            4 * time.Hour,
		ConfirmLimit:          128,
		ConfirmationDelay:     time.Second,
		MaxTemporaryDiskBytes: 1024 * 1024 * 1024,
		MaxOutputBytes:        1024 * 1024 * 1024,
		Output:                "-",
	}
}

func (c Config) validate() error {
	if len(c.Endpoints) == 0 {
		return errors.New("at least one PD endpoint is required")
	}
	if c.BatchSize < 1 || c.BatchSize > 1024 {
		return errors.New("batch size must be in [1, 1024]")
	}
	if c.Interval < 0 || c.Timeout <= 0 || c.MaxRuntime <= 0 || c.ConfirmationDelay < 0 {
		return errors.New("interval must be non-negative and timeout and max runtime must be positive")
	}
	if c.Retries < 0 || c.Retries > 10 || c.ScanRetries < 0 || c.ScanRetries > 3 {
		return errors.New("retries must be in [0, 10] and scan retries must be in [0, 3]")
	}
	if c.ConfirmLimit < 0 || c.ConfirmLimit > 1024 {
		return errors.New("confirmation limit must be in [0, 1024]")
	}
	if c.MaxTemporaryDiskBytes <= 0 || c.MaxOutputBytes <= 0 {
		return errors.New("temporary disk and output limits must be positive")
	}
	if c.Output == "" {
		return errors.New("output must be '-' or a file path")
	}
	return nil
}

type node struct {
	MemberID uint64
	Name     string
	URL      string
	Role     string
}

func (n node) instance() string {
	u, _ := url.Parse(n.URL)
	return n.Name + "@" + u.Host
}

type peer struct {
	ID        uint64 `json:"id"`
	StoreID   uint64 `json:"store_id"`
	Role      uint32 `json:"role"`
	IsWitness bool   `json:"is_witness"`
}

type regionMeta struct {
	StartKey string `json:"s"`
	EndKey   string `json:"e"`
	ConfVer  uint64 `json:"c"`
	Version  uint64 `json:"v"`
	Peers    []peer `json:"p"`
	Leader   *peer  `json:"l"`
}

func (m regionMeta) equal(other regionMeta) bool {
	return m.StartKey == other.StartKey && m.EndKey == other.EndKey &&
		m.ConfVer == other.ConfVer && m.Version == other.Version &&
		slices.Equal(m.Peers, other.Peers) && sameLeader(m.Leader, other.Leader)
}

type regionRecord struct {
	ID   uint64
	Meta regionMeta
}

func (r regionRecord) equal(other regionRecord) bool {
	return r.ID == other.ID && r.Meta.equal(other.Meta)
}

type wirePeer struct {
	ID        uint64 `json:"id"`
	StoreID   uint64 `json:"store_id"`
	Role      uint32 `json:"role"`
	IsLearner bool   `json:"is_learner"`
	IsWitness bool   `json:"is_witness"`
}

type wireEpoch struct {
	ConfVer uint64 `json:"conf_ver"`
	Version uint64 `json:"version"`
}

type wireRegion struct {
	ID       uint64     `json:"id"`
	StartKey string     `json:"start_key"`
	EndKey   string     `json:"end_key"`
	Epoch    wireEpoch  `json:"epoch"`
	Peers    []wirePeer `json:"peers"`
	Leader   *wirePeer  `json:"leader"`
}

type wireRegions struct {
	Count   *int         `json:"count"`
	Regions []wireRegion `json:"regions"`
}

func normalizeRegion(raw wireRegion) (regionRecord, error) {
	if raw.ID == 0 {
		return regionRecord{}, errors.New("region response contains an invalid uint64 id")
	}
	startKey, err := normalizeHexKey(raw.StartKey, "start_key", raw.ID)
	if err != nil {
		return regionRecord{}, err
	}
	endKey, err := normalizeHexKey(raw.EndKey, "end_key", raw.ID)
	if err != nil {
		return regionRecord{}, err
	}
	peers := make([]peer, 0, len(raw.Peers))
	for _, value := range raw.Peers {
		peers = append(peers, normalizePeer(value))
	}
	slices.SortFunc(peers, comparePeer)
	var leader *peer
	if raw.Leader != nil {
		value := normalizePeer(*raw.Leader)
		if value.ID != 0 || value.StoreID != 0 {
			leader = &value
		}
	}
	return regionRecord{ID: raw.ID, Meta: regionMeta{
		StartKey: startKey, EndKey: endKey, ConfVer: raw.Epoch.ConfVer,
		Version: raw.Epoch.Version, Peers: peers, Leader: leader,
	}}, nil
}

func normalizePeer(raw wirePeer) peer {
	role := raw.Role
	if raw.IsLearner && role == 0 {
		role = 1
	}
	return peer{ID: raw.ID, StoreID: raw.StoreID, Role: role, IsWitness: raw.IsWitness}
}

func comparePeer(a, b peer) int {
	if a.ID != b.ID {
		return compareUint64(a.ID, b.ID)
	}
	if a.StoreID != b.StoreID {
		return compareUint64(a.StoreID, b.StoreID)
	}
	if a.Role != b.Role {
		return int(a.Role) - int(b.Role)
	}
	if a.IsWitness == b.IsWitness {
		return 0
	}
	if !a.IsWitness {
		return -1
	}
	return 1
}

func compareUint64(a, b uint64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func normalizeHexKey(value, field string, regionID uint64) (string, error) {
	value = strings.ToUpper(value)
	if len(value)%2 != 0 {
		return "", fmt.Errorf("region %d has invalid hexadecimal %s", regionID, field)
	}
	if _, err := hex.DecodeString(value); err != nil {
		return "", fmt.Errorf("region %d has invalid hexadecimal %s: %w", regionID, field, err)
	}
	return value, nil
}

type keyRange struct {
	StartKey string `json:"start_key"`
	EndKey   string `json:"end_key"`
}

type epoch struct {
	ConfVer uint64 `json:"conf_ver"`
	Version uint64 `json:"version"`
}

type difference struct {
	RegionID   uint64              `json:"region_id"`
	MissingOn  []string            `json:"missing_on,omitempty"`
	KeyRange   map[string]keyRange `json:"key_range,omitempty"`
	Epoch      map[string]epoch    `json:"epoch,omitempty"`
	Peers      map[string][]peer   `json:"peers,omitempty"`
	LeaderPeer map[string]*peer    `json:"leader_peer,omitempty"`
}

func (d difference) equal(other difference) bool {
	return d.RegionID == other.RegionID && slices.Equal(d.MissingOn, other.MissingOn) &&
		mapsEqual(d.KeyRange, other.KeyRange) && mapsEqual(d.Epoch, other.Epoch) &&
		peerMapsEqual(d.Peers, other.Peers) && leaderMapsEqual(d.LeaderPeer, other.LeaderPeer)
}

func mapsEqual[K comparable, V comparable](a, b map[K]V) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if other, ok := b[key]; !ok || value != other {
			return false
		}
	}
	return true
}

func peerMapsEqual(a, b map[string][]peer) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if other, ok := b[key]; !ok || !slices.Equal(value, other) {
			return false
		}
	}
	return true
}

func leaderMapsEqual(a, b map[string]*peer) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		other, ok := b[key]
		if !ok || (value == nil) != (other == nil) || (value != nil && *value != *other) {
			return false
		}
	}
	return true
}

func makeDifference(regionID uint64, rows []*regionMeta, nodes []node) *difference {
	missing := false
	var firstPresent *regionMeta
	for _, row := range rows {
		if row == nil {
			missing = true
		} else if firstPresent == nil {
			firstPresent = row
		}
	}
	if firstPresent == nil {
		return nil
	}
	d := &difference{RegionID: regionID}
	if missing {
		for i, row := range rows {
			if row == nil {
				d.MissingOn = append(d.MissingOn, nodes[i].instance())
			}
		}
	}
	keyRangeDiff, epochDiff, peersDiff, leaderDiff := false, false, false, false
	for _, row := range rows {
		if row == nil {
			continue
		}
		keyRangeDiff = keyRangeDiff || row.StartKey != firstPresent.StartKey || row.EndKey != firstPresent.EndKey
		epochDiff = epochDiff || row.ConfVer != firstPresent.ConfVer || row.Version != firstPresent.Version
		peersDiff = peersDiff || !slices.Equal(row.Peers, firstPresent.Peers)
		leaderDiff = leaderDiff || !sameLeader(row.Leader, firstPresent.Leader)
	}
	if keyRangeDiff {
		d.KeyRange = make(map[string]keyRange)
	}
	if epochDiff {
		d.Epoch = make(map[string]epoch)
	}
	if peersDiff {
		d.Peers = make(map[string][]peer)
	}
	if leaderDiff {
		d.LeaderPeer = make(map[string]*peer)
	}
	for i, row := range rows {
		if row == nil {
			continue
		}
		instance := nodes[i].instance()
		if keyRangeDiff {
			d.KeyRange[instance] = keyRange{StartKey: row.StartKey, EndKey: row.EndKey}
		}
		if epochDiff {
			d.Epoch[instance] = epoch{ConfVer: row.ConfVer, Version: row.Version}
		}
		if peersDiff {
			d.Peers[instance] = row.Peers
		}
		if leaderDiff {
			d.LeaderPeer[instance] = row.Leader
		}
	}
	if len(d.MissingOn) == 0 && !keyRangeDiff && !epochDiff && !peersDiff && !leaderDiff {
		return nil
	}
	return d
}

func sameLeader(a, b *peer) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}
