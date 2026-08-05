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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakePeer struct {
	ID        uint64 `json:"id"`
	StoreID   uint64 `json:"store_id"`
	Role      uint32 `json:"role,omitempty"`
	IsWitness bool   `json:"is_witness,omitempty"`
}

type fakeEpoch struct {
	ConfVer uint64 `json:"conf_ver"`
	Version uint64 `json:"version"`
}

type fakeRegion struct {
	ID           uint64     `json:"id"`
	StartKey     string     `json:"start_key"`
	EndKey       string     `json:"end_key"`
	Epoch        fakeEpoch  `json:"epoch"`
	Peers        []fakePeer `json:"peers"`
	Leader       *fakePeer  `json:"leader,omitempty"`
	WrittenBytes uint64     `json:"written_bytes,omitempty"`
	PendingPeers []fakePeer `json:"pending_peers,omitempty"`
	Buckets      []string   `json:"buckets,omitempty"`
}

func newFakeRegion(id uint64, startKey, endKey string) fakeRegion {
	peer := fakePeer{ID: id * 10, StoreID: 1}
	return fakeRegion{
		ID:       id,
		StartKey: startKey,
		EndKey:   endKey,
		Epoch:    fakeEpoch{ConfVer: 1, Version: 1},
		Peers:    []fakePeer{peer},
		Leader:   &peer,
	}
}

type fakePD struct {
	name       string
	memberID   uint64
	server     *httptest.Server
	mu         sync.Mutex
	regions    []fakeRegion
	membership any
	local      [][3]string
	scanCalls  int
	countCalls int
	regionGets int
	scanHook   func(int)
	countHook  func(int)
	getHook    func(int)
	firstPage  func()
}

func (p *fakePD) start() {
	p.server = httptest.NewServer(http.HandlerFunc(p.serveHTTP))
}

func (p *fakePD) close() {
	p.server.Close()
}

func (p *fakePD) serveHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/pd/api/v1/members":
		value := p.membership
		if function, ok := value.(membershipFunc); ok {
			value = function()
		}
		p.writeJSON(w, value)
	case r.URL.Path == "/pd/api/v1/regions/count":
		p.recordLocal(r)
		p.mu.Lock()
		p.countCalls++
		call := p.countCalls
		hook := p.countHook
		p.mu.Unlock()
		if hook != nil {
			hook(call)
		}
		p.mu.Lock()
		count := len(p.regions)
		p.mu.Unlock()
		p.writeJSON(w, map[string]any{"count": count})
	case r.URL.Path == "/pd/api/v1/regions/key":
		p.recordLocal(r)
		p.mu.Lock()
		p.scanCalls++
		call := p.scanCalls
		hook := p.scanHook
		firstPage := p.firstPage
		p.mu.Unlock()
		if call == 1 && firstPage != nil {
			firstPage()
		}
		if hook != nil {
			hook(call)
		}
		start := strings.ToUpper(r.URL.Query().Get("key"))
		limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
		if err != nil || limit < 1 {
			http.Error(w, "bad limit", http.StatusBadRequest)
			return
		}
		p.mu.Lock()
		regions := make([]fakeRegion, 0, limit)
		for _, region := range p.regions {
			if region.EndKey == "" || region.EndKey > start {
				regions = append(regions, region)
				if len(regions) == limit {
					break
				}
			}
		}
		p.mu.Unlock()
		p.writeJSON(w, map[string]any{"count": len(regions), "regions": regions})
	case strings.HasPrefix(r.URL.Path, "/pd/api/v1/region/id/"):
		p.recordLocal(r)
		id, err := strconv.ParseUint(strings.TrimPrefix(r.URL.Path, "/pd/api/v1/region/id/"), 10, 64)
		if err != nil {
			http.Error(w, "bad region id", http.StatusBadRequest)
			return
		}
		p.mu.Lock()
		p.regionGets++
		call := p.regionGets
		hook := p.getHook
		p.mu.Unlock()
		if hook != nil {
			hook(call)
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, region := range p.regions {
			if region.ID == id {
				p.writeJSON(w, region)
				return
			}
		}
		http.Error(w, "region not found", http.StatusNotFound)
	default:
		http.NotFound(w, r)
	}
}

func (p *fakePD) recordLocal(r *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.local = append(p.local, [3]string{
		r.Header.Get("PD-Allow-Follower-Handle"),
		r.Header.Get("X-Caller-ID"),
		r.Header.Get("PD-Redirector"),
	})
}

func (*fakePD) writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

type fakeCluster struct {
	nodes []*fakePD
}

func newFakeCluster(t *testing.T) *fakeCluster {
	t.Helper()
	base := []fakeRegion{
		newFakeRegion(1, "", "10"),
		newFakeRegion(2, "10", "20"),
		newFakeRegion(3, "20", "30"),
		newFakeRegion(4, "30", "40"),
		newFakeRegion(5, "40", ""),
	}
	nodes := []*fakePD{
		{name: "pd-leader", memberID: 1},
		{name: "pd-follower", memberID: 2},
		{name: "pd-follower-2", memberID: 3},
	}
	for _, node := range nodes {
		node.regions = cloneRegions(t, base)
		node.start()
	}
	members := make([]map[string]any, 0, len(nodes))
	for _, node := range nodes {
		members = append(members, map[string]any{
			"name": node.name, "member_id": node.memberID, "client_urls": []string{node.server.URL},
		})
	}
	membership := map[string]any{
		"header":  map[string]any{"cluster_id": uint64(123)},
		"members": members,
		"leader":  members[0],
	}
	for _, node := range nodes {
		node.membership = membership
	}
	cluster := &fakeCluster{nodes: nodes}
	t.Cleanup(cluster.close)
	return cluster
}

func cloneRegions(t *testing.T, regions []fakeRegion) []fakeRegion {
	t.Helper()
	payload, err := json.Marshal(regions)
	require.NoError(t, err)
	var cloned []fakeRegion
	require.NoError(t, json.Unmarshal(payload, &cloned))
	return cloned
}

func (c *fakeCluster) close() {
	for _, node := range c.nodes {
		node.close()
	}
}

func (c *fakeCluster) instance(index int) string {
	u, _ := url.Parse(c.nodes[index].server.URL)
	return c.nodes[index].name + "@" + u.Host
}

func runFakeCheck(t *testing.T, cluster *fakeCluster, mutate func(*Config)) (Outcome, map[string]any, string, error) {
	t.Helper()
	outcome, payload, stderr, err := runFakeCheckRaw(t, cluster, mutate)
	if err != nil {
		return outcome, nil, stderr, err
	}
	var report map[string]any
	require.NoError(t, json.Unmarshal(payload, &report))
	return outcome, report, stderr, nil
}

func runFakeCheckRaw(t *testing.T, cluster *fakeCluster, mutate func(*Config)) (Outcome, []byte, string, error) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Endpoints = []string{cluster.nodes[0].server.URL}
	cfg.BatchSize = 2
	cfg.Interval = 0
	cfg.Timeout = 2 * time.Second
	cfg.ConfirmationDelay = 0
	cfg.WorkDir = t.TempDir()
	cfg.Output = "-"
	if mutate != nil {
		mutate(&cfg)
	}
	var stdout, stderr bytes.Buffer
	outcome, err := Run(context.Background(), cfg, &stdout, &stderr)
	return outcome, stdout.Bytes(), stderr.String(), err
}

func TestConsistentClusterUsesConcurrentLocalBatches(t *testing.T) {
	cluster := newFakeCluster(t)
	var arrived atomic.Int32
	release := make(chan struct{})
	for _, node := range cluster.nodes {
		node.firstPage = func() {
			if arrived.Add(1) == int32(len(cluster.nodes)) {
				close(release)
			}
			select {
			case <-release:
			case <-time.After(2 * time.Second):
				t.Error("first Region page was not fetched concurrently")
			}
		}
	}

	outcome, report, _, err := runFakeCheck(t, cluster, nil)
	require.NoError(t, err)
	require.Equal(t, StatusConsistent, outcome.Status)
	require.Equal(t, float64(0), report["summary"].(map[string]any)["different_regions"])
	require.Empty(t, report["differences"])
	settings := report["settings"].(map[string]any)
	require.Equal(t, float64(17), settings["http_requests"])
	require.Equal(t, float64(3), settings["global_concurrency"])
	require.Equal(t, float64(1), settings["per_node_concurrency"])
	require.Equal(t, float64(0), settings["temporary_disk_peak_bytes"])

	for _, node := range cluster.nodes {
		node.mu.Lock()
		require.NotEmpty(t, node.local)
		for _, headers := range node.local {
			require.Equal(t, [3]string{"true", "pd-ctl", "pd-ctl-region-meta-consistency"}, headers)
		}
		node.mu.Unlock()
	}
}

func TestReportsEveryRegionMetaDifference(t *testing.T) {
	cluster := newFakeCluster(t)
	follower := cluster.nodes[1]
	follower.regions[0].Epoch.Version = 2
	follower.regions[1].StartKey = "11"
	follower.regions[2].Leader = &fakePeer{ID: 31, StoreID: 2}
	follower.regions[3].Peers = append(follower.regions[3].Peers, fakePeer{ID: 41, StoreID: 2})
	follower.regions = follower.regions[:4]

	outcome, report, _, err := runFakeCheck(t, cluster, nil)
	require.NoError(t, err)
	require.Equal(t, StatusInconsistent, outcome.Status)
	summary := report["summary"].(map[string]any)
	require.Equal(t, float64(5), summary["different_regions"])
	require.Equal(t, map[string]any{
		"missing_on": float64(1), "key_range": float64(1), "epoch": float64(1),
		"peers": float64(1), "leader_peer": float64(1),
	}, summary["by_field"])

	differences := make(map[uint64]map[string]any)
	for _, value := range report["differences"].([]any) {
		difference := value.(map[string]any)
		differences[uint64(difference["region_id"].(float64))] = difference
	}
	require.Equal(t, float64(2), differences[1]["epoch"].(map[string]any)[cluster.instance(1)].(map[string]any)["version"])
	require.Equal(t, "11", differences[2]["key_range"].(map[string]any)[cluster.instance(1)].(map[string]any)["start_key"])
	require.Equal(t, float64(31), differences[3]["leader_peer"].(map[string]any)[cluster.instance(1)].(map[string]any)["id"])
	require.Equal(t, float64(41), differences[4]["peers"].(map[string]any)[cluster.instance(1)].([]any)[1].(map[string]any)["id"])
	require.Equal(t, []any{cluster.instance(1)}, differences[5]["missing_on"])
}

func TestIgnoresPeerOrderAndHeartbeatStatistics(t *testing.T) {
	cluster := newFakeCluster(t)
	for _, node := range cluster.nodes[1:] {
		node.regions[2].Peers = []fakePeer{{ID: 31, StoreID: 2}, {ID: 30, StoreID: 1}}
		node.regions[2].Leader = &fakePeer{ID: 30, StoreID: 1}
		node.regions[0].WrittenBytes = 999
		node.regions[0].PendingPeers = []fakePeer{{ID: 999, StoreID: 9}}
		node.regions[0].Buckets = []string{"10"}
	}
	cluster.nodes[0].regions[2].Peers = []fakePeer{{ID: 30, StoreID: 1}, {ID: 31, StoreID: 2}}
	cluster.nodes[0].regions[2].Leader = &fakePeer{ID: 30, StoreID: 1}

	outcome, report, _, err := runFakeCheck(t, cluster, nil)
	require.NoError(t, err)
	require.Equal(t, StatusConsistent, outcome.Status)
	require.Empty(t, report["differences"])
}

func TestReportsPeerRoleWitnessAndUint64Values(t *testing.T) {
	cluster := newFakeCluster(t)
	maxID := ^uint64(0)
	for _, node := range cluster.nodes {
		node.regions[0].ID = maxID
		node.regions[0].Epoch = fakeEpoch{ConfVer: maxID, Version: maxID}
		node.regions[0].Peers = []fakePeer{{ID: maxID, StoreID: maxID}}
		node.regions[0].Leader = &fakePeer{ID: maxID, StoreID: maxID}
	}
	cluster.nodes[1].regions[0].Epoch.Version--
	cluster.nodes[1].regions[0].Peers[0].StoreID--
	cluster.nodes[1].regions[2].Peers[0].Role = 1
	cluster.nodes[2].regions[2].Peers[0].IsWitness = true

	outcome, payload, _, err := runFakeCheckRaw(t, cluster, nil)
	require.NoError(t, err)
	require.Equal(t, StatusInconsistent, outcome.Status)
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var report map[string]any
	require.NoError(t, decoder.Decode(&report))
	differences := report["differences"].([]any)
	require.Len(t, differences, 2)
	large := differences[1].(map[string]any)
	require.Equal(t, strconv.FormatUint(maxID, 10), large["region_id"].(json.Number).String())
	require.Equal(t, strconv.FormatUint(maxID, 10),
		large["epoch"].(map[string]any)[cluster.instance(0)].(map[string]any)["version"].(json.Number).String())
	require.Equal(t, strconv.FormatUint(maxID, 10),
		large["peers"].(map[string]any)[cluster.instance(0)].([]any)[0].(map[string]any)["id"].(json.Number).String())
	peers := differences[0].(map[string]any)["peers"].(map[string]any)
	require.Equal(t, "1", peers[cluster.instance(1)].([]any)[0].(map[string]any)["role"].(json.Number).String())
	require.Equal(t, true, peers[cluster.instance(2)].([]any)[0].(map[string]any)["is_witness"])
}

func TestRechecksTransientDifferences(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.nodes[1].regions[0].Epoch.Version = 2
	for _, node := range cluster.nodes {
		node.getHook = func(call int) {
			if call == 1 {
				cluster.nodes[1].mu.Lock()
				cluster.nodes[1].regions[0].Epoch.Version = 1
				cluster.nodes[1].mu.Unlock()
			}
		}
	}

	outcome, report, _, err := runFakeCheck(t, cluster, nil)
	require.NoError(t, err)
	require.Equal(t, StatusConsistent, outcome.Status)
	require.Empty(t, report["differences"])
	confirmation := report["confirmation"].(map[string]any)
	require.Equal(t, "resolved", confirmation["result"])
}

func TestIncompleteWhenConfirmationIsDisabled(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.nodes[1].regions[0].Epoch.Version = 2

	outcome, report, _, err := runFakeCheck(t, cluster, func(cfg *Config) { cfg.ConfirmLimit = 0 })
	require.NoError(t, err)
	require.Equal(t, StatusIncomplete, outcome.Status)
	require.Equal(t, "confirmation_disabled", report["confirmation"].(map[string]any)["result"])
	require.Len(t, report["differences"], 1)
}

func TestRejectsUnstableRegionCountAndMembershipChange(t *testing.T) {
	t.Run("count", func(t *testing.T) {
		cluster := newFakeCluster(t)
		cluster.nodes[1].countHook = func(call int) {
			if call == 2 {
				cluster.nodes[1].mu.Lock()
				cluster.nodes[1].regions = cluster.nodes[1].regions[:4]
				cluster.nodes[1].mu.Unlock()
			}
		}
		_, _, _, err := runFakeCheck(t, cluster, nil)
		require.ErrorContains(t, err, "unstable region set")
	})

	t.Run("membership", func(t *testing.T) {
		cluster := newFakeCluster(t)
		initial := cluster.nodes[0].membership
		var memberCalls atomic.Int32
		cluster.nodes[0].membership = membershipFunc(func() any {
			if memberCalls.Add(1) == 1 {
				return initial
			}
			changed := cloneJSON[map[string]any](t, initial)
			changed["leader"] = changed["members"].([]any)[1]
			return changed
		})
		_, _, _, err := runFakeCheck(t, cluster, nil)
		require.ErrorContains(t, err, "membership or PD leader changed")
	})
}

func TestScanRetryHandlesGrowthBeyondOneBatch(t *testing.T) {
	cluster := newFakeCluster(t)
	follower := cluster.nodes[1]
	original := cloneRegions(t, follower.regions)
	grown := cloneRegions(t, original[:4])
	grown = append(grown,
		newFakeRegion(5, "40", "50"),
		newFakeRegion(6, "50", "60"),
		newFakeRegion(7, "60", "70"),
		newFakeRegion(8, "70", ""),
	)
	follower.firstPage = func() {
		follower.mu.Lock()
		follower.regions = grown
		follower.mu.Unlock()
	}
	follower.countHook = func(call int) {
		if call == 3 {
			follower.mu.Lock()
			follower.regions = original
			follower.mu.Unlock()
		}
	}

	outcome, report, stderr, err := runFakeCheck(t, cluster, func(cfg *Config) { cfg.ScanRetries = 1 })
	require.NoError(t, err)
	require.Equal(t, StatusConsistent, outcome.Status)
	require.Empty(t, report["differences"])
	require.Contains(t, stderr, "region count changed during scan; retrying")
}

type membershipFunc func() any

func cloneJSON[T any](t *testing.T, value any) T {
	t.Helper()
	payload, err := json.Marshal(value)
	require.NoError(t, err)
	var cloned T
	require.NoError(t, json.Unmarshal(payload, &cloned))
	return cloned
}

func TestValidationAndOutputLimit(t *testing.T) {
	cluster := newFakeCluster(t)
	_, _, _, err := runFakeCheck(t, cluster, func(cfg *Config) { cfg.BatchSize = 0 })
	require.ErrorContains(t, err, "batch size")

	cluster.nodes[1].regions[0].Epoch.Version = 2
	_, _, _, err = runFakeCheck(t, cluster, func(cfg *Config) { cfg.MaxOutputBytes = 16 })
	require.ErrorContains(t, err, "report JSON exceeds")
}

func TestDifferenceOrdering(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.nodes[1].regions[4].Epoch.Version = 2
	cluster.nodes[1].regions[0].Epoch.Version = 2
	outcome, report, _, err := runFakeCheck(t, cluster, nil)
	require.NoError(t, err)
	require.Equal(t, StatusInconsistent, outcome.Status)
	values := report["differences"].([]any)
	ids := make([]uint64, 0, len(values))
	for _, value := range values {
		ids = append(ids, uint64(value.(map[string]any)["region_id"].(float64)))
	}
	require.True(t, slices.IsSorted(ids), "Region IDs are not sorted: %v", ids)
}

func TestMatchesRegionIDsAcrossDivergentKeySegments(t *testing.T) {
	cluster := newFakeCluster(t)
	moved := cluster.nodes[1].regions[0]
	moved.StartKey = "10"
	moved.EndKey = "20"
	replacement := newFakeRegion(9, "", "10")
	cluster.nodes[1].regions = append([]fakeRegion{replacement, moved}, cluster.nodes[1].regions[2:]...)

	outcome, report, _, err := runFakeCheck(t, cluster, nil)
	require.NoError(t, err)
	require.Equal(t, StatusInconsistent, outcome.Status)
	differences := make(map[uint64]map[string]any)
	for _, value := range report["differences"].([]any) {
		difference := value.(map[string]any)
		differences[uint64(difference["region_id"].(float64))] = difference
	}
	ids := make([]uint64, 0, len(differences))
	for id := range differences {
		ids = append(ids, id)
	}
	require.ElementsMatch(t, []uint64{1, 2, 9}, ids)
	require.Equal(t, []any{cluster.instance(1)}, differences[2]["missing_on"])
	require.Equal(t, []any{cluster.instance(0), cluster.instance(2)}, differences[9]["missing_on"])
}

func TestConfirmationLimitDoesNotTruncateDifferences(t *testing.T) {
	cluster := newFakeCluster(t)
	for i := range cluster.nodes[1].regions {
		cluster.nodes[1].regions[i].Epoch.Version++
	}
	outcome, report, _, err := runFakeCheck(t, cluster, func(cfg *Config) { cfg.ConfirmLimit = 2 })
	require.NoError(t, err)
	require.Equal(t, StatusInconsistent, outcome.Status)
	require.Len(t, report["differences"], 5)
	confirmation := report["confirmation"].(map[string]any)
	require.Equal(t, float64(2), confirmation["checked_regions"])
	require.Equal(t, float64(3), confirmation["unconfirmed_regions"])
	for _, node := range cluster.nodes {
		node.mu.Lock()
		require.Equal(t, 2, node.regionGets)
		node.mu.Unlock()
	}
}

func TestAuthorizationRequiresHTTPS(t *testing.T) {
	cluster := newFakeCluster(t)
	path := filepath.Join(t.TempDir(), "authorization")
	require.NoError(t, os.WriteFile(path, []byte("Bearer secret\n"), 0o600))
	_, _, _, err := runFakeCheck(t, cluster, func(cfg *Config) { cfg.AuthorizationFile = path })
	require.ErrorContains(t, err, "authorization requires HTTPS")
	for _, node := range cluster.nodes {
		node.mu.Lock()
		require.Empty(t, node.local)
		node.mu.Unlock()
	}
}

func TestTemporaryDifferenceLimit(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.nodes[1].regions[0].Epoch.Version++
	_, _, _, err := runFakeCheck(t, cluster, func(cfg *Config) { cfg.MaxTemporaryDiskBytes = 32 })
	require.ErrorContains(t, err, "temporary JSON data exceeds")
}
