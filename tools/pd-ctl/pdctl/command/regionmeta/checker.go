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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"
)

type summary struct {
	DifferentRegions int            `json:"different_regions"`
	ByField          map[string]int `json:"by_field"`
}

type confirmation struct {
	Result             string   `json:"result"`
	InitialDifferences int      `json:"initial_differences"`
	Limit              int      `json:"limit"`
	DelaySeconds       float64  `json:"delay_seconds"`
	CheckedRegions     int      `json:"checked_regions,omitempty"`
	UnconfirmedRegions int      `json:"unconfirmed_regions,omitempty"`
	StableRegions      []uint64 `json:"stable_regions,omitempty"`
	ResolvedRegions    []uint64 `json:"resolved_regions,omitempty"`
	ChangedRegions     []uint64 `json:"changed_regions,omitempty"`
	FinalDifferences   int      `json:"final_differences"`
}

type replacement struct {
	Difference *difference
}

type referenceReport struct {
	Name     string `json:"name"`
	MemberID uint64 `json:"member_id"`
	URL      string `json:"url"`
}

type settingsReport struct {
	BatchSize              int     `json:"batch_size"`
	RequestIntervalSeconds float64 `json:"request_interval_seconds"`
	RequestTimeoutSeconds  float64 `json:"request_timeout_seconds"`
	MaxRuntimeSeconds      float64 `json:"max_runtime_seconds"`
	GlobalConcurrency      int     `json:"global_concurrency"`
	PerNodeConcurrency     int     `json:"per_node_concurrency"`
	ConfirmationLimit      int     `json:"confirmation_limit"`
	TemporaryDiskLimitMiB  int64   `json:"temporary_disk_limit_mib"`
	OutputLimitMiB         int64   `json:"output_limit_mib"`
	HTTPRequests           int64   `json:"http_requests"`
	HTTPResponseBytes      int64   `json:"http_response_bytes"`
	TemporaryDiskPeakBytes int64   `json:"temporary_disk_peak_bytes"`
	SnapshotSemantics      string  `json:"snapshot_semantics"`
}

type nodeReport struct {
	Name         string `json:"name"`
	MemberID     uint64 `json:"member_id"`
	URL          string `json:"url"`
	Role         string `json:"role"`
	RegionCount  int    `json:"region_count"`
	Batches      int    `json:"batches"`
	ScanAttempts int    `json:"scan_attempts"`
	StartedAt    string `json:"started_at"`
	FinishedAt   string `json:"finished_at"`
}

type reportBase struct {
	Status       Status          `json:"status"`
	GeneratedAt  string          `json:"generated_at"`
	ClusterID    uint64          `json:"cluster_id"`
	Reference    referenceReport `json:"reference"`
	Settings     settingsReport  `json:"settings"`
	Nodes        []nodeReport    `json:"nodes"`
	Confirmation confirmation    `json:"confirmation"`
	Summary      summary         `json:"summary"`
}

// Run checks region meta from every discovered PD member and writes one JSON report.
func Run(ctx context.Context, cfg Config, stdout, stderr io.Writer) (Outcome, error) {
	if err := cfg.validate(); err != nil {
		return Outcome{}, err
	}
	supplied := make([]string, 0, len(cfg.Endpoints))
	seenEndpoints := make(map[string]struct{}, len(cfg.Endpoints))
	for _, raw := range cfg.Endpoints {
		endpoint, err := normalizeURL(raw)
		if err != nil {
			return Outcome{}, err
		}
		if _, exists := seenEndpoints[endpoint]; exists {
			return Outcome{}, fmt.Errorf("duplicate PD endpoint was supplied: %s", endpoint)
		}
		seenEndpoints[endpoint] = struct{}{}
		supplied = append(supplied, endpoint)
	}
	authorization, err := readAuthorization(cfg.AuthorizationFile)
	if err != nil {
		return Outcome{}, err
	}
	if authorization != "" && !allHTTPS(supplied) {
		return Outcome{}, errors.New("authorization requires HTTPS for every supplied PD URL")
	}
	if cfg.WorkDir != "" {
		info, err := os.Stat(cfg.WorkDir)
		if err != nil || !info.IsDir() {
			return Outcome{}, fmt.Errorf("work directory does not exist: %s", cfg.WorkDir)
		}
	}
	directory, err := os.MkdirTemp(cfg.WorkDir, "pd-region-meta-consistency-")
	if err != nil {
		return Outcome{}, err
	}
	defer os.RemoveAll(directory)

	runtimeCtx, cancel := context.WithTimeout(ctx, cfg.MaxRuntime)
	defer cancel()
	limiter := &rateLimiter{interval: cfg.Interval}
	sharedClient := newSharedHTTPClient(cfg.TLSConfig)
	defer sharedClient.CloseIdleConnections()
	seed := &httpClient{
		endpoint: supplied[0], client: sharedClient, limiter: limiter, timeout: cfg.Timeout,
		retries: cfg.Retries, authorization: authorization,
	}
	nodes, membershipStart, err := discoverNodes(runtimeCtx, seed, supplied)
	if err != nil {
		return Outcome{}, runtimeError(runtimeCtx, err)
	}
	memberEndpoints := make([]string, 0, len(nodes))
	for _, current := range nodes {
		memberEndpoints = append(memberEndpoints, current.URL)
	}
	if authorization != "" && !allHTTPS(memberEndpoints) {
		return Outcome{}, errors.New("authorization requires HTTPS for every PD member URL")
	}
	clients := make([]*httpClient, 0, len(nodes))
	for _, current := range nodes {
		clients = append(clients, &httpClient{
			endpoint: current.URL, client: sharedClient, limiter: limiter, timeout: cfg.Timeout,
			retries: cfg.Retries, authorization: authorization,
		})
	}
	requester := &batchRequester{limiter: limiter}
	budget := newDiskBudget(cfg.MaxTemporaryDiskBytes)
	states, rows, err := collectRegions(
		runtimeCtx, nodes, clients, cfg.BatchSize, cfg.ScanRetries, directory, budget, requester, stderr,
	)
	if err != nil {
		return Outcome{}, runtimeError(runtimeCtx, err)
	}
	defer rows.cleanup()
	initialSummary, candidates, err := summarizeDifferences(rows, nodes, cfg.ConfirmLimit)
	if err != nil {
		return Outcome{}, err
	}
	confirmationResult, replacements, err := recheckDifferences(
		runtimeCtx, candidates, initialSummary.DifferentRegions, cfg, nodes, clients, requester, stderr,
	)
	if err != nil {
		return Outcome{}, runtimeError(runtimeCtx, err)
	}
	var membershipEnd membership
	if err := seed.getJSON(runtimeCtx, "/pd/api/v1/members", nil, true, false, &membershipEnd); err != nil {
		return Outcome{}, runtimeError(runtimeCtx, err)
	}
	startSignature, err := membershipStart.signature()
	if err != nil {
		return Outcome{}, err
	}
	endSignature, err := membershipEnd.signature()
	if err != nil {
		return Outcome{}, err
	}
	if startSignature.ClusterID != endSignature.ClusterID || startSignature.LeaderID != endSignature.LeaderID ||
		!slices.Equal(startSignature.Members, endSignature.Members) {
		return Outcome{}, errors.New("membership or PD leader changed during the scan")
	}
	finalSummary := adjustSummary(initialSummary, candidates, replacements)
	confirmationResult.FinalDifferences = finalSummary.DifferentRegions
	status := finalStatus(confirmationResult, finalSummary)
	reference := nodes[0]
	report := reportBase{
		Status:      status,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		ClusterID:   membershipStart.Header.ClusterID,
		Reference: referenceReport{
			Name: reference.Name, MemberID: reference.MemberID, URL: reference.URL,
		},
		Settings: settingsReport{
			BatchSize: cfg.BatchSize, RequestIntervalSeconds: cfg.Interval.Seconds(),
			RequestTimeoutSeconds: cfg.Timeout.Seconds(), MaxRuntimeSeconds: cfg.MaxRuntime.Seconds(),
			GlobalConcurrency: len(nodes), PerNodeConcurrency: 1, ConfirmationLimit: cfg.ConfirmLimit,
			TemporaryDiskLimitMiB:  cfg.MaxTemporaryDiskBytes / (1024 * 1024),
			OutputLimitMiB:         cfg.MaxOutputBytes / (1024 * 1024),
			HTTPRequests:           seed.requests.Load() + sumRequests(clients),
			HTTPResponseBytes:      seed.responseBytes.Load() + sumResponseBytes(clients),
			TemporaryDiskPeakBytes: budget.peak,
			SnapshotSemantics: "bounded parallel batch scans; differences are rechecked, " +
				"but the result is not an atomic snapshot",
		},
		Confirmation: confirmationResult,
		Summary:      finalSummary,
	}
	for _, state := range states {
		report.Nodes = append(report.Nodes, nodeReport{
			Name: state.Node.Name, MemberID: state.Node.MemberID, URL: state.Node.URL,
			Role: state.Node.Role, RegionCount: state.Scanned, Batches: state.Pages,
			ScanAttempts: state.Attempts, StartedAt: state.StartedAt, FinishedAt: state.FinishedAt,
		})
	}
	if err := writeReport(runtimeCtx, cfg.Output, report, rows, nodes, replacements, directory, cfg.MaxOutputBytes, stdout); err != nil {
		return Outcome{}, runtimeError(runtimeCtx, err)
	}
	return Outcome{Status: status}, nil
}

func runtimeError(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("maximum runtime exceeded: %w", ctx.Err())
	}
	return err
}

func allHTTPS(endpoints []string) bool {
	for _, endpoint := range endpoints {
		u, _ := url.Parse(endpoint)
		if u.Scheme != "https" {
			return false
		}
	}
	return true
}

func sumRequests(clients []*httpClient) int64 {
	var total int64
	for _, client := range clients {
		total += client.requests.Load()
	}
	return total
}

func sumResponseBytes(clients []*httpClient) int64 {
	var total int64
	for _, client := range clients {
		total += client.responseBytes.Load()
	}
	return total
}

func iterateDifferences(rows *sortedRows, nodes []node, visit func(difference) error) error {
	var currentID uint64
	var currentRows []*regionMeta
	flush := func() error {
		if currentRows == nil {
			return nil
		}
		value := makeDifference(currentID, currentRows, nodes)
		if value == nil {
			return nil
		}
		return visit(*value)
	}
	err := rows.iterate(func(row diskRow) error {
		if currentRows == nil || row.RegionID != currentID {
			if err := flush(); err != nil {
				return err
			}
			currentID = row.RegionID
			currentRows = make([]*regionMeta, len(nodes))
		}
		if row.Node < 0 || row.Node >= len(nodes) {
			return fmt.Errorf("invalid node index %d in difference rows", row.Node)
		}
		if currentRows[row.Node] != nil {
			return fmt.Errorf("%s: duplicate region id during scan", nodes[row.Node].Name)
		}
		meta := row.Meta
		currentRows[row.Node] = &meta
		return nil
	})
	if err != nil {
		return err
	}
	return flush()
}

func summarizeDifferences(rows *sortedRows, nodes []node, limit int) (summary, []difference, error) {
	result := summary{ByField: make(map[string]int)}
	candidates := make([]difference, 0, limit)
	err := iterateDifferences(rows, nodes, func(value difference) error {
		result.DifferentRegions++
		addFields(result.ByField, value, 1)
		if len(candidates) < limit {
			candidates = append(candidates, value)
		}
		return nil
	})
	return result, candidates, err
}

func addFields(counts map[string]int, value difference, delta int) {
	if len(value.MissingOn) > 0 {
		counts["missing_on"] += delta
	}
	if len(value.KeyRange) > 0 {
		counts["key_range"] += delta
	}
	if len(value.Epoch) > 0 {
		counts["epoch"] += delta
	}
	if len(value.Peers) > 0 {
		counts["peers"] += delta
	}
	if len(value.LeaderPeer) > 0 {
		counts["leader_peer"] += delta
	}
}

func recheckDifferences(
	ctx context.Context,
	initial []difference,
	differenceCount int,
	cfg Config,
	nodes []node,
	clients []*httpClient,
	requester *batchRequester,
	progress io.Writer,
) (confirmation, map[uint64]replacement, error) {
	result := confirmation{
		InitialDifferences: differenceCount,
		Limit:              cfg.ConfirmLimit,
		DelaySeconds:       cfg.ConfirmationDelay.Seconds(),
	}
	replacements := make(map[uint64]replacement, len(initial))
	if differenceCount == 0 {
		result.Result = "not_needed"
		return result, replacements, nil
	}
	if cfg.ConfirmLimit == 0 {
		result.Result = "confirmation_disabled"
		result.UnconfirmedRegions = differenceCount
		return result, replacements, nil
	}
	result.CheckedRegions = len(initial)
	result.UnconfirmedRegions = differenceCount - len(initial)
	_, _ = fmt.Fprintf(progress, "rechecking %d differing Regions after %s\n", len(initial), cfg.ConfirmationDelay)
	if err := waitContext(ctx, cfg.ConfirmationDelay); err != nil {
		return confirmation{}, nil, err
	}
	initialByID := make(map[uint64]difference, len(initial))
	for _, original := range initial {
		initialByID[original.RegionID] = original
		raw := make([]json.RawMessage, len(nodes))
		calls := make([]requestCall, 0, len(nodes))
		for i := range nodes {
			calls = append(calls, requestCall{
				client: clients[i], path: "/pd/api/v1/region/id/" + strconv.FormatUint(original.RegionID, 10),
				local: true, allowNotFound: true, destination: &raw[i],
			})
		}
		if err := requester.getJSON(ctx, calls); err != nil {
			return confirmation{}, nil, err
		}
		metas := make([]*regionMeta, len(nodes))
		for i, payload := range raw {
			trimmed := bytes.TrimSpace(payload)
			if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) || bytes.Equal(trimmed, []byte("{}")) {
				continue
			}
			var wire wireRegion
			if err := json.Unmarshal(trimmed, &wire); err != nil {
				return confirmation{}, nil, fmt.Errorf("%s: invalid /region/id/%d response: %w",
					nodes[i].Name, original.RegionID, err)
			}
			// Some supported PD versions encode a missing Region as a zero-value RegionInfo.
			if wire.ID == 0 {
				continue
			}
			if wire.ID != original.RegionID {
				return confirmation{}, nil, fmt.Errorf("%s: invalid /region/id/%d response", nodes[i].Name, original.RegionID)
			}
			record, err := normalizeRegion(wire)
			if err != nil {
				return confirmation{}, nil, err
			}
			meta := record.Meta
			metas[i] = &meta
		}
		rechecked := makeDifference(original.RegionID, metas, nodes)
		replacements[original.RegionID] = replacement{Difference: rechecked}
	}
	for regionID, original := range initialByID {
		current := replacements[regionID].Difference
		switch {
		case current == nil:
			result.ResolvedRegions = append(result.ResolvedRegions, regionID)
		case original.equal(*current):
			result.StableRegions = append(result.StableRegions, regionID)
		default:
			result.ChangedRegions = append(result.ChangedRegions, regionID)
		}
	}
	slices.Sort(result.StableRegions)
	slices.Sort(result.ResolvedRegions)
	slices.Sort(result.ChangedRegions)
	if len(result.StableRegions) > 0 {
		result.Result = "stable"
	} else if len(initial) == differenceCount && len(result.ResolvedRegions) == len(initial) {
		result.Result = "resolved"
	} else {
		result.Result = "changed_during_recheck"
	}
	return result, replacements, nil
}

func adjustSummary(initial summary, candidates []difference, replacements map[uint64]replacement) summary {
	result := summary{DifferentRegions: initial.DifferentRegions, ByField: make(map[string]int, len(initial.ByField))}
	for field, count := range initial.ByField {
		result.ByField[field] = count
	}
	for _, original := range candidates {
		replacement, exists := replacements[original.RegionID]
		if !exists {
			continue
		}
		addFields(result.ByField, original, -1)
		if replacement.Difference == nil {
			result.DifferentRegions--
		} else {
			addFields(result.ByField, *replacement.Difference, 1)
		}
	}
	for field, count := range result.ByField {
		if count == 0 {
			delete(result.ByField, field)
		}
	}
	return result
}

func finalStatus(value confirmation, result summary) Status {
	if value.Result == "confirmation_disabled" || value.Result == "changed_during_recheck" {
		return StatusIncomplete
	}
	if result.DifferentRegions == 0 {
		return StatusConsistent
	}
	return StatusInconsistent
}

func iterateFinalDifferences(
	rows *sortedRows,
	nodes []node,
	replacements map[uint64]replacement,
	visit func(difference) error,
) error {
	return iterateDifferences(rows, nodes, func(value difference) error {
		if replacement, exists := replacements[value.RegionID]; exists {
			if replacement.Difference == nil {
				return nil
			}
			return visit(*replacement.Difference)
		}
		return visit(value)
	})
}

type limitedWriter struct {
	writer  io.Writer
	limit   int64
	written int64
}

func (w *limitedWriter) Write(payload []byte) (int, error) {
	if w.written+int64(len(payload)) > w.limit {
		return 0, fmt.Errorf("report JSON exceeds %d MiB", w.limit/(1024*1024))
	}
	n, err := w.writer.Write(payload)
	w.written += int64(n)
	return n, err
}

func writeReport(
	ctx context.Context,
	outputPath string,
	report reportBase,
	rows *sortedRows,
	nodes []node,
	replacements map[uint64]replacement,
	workDirectory string,
	maxBytes int64,
	stdout io.Writer,
) (returnErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	directory := workDirectory
	var target string
	if outputPath != "-" {
		absolute, err := filepath.Abs(outputPath)
		if err != nil {
			return err
		}
		target = absolute
		directory = filepath.Dir(target)
		if info, err := os.Stat(directory); err != nil || !info.IsDir() {
			return fmt.Errorf("output directory does not exist: %s", directory)
		}
	}
	temporary, err := os.CreateTemp(directory, "region-meta-report-*.json")
	if err != nil {
		return err
	}
	path := temporary.Name()
	defer func() {
		if returnErr != nil {
			_ = temporary.Close()
			_ = os.Remove(path)
		}
	}()
	buffered := bufio.NewWriter(temporary)
	writer := &limitedWriter{writer: buffered, limit: maxBytes}
	header, err := json.Marshal(report)
	if err != nil {
		return err
	}
	if len(header) == 0 || header[len(header)-1] != '}' {
		return errors.New("internal report JSON header is invalid")
	}
	if _, err := writer.Write(header[:len(header)-1]); err != nil {
		return err
	}
	if _, err := io.WriteString(writer, `,"differences":[`); err != nil {
		return err
	}
	first := true
	if err := iterateFinalDifferences(rows, nodes, replacements, func(value difference) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !first {
			if _, err := io.WriteString(writer, ","); err != nil {
				return err
			}
		}
		first = false
		payload, err := json.Marshal(value)
		if err != nil {
			return err
		}
		_, err = writer.Write(payload)
		return err
	}); err != nil {
		return err
	}
	if _, err := io.WriteString(writer, "]}\n"); err != nil {
		return err
	}
	if err := buffered.Flush(); err != nil {
		return err
	}
	if target != "" {
		if err := temporary.Sync(); err != nil {
			return err
		}
		if err := temporary.Close(); err != nil {
			return err
		}
		return os.Rename(path, target)
	}
	if _, err := temporary.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := io.Copy(stdout, temporary); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Remove(path)
}
