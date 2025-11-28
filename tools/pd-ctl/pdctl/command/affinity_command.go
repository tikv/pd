// Copyright 2025 TiKV Project Authors.
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

package command

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/pingcap/errors"

	pd "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/pkg/codec"
)

const (
	tableGroupIDPattern     = "_tidb_c_t_%d"
	partitionGroupIDPattern = "_tidb_p_t_%d_p%d"
	httpRequestTimeout      = 30 * time.Second
)

// NOTE: The regex patterns below must match the format defined in the constants above:
// - tableGroupRegexp must match tableGroupIDPattern format
// - partitionGroupRegexp must match partitionGroupIDPattern format
var (
	partitionGroupRegexp = regexp.MustCompile(`^_tidb_p_t_(\d+)_p(\d+)$`)
	tableGroupRegexp     = regexp.MustCompile(`^_tidb_c_t_(\d+)$`)
)

// ciStr represents a case-insensitive string (TiDB's CIStr type).
type ciStr struct {
	O string `json:"O"` // Original string
	L string `json:"L"` // Lowercase string
}

// String returns the original string value.
func (s ciStr) String() string {
	return s.O
}

// tidbTableInfo represents the table schema from /schema/{db}/{table} API.
type tidbTableInfo struct {
	ID        int64              `json:"id"`
	Name      ciStr              `json:"name"`
	Partition *tidbPartitionInfo `json:"partition,omitempty"`
}

type tidbPartitionInfo struct {
	Enable      bool                      `json:"enable"`
	Definitions []tidbPartitionDefinition `json:"definitions"`
}

type tidbPartitionDefinition struct {
	ID   int64 `json:"id"`
	Name ciStr `json:"name"`
}

type partitionInfo struct {
	ID   int64
	Name string
}

type tableAffinityInfo struct {
	DB         string
	Table      string
	TableID    int64
	Partitions []partitionInfo
}

type affinityGroupDefinition struct {
	id     string
	ranges []pd.AffinityGroupKeyRange
}

type affinityGroupResolved struct {
	GroupID     string                 `json:"group_id"`
	Database    string                 `json:"database,omitempty"`
	Table       string                 `json:"table,omitempty"`
	Partition   string                 `json:"partition,omitempty"`
	TableID     int64                  `json:"table_id"`
	PartitionID int64                  `json:"partition_id,omitempty"`
	State       *pd.AffinityGroupState `json:"state,omitempty"`
}

// NewAffinityCommand creates the affinity command.
func NewAffinityCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "affinity",
		Short:             "affinity group commands based on TiDB metadata",
		Long:              `Affinity group commands use TiDB HTTP API to fetch table/partition metadata.`,
		PersistentPreRunE: requirePDClient,
	}
	cmd.PersistentFlags().String("tidb-http", "", "TiDB HTTP status address (e.g., http://127.0.0.1:10080)")
	cmd.PersistentFlags().String("db", "", "database name of the target table")
	cmd.PersistentFlags().String("table", "", "table name of the target table")
	cmd.PersistentFlags().String("partition", "", "target partition name or ID when operating on a partitioned table")

	cmd.AddCommand(
		newAffinityCreateCommand(),
		newAffinityShowCommand(),
		newAffinityDeleteCommand(),
		newAffinityUpdatePeersCommand(),
		newAffinityListCommand(),
	)
	return cmd
}

func newAffinityCreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "create affinity groups for a table or its partitions",
		Run:   affinityCreateCommandFunc,
	}
	return cmd
}

func newAffinityShowCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show",
		Short: "show affinity group states for the target table",
		Run:   affinityShowCommandFunc,
	}
	return cmd
}

func newAffinityDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "delete affinity groups for the target table",
		Run:   affinityDeleteCommandFunc,
	}
	cmd.Flags().Bool("force", false, "force delete the affinity group even if it has key ranges")
	return cmd
}

func newAffinityUpdatePeersCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update",
		Short: "update leader and voters for a specific affinity group",
		Run:   affinityUpdatePeersCommandFunc,
	}
	cmd.Flags().Uint64("leader", 0, "leader store ID")
	cmd.Flags().String("voters", "", "comma separated voter store IDs, e.g. 1,2,3")
	return cmd
}

func newAffinityListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "list affinity groups with resolved table and partition names",
		Run:   affinityListCommandFunc,
	}
	cmd.Flags().String("tidb-http", "", "TiDB HTTP status address to resolve table names")
	return cmd
}

func affinityCreateCommandFunc(cmd *cobra.Command, _ []string) {
	defs, err := loadAffinityGroupDefinitions(cmd)
	if err != nil {
		cmd.Println(err)
		return
	}

	groups := make(map[string][]pd.AffinityGroupKeyRange, len(defs))
	for _, def := range defs {
		groups[def.id] = def.ranges
	}

	resp, err := PDCli.CreateAffinityGroups(cmd.Context(), groups)
	if err != nil {
		cmd.Printf("Failed to create affinity groups: %v\n", err)
		return
	}
	jsonPrint(cmd, resp)
}

func affinityShowCommandFunc(cmd *cobra.Command, _ []string) {
	defs, err := loadAffinityGroupDefinitions(cmd)
	if err != nil {
		cmd.Println(err)
		return
	}

	result := make(map[string]*pd.AffinityGroupState, len(defs))
	found := false
	for _, def := range defs {
		state, err := PDCli.GetAffinityGroup(cmd.Context(), def.id)
		if err != nil {
			// Check for 404 Not Found status
			if strings.Contains(err.Error(), "404 Not Found") {
				continue
			}
			cmd.Printf("Failed to show affinity group %s: %v\n", def.id, err)
			return
		}
		result[def.id] = state
		found = true
	}
	if !found {
		cmd.Println("No affinity groups found")
		return
	}
	jsonPrint(cmd, result)
}

func affinityDeleteCommandFunc(cmd *cobra.Command, _ []string) {
	defs, err := loadAffinityGroupDefinitions(cmd)
	if err != nil {
		cmd.Println(err)
		return
	}

	force, _ := cmd.Flags().GetBool("force")
	ids := make([]string, len(defs))
	for i, def := range defs {
		ids[i] = def.id
	}

	if err := PDCli.BatchDeleteAffinityGroups(cmd.Context(), ids, force); err != nil {
		cmd.Printf("Failed to delete affinity groups: %v\n", err)
		return
	}
	if len(ids) == 1 {
		cmd.Printf("Affinity group %s deleted\n", ids[0])
	} else {
		cmd.Printf("Affinity groups deleted: %s\n", strings.Join(ids, ","))
	}
}

func affinityUpdatePeersCommandFunc(cmd *cobra.Command, _ []string) {
	defs, err := loadAffinityGroupDefinitions(cmd)
	if err != nil {
		cmd.Println(err)
		return
	}

	// TODO: support batch updating all partitions in one call when needed.
	if len(defs) != 1 {
		cmd.Println("Specify --partition to target a single affinity group in partitioned tables")
		return
	}

	leader, _ := cmd.Flags().GetUint64("leader")
	if leader == 0 {
		cmd.Println("leader is required")
		return
	}

	voterStr, _ := cmd.Flags().GetString("voters")
	voters, err := parseUint64List(voterStr)
	if err != nil {
		cmd.Printf("Failed to parse voters: %v\n", err)
		return
	}
	if len(voters) == 0 {
		cmd.Println("voters is required")
		return
	}

	state, err := PDCli.UpdateAffinityGroupPeers(cmd.Context(), defs[0].id, leader, voters)
	if err != nil {
		cmd.Printf("Failed to update affinity group peers: %v\n", err)
		return
	}
	jsonPrint(cmd, state)
}

// loadAffinityGroupDefinitions loads table info and builds affinity group definitions.
func loadAffinityGroupDefinitions(cmd *cobra.Command) ([]affinityGroupDefinition, error) {
	info, partition, err := loadTableAffinityInfo(cmd)
	if err != nil {
		return nil, err
	}
	return buildAffinityGroupDefinitions(info, partition)
}

func loadTableAffinityInfo(cmd *cobra.Command) (tableAffinityInfo, string, error) {
	dbName, _ := cmd.Flags().GetString("db")
	tableName, _ := cmd.Flags().GetString("table")
	tidbHTTP, _ := cmd.Flags().GetString("tidb-http")
	partition, _ := cmd.Flags().GetString("partition")
	if dbName == "" || tableName == "" {
		return tableAffinityInfo{}, "", errors.New("db and table are required")
	}
	info, err := fetchTableAffinityInfo(cmd.Context(), cmd, tidbHTTP, dbName, tableName)
	if err != nil {
		return tableAffinityInfo{}, "", err
	}
	return info, partition, nil
}

func fetchTableAffinityInfo(ctx context.Context, cmd *cobra.Command, tidbHTTP, dbName, tableName string) (tableAffinityInfo, error) {
	// Get HTTP client with TLS support
	httpClient, err := createHTTPClient(cmd)
	if err != nil {
		return tableAffinityInfo{}, err
	}

	// Resolve TiDB HTTP address
	httpAddr, err := resolveTiDBHTTPAddress(ctx, httpClient, tidbHTTP)
	if err != nil {
		return tableAffinityInfo{}, err
	}

	// Fetch table schema via HTTP API
	tableInfo, err := fetchTableSchema(ctx, httpClient, httpAddr, dbName, tableName)
	if err != nil {
		return tableAffinityInfo{}, err
	}

	// Convert TiDB API response to our internal format
	var partitions []partitionInfo
	if tableInfo.Partition != nil && tableInfo.Partition.Enable {
		for _, def := range tableInfo.Partition.Definitions {
			partitions = append(partitions, partitionInfo{
				ID:   def.ID,
				Name: def.Name.String(),
			})
		}
	}

	return tableAffinityInfo{
		DB:         dbName,
		Table:      tableName,
		TableID:    tableInfo.ID,
		Partitions: partitions,
	}, nil
}

// resolveTiDBHTTPAddress resolves the TiDB HTTP address from flag or auto-discovery.
func resolveTiDBHTTPAddress(ctx context.Context, httpClient *http.Client, tidbHTTP string) (string, error) {
	if tidbHTTP != "" {
		return tidbHTTP, nil
	}

	// Auto-discover from default address
	httpAddr, err := discoverTiDBHTTPAddress(ctx, httpClient)
	if err != nil {
		return "", errors.New("failed to discover TiDB HTTP address. Please specify --tidb-http flag (e.g., --tidb-http=http://127.0.0.1:10080)")
	}
	return httpAddr, nil
}

// createHTTPClient creates an HTTP client with optional TLS configuration.
func createHTTPClient(cmd *cobra.Command) (*http.Client, error) {
	tlsConfig, err := parseTLSConfig(cmd)
	if err != nil {
		return nil, err
	}

	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}

	return &http.Client{
		Timeout:   httpRequestTimeout,
		Transport: transport,
	}, nil
}

// doHTTPRequest performs an HTTP GET request and optionally decodes the JSON response.
func doHTTPRequest(ctx context.Context, httpClient *http.Client, url string, result any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return errors.Wrapf(err, "failed to create request for %s", url)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return errors.Wrapf(err, "failed to request %s", url)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return errors.Errorf("HTTP %d from %s: %s", resp.StatusCode, url, string(body))
	}

	// If result is nil, just verify the request succeeded
	if result == nil {
		return nil
	}

	// Decode JSON response
	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return errors.Wrapf(err, "failed to decode response from %s", url)
	}

	return nil
}

// discoverTiDBHTTPAddress attempts to discover a TiDB server address using the default address.
func discoverTiDBHTTPAddress(ctx context.Context, httpClient *http.Client) (string, error) {
	// Use default TiDB status port
	addr := "http://127.0.0.1:10080"
	url := fmt.Sprintf("%s/status", addr)

	// Try /status endpoint to verify TiDB is accessible
	if err := doHTTPRequest(ctx, httpClient, url, nil); err != nil {
		return "", errors.Wrapf(err, "TiDB not accessible at %s", addr)
	}

	return addr, nil
}

// fetchTableSchema fetches table schema from TiDB HTTP API.
func fetchTableSchema(ctx context.Context, httpClient *http.Client, httpAddr, dbName, tableName string) (*tidbTableInfo, error) {
	url := fmt.Sprintf("%s/schema/%s/%s", httpAddr, dbName, tableName)

	var tableInfo tidbTableInfo
	if err := doHTTPRequest(ctx, httpClient, url, &tableInfo); err != nil {
		if strings.Contains(err.Error(), "HTTP 404") {
			return nil, errors.Errorf("table %s.%s not found", dbName, tableName)
		}
		return nil, err
	}

	return &tableInfo, nil
}

func buildAffinityGroupDefinitions(info tableAffinityInfo, partition string) ([]affinityGroupDefinition, error) {
	if len(info.Partitions) == 0 {
		if partition != "" {
			return nil, errors.New("--partition is only allowed for partitioned tables")
		}
		start, end := tableKeyRange(info.TableID)
		return []affinityGroupDefinition{{
			id: tableGroupID(info.TableID),
			ranges: []pd.AffinityGroupKeyRange{{
				StartKey: start,
				EndKey:   end,
			}},
		}}, nil
	}

	selected := info.Partitions
	if partition != "" {
		match, err := selectPartition(info.Partitions, partition)
		if err != nil {
			return nil, err
		}
		selected = []partitionInfo{match}
	}

	defs := make([]affinityGroupDefinition, 0, len(selected))
	for _, p := range selected {
		start, end := tableKeyRange(p.ID)
		defs = append(defs, affinityGroupDefinition{
			id: partitionGroupID(info.TableID, p.ID),
			ranges: []pd.AffinityGroupKeyRange{{
				StartKey: start,
				EndKey:   end,
			}},
		})
	}
	return defs, nil
}

func selectPartition(partitions []partitionInfo, target string) (partitionInfo, error) {
	for _, p := range partitions {
		if strings.EqualFold(p.Name, target) || strconv.FormatInt(p.ID, 10) == target {
			return p, nil
		}
	}
	return partitionInfo{}, errors.Errorf("partition %s not found", target)
}

func collectGroupIDs(defs []affinityGroupDefinition) []string {
	ids := make([]string, 0, len(defs))
	for _, def := range defs {
		ids = append(ids, def.id)
	}
	return ids
}

func tableKeyRange(id int64) (start []byte, end []byte) {
	start = encodeTablePrefix(id)
	end = encodeTablePrefix(id + 1)
	return
}

func encodeTablePrefix(id int64) []byte {
	key := codec.GenerateTableKey(id)
	return codec.EncodeBytes(key)
}

func partitionGroupID(tableID, partitionID int64) string {
	return fmt.Sprintf(partitionGroupIDPattern, tableID, partitionID)
}

func tableGroupID(tableID int64) string {
	return fmt.Sprintf(tableGroupIDPattern, tableID)
}

func parseUint64List(input string) ([]uint64, error) {
	if strings.TrimSpace(input) == "" {
		return nil, nil
	}
	parts := strings.Split(input, ",")
	res := make([]uint64, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		v, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		res = append(res, v)
	}
	return res, nil
}

func affinityListCommandFunc(cmd *cobra.Command, _ []string) {
	tidbHTTP, _ := cmd.Flags().GetString("tidb-http")
	groups, err := PDCli.GetAllAffinityGroups(cmd.Context())
	if err != nil {
		cmd.Printf("Failed to get affinity groups: %v\n", err)
		return
	}

	parsed := parseGroupIDs(groups)
	if len(parsed.tableIDs) == 0 && len(parsed.partitionIDs) == 0 {
		jsonPrint(cmd, []affinityGroupResolved{})
		return
	}

	// Get HTTP client
	httpClient, err := createHTTPClient(cmd)
	if err != nil {
		cmd.Printf("Failed to create HTTP client: %v\n", err)
		return
	}

	// Resolve TiDB HTTP address
	httpAddr, err := resolveTiDBHTTPAddress(cmd.Context(), httpClient, tidbHTTP)
	if err != nil {
		cmd.Println(err)
		return
	}

	// Resolve all table and partition names in a single pass
	names := resolveNames(cmd.Context(), httpClient, httpAddr, parsed.tableIDs, parsed.partitionIDs)

	result := make([]affinityGroupResolved, 0, len(groups))
	for id, state := range groups {
		if info, ok := parsed.partitionGroups[id]; ok {
			pName := names.partitions[info.partitionID]
			tName := names.tables[pName.TableID]
			result = append(result, affinityGroupResolved{
				GroupID:     id,
				Database:    tName.Schema,
				Table:       tName.Name,
				Partition:   pName.Name,
				TableID:     info.tableID,
				PartitionID: info.partitionID,
				State:       state,
			})
			continue
		}
		if info, ok := parsed.tableGroups[id]; ok {
			tName := names.tables[info.tableID]
			result = append(result, affinityGroupResolved{
				GroupID:  id,
				Database: tName.Schema,
				Table:    tName.Name,
				TableID:  info.tableID,
				State:    state,
			})
		}
	}
	jsonPrint(cmd, result)
}

type parsedGroups struct {
	tableIDs        []int64
	partitionIDs    []int64
	partitionGroups map[string]struct {
		tableID     int64
		partitionID int64
	}
	tableGroups map[string]struct {
		tableID int64
	}
}

func parseGroupIDs(groups map[string]*pd.AffinityGroupState) parsedGroups {
	pg := parsedGroups{
		partitionGroups: make(map[string]struct {
			tableID     int64
			partitionID int64
		}),
		tableGroups: make(map[string]struct{ tableID int64 }),
	}
	for id := range groups {
		if matches := partitionGroupRegexp.FindStringSubmatch(id); len(matches) == 3 {
			tableID, _ := strconv.ParseInt(matches[1], 10, 64)
			partitionID, _ := strconv.ParseInt(matches[2], 10, 64)
			pg.partitionGroups[id] = struct {
				tableID     int64
				partitionID int64
			}{tableID: tableID, partitionID: partitionID}
			pg.tableIDs = append(pg.tableIDs, tableID)
			pg.partitionIDs = append(pg.partitionIDs, partitionID)
			continue
		}
		if matches := tableGroupRegexp.FindStringSubmatch(id); len(matches) == 2 {
			tableID, _ := strconv.ParseInt(matches[1], 10, 64)
			pg.tableGroups[id] = struct{ tableID int64 }{tableID: tableID}
			pg.tableIDs = append(pg.tableIDs, tableID)
		}
	}
	pg.tableIDs = uniqInt64(pg.tableIDs)
	pg.partitionIDs = uniqInt64(pg.partitionIDs)
	return pg
}

func uniqInt64(ids []int64) []int64 {
	if len(ids) == 0 {
		return ids
	}
	m := make(map[int64]struct{}, len(ids))
	res := make([]int64, 0, len(ids))
	for _, id := range ids {
		if _, ok := m[id]; ok {
			continue
		}
		m[id] = struct{}{}
		res = append(res, id)
	}
	return res
}

type tableName struct {
	Schema string
	Name   string
}

type partitionName struct {
	TableID int64
	Name    string
}

type resolvedNames struct {
	tables     map[int64]tableName     // tableID -> tableName
	partitions map[int64]partitionName // partitionID -> partitionName
}

// resolveNames fetches table and partition names by IDs in a single pass.
// This function performs a single traversal of all schemas and tables,
// resolving both table IDs and partition IDs simultaneously to minimize HTTP requests.
// Returns a best-effort result, ignoring any errors.
func resolveNames(ctx context.Context, httpClient *http.Client, httpAddr string, tableIDs, partitionIDs []int64) resolvedNames {
	result := resolvedNames{
		tables:     make(map[int64]tableName),
		partitions: make(map[int64]partitionName),
	}

	if len(tableIDs) == 0 && len(partitionIDs) == 0 {
		return result
	}

	// Build lookup sets for O(1) checking
	neededTableIDs := make(map[int64]bool, len(tableIDs))
	for _, id := range tableIDs {
		neededTableIDs[id] = true
	}

	neededPartitionIDs := make(map[int64]bool, len(partitionIDs))
	for _, id := range partitionIDs {
		neededPartitionIDs[id] = true
	}

	// Fetch all schemas
	var schemas []struct {
		Name string `json:"name"`
	}
	url := fmt.Sprintf("%s/schema", httpAddr)
	if err := doHTTPRequest(ctx, httpClient, url, &schemas); err != nil {
		return result // Return empty result if API fails
	}

	// Single pass: fetch all tables and resolve both table and partition names
	for _, schema := range schemas {
		var tables []tidbTableInfo
		url := fmt.Sprintf("%s/schema/%s", httpAddr, schema.Name)
		if err := doHTTPRequest(ctx, httpClient, url, &tables); err != nil {
			continue
		}

		for _, table := range tables {
			// Check if this table ID is needed
			if neededTableIDs[table.ID] {
				result.tables[table.ID] = tableName{
					Schema: schema.Name,
					Name:   table.Name.String(),
				}
			}

			// Check partitions if present
			if table.Partition != nil && table.Partition.Enable {
				for _, partition := range table.Partition.Definitions {
					if neededPartitionIDs[partition.ID] {
						result.partitions[partition.ID] = partitionName{
							TableID: table.ID,
							Name:    partition.Name.String(),
						}
					}
				}
			}
		}
	}

	return result
}
