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
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/docker/go-units"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/pingcap/tidb/pkg/util/codec"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/router"
	"github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

const (
	msgSize                = 16 * units.MiB
	defaultLimit           = 8192
	statusMergeFailed      = "merge_failed"
	statusMergeSkipped     = "merge_skipped"
	statusMergeRequestSent = "merge_request_sent"
	maxMergeRetries        = 10
	mergeRetryDelay        = 6 * time.Second
)

// PatrolResult defines the structure for JSON output of each processed region.
type PatrolResult struct {
	RegionID    uint64 `json:"region_id"`
	Key         string `json:"key"`
	TableID     int64  `json:"table_id"`
	Status      string `json:"status"`
	Description string `json:"description,omitempty"`
}

// PatrolResults defines the structure for the final JSON output of the patrol command.
type PatrolResults struct {
	ScanCount    int               `json:"scan_count"`
	ScanDuration typeutil.Duration `json:"scan_duration"`
	Count        int               `json:"count"`
	Results      []PatrolResult    `json:"results"`
}

// NewPatrolCommand creates the patrol subcommand.
func NewPatrolCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "patrol",
		Short:             "Patrol regions to find special keys and optionally merge them. Note that this command is temporary for tiflash#10147.",
		PersistentPreRunE: requirePDClient,
		Run:               patrolCommandFunc,
	}
	cmd.Flags().Int("limit", defaultLimit, "Limit of regions to scan per batch. If limit is not positive, it means no limit.")
	cmd.Flags().Bool("enable-auto-merge", false, "Enable automatic region merge for regions with special keys.")
	return cmd
}

func patrolCommandFunc(cmd *cobra.Command, _ []string) {
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	// Create a pd client
	opts, err := getSecurityOpt(cmd)
	if err != nil {
		cmd.Printf("Failed to parse TLS options: %v\n", err)
		return
	}
	addr := getEndpoints(cmd)
	cli, err := pd.NewClientWithContext(ctx, caller.PDCtlComponent, addr, opts,
		opt.WithGRPCDialOptions(grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(msgSize))))
	if err != nil {
		cmd.Printf("Failed to create pd client: %v\n", err)
		return
	}
	defer cli.Close()

	// Scan for special regions
	startTime := time.Now()
	specialRegions, scanCount, err := scanForSpecialKeys(ctx, cmd, cli)
	if err != nil {
		cmd.Printf("Error during region scan: %v\n", err)
		return
	}

	// Process special regions
	// If enableAutoMerge is true, we will try to merge regions with special keys.
	// If it is false, we will skip the merge operation and just report the regions.
	results := processSpecialRegions(ctx, cmd, specialRegions)

	// Print final JSON output
	patrolResults := &PatrolResults{
		ScanCount:    scanCount,
		ScanDuration: typeutil.NewDuration(time.Since(startTime)),
		Count:        len(results),
		Results:      results,
	}
	finalOutput, err := json.MarshalIndent(patrolResults, "", "  ")
	if err != nil {
		cmd.Printf("Failed to marshal patrol results to JSON: %v\n", err)
		return
	}
	cmd.Println(string(finalOutput))
}

func scanForSpecialKeys(ctx context.Context, cmd *cobra.Command, cli pd.Client) (map[uint64]*router.Region, int, error) {
	startKey := []byte{}
	count := 0
	specialRegions := make(map[uint64]*router.Region)
	limit, _ := cmd.Flags().GetInt("limit")
	for {
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		default:
		}
		regions, err := cli.ScanRegions(ctx, startKey, nil, limit)
		if err != nil {
			return nil, 0, ctx.Err()
		}
		for _, region := range regions {
			found := checkRegion(cmd, region)
			if found {
				specialRegions[region.Meta.GetId()] = region
			}
		}
		count += len(regions)
		if len(regions) == 0 {
			break
		}
		endKey := regions[len(regions)-1].Meta.GetEndKey()
		if len(endKey) == 0 || bytes.Compare(startKey, endKey) >= 0 {
			break
		}
		startKey = endKey
	}
	return specialRegions, count, nil
}

func checkRegion(cmd *cobra.Command, region *router.Region) bool {
	key := region.Meta.GetEndKey()
	if len(key) == 0 {
		return false
	}
	// Add a panic recovery to ensure we don't crash the entire program
	defer func() {
		if r := recover(); r != nil {
			regionID := region.Meta.GetId()
			hexKeyStr := hex.EncodeToString(key)
			cmd.Printf("Recovered from panic while processing region %d with key %s: %v\n", regionID, hexKeyStr, r)
		}
	}()
	rootNode := N("key", region.Meta.GetEndKey())
	rootNode.Expand()
	return hasSpecialPatternRecursive(rootNode)
}

func processSpecialRegions(ctx context.Context, cmd *cobra.Command, specialRegions map[uint64]*router.Region) []PatrolResult {
	enableAutoMerge, _ := cmd.Flags().GetBool("enable-auto-merge")
	results := make([]PatrolResult, 0)
	for regionID, region := range specialRegions {
		// Prepare the result structure
		endKeyHex := hex.EncodeToString(region.Meta.GetEndKey())
		tableID, err := extractTableID(region)
		result := PatrolResult{
			RegionID: regionID,
			Key:      endKeyHex,
			TableID:  tableID,
		}
		if err != nil {
			result.Status = statusMergeSkipped
			result.Description = fmt.Sprintf("failed to extract table ID from region %d: %v", regionID, err)
			results = append(results, result)
			continue
		}
		if !enableAutoMerge {
			result.Status = statusMergeSkipped
			results = append(results, result)
			continue
		}
		// Find the next region that matches the end key of the current region
		nextRegion, err := findMatchingSiblingWithRetry(ctx, cmd, region)
		if err != nil {
			result.Status = statusMergeFailed
			result.Description = err.Error()
			results = append(results, result)
			continue
		}
		// Create merge operator
		input := map[string]any{
			"name":             "merge-region",
			"source_region_id": regionID,
			"target_region_id": nextRegion.ID,
		}
		err = PDCli.CreateOperators(ctx, input)
		if err != nil {
			result.Status = statusMergeFailed
			result.Description = fmt.Sprintf("failed to create merge operator for region %d: %v", regionID, err)
		} else {
			result.Status = statusMergeRequestSent
			result.Description = fmt.Sprintf("merge request sent for region %d and region %d", regionID, nextRegion.ID)
		}
		results = append(results, result)
	}
	return results
}

// findMatchingSiblingWithRetry contains the complex retry logic for finding a stable sibling.
func findMatchingSiblingWithRetry(ctx context.Context, cmd *cobra.Command, region *router.Region) (*http.RegionInfo, error) {
	regionID := region.Meta.GetId()
	ticker := time.NewTicker(mergeRetryDelay)
	defer ticker.Stop()
	for i := range maxMergeRetries {
		siblingRegions, err := PDCli.GetRegionSiblingsByID(ctx, regionID)
		if err != nil {
			return nil, fmt.Errorf("failed to get sibling regions for region %d: %v", regionID, err)
		}
		if siblingRegions.Count == 0 {
			return nil, fmt.Errorf("no sibling regions found for region %d", regionID)
		}

		nextRegion := siblingRegions.Regions[siblingRegions.Count-1]
		nextRegionStartKeyBytes, err := hex.DecodeString(nextRegion.GetStartKey())
		if err != nil {
			return nil, fmt.Errorf("failed to decode sibling region's start key for region %d: %w", regionID, err)
		}
		if bytes.Equal(region.Meta.GetEndKey(), nextRegionStartKeyBytes) {
			return &nextRegion, nil // Success
		}

		if i == maxMergeRetries-1 {
			break // Last attempt failed, break to return the final error
		}

		cmd.Printf("Region %d's endKey does not match sibling %d's startKey. Retrying... (Attempt %d/%d)\n",
			region.Meta.GetId(), nextRegion.ID, i+1, maxMergeRetries)

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return nil, fmt.Errorf("merge cancelled during retry-wait: %w", ctx.Err())
		}
	}

	return nil, fmt.Errorf("merge failed: no matching sibling found for region %d", regionID)
}

func extractTableID(region *router.Region) (int64, error) {
	rootNode := N("key", region.Meta.GetEndKey())
	rootNode.Expand()
	tableID, _, err := extractTableIDRecursive(rootNode)
	return tableID, err
}

// hasSpecialPatternRecursive recursively searches the Node tree for the specific key pattern.
func hasSpecialPatternRecursive(node *Node) bool {
	for _, variant := range node.variants {
		// Target pattern path:
		// Node (rootNode or child of DecodeHex)
		//  -> Variant (method: "decode hex key") [It has been finished in Expand()]
		//    -> Node (val: hex_decoded_bytes)
		//       -> Variant (method: "decode mvcc key")
		//          -> Node (val: mvcc_key_body, call as mvccBodyNode)
		//             -> Variant (method: "table row key", call as tableRowVariant)
		//                -> Node (typ: "table_id", ...)
		//                -> Node (typ: "index_values" or "row_id", val: row_data, call as rowDataNode)
		//                   -> Variant (method: "decode index values")

		if variant.method == "decode mvcc key" {
			for _, mvccBodyNode := range variant.children {
				for _, tableRowVariant := range mvccBodyNode.variants {
					if tableRowVariant.method == "table row key" {
						// According to DecodeTableRow, it should have 2 children:
						// children[0] is N("table_id", ...)
						// children[1] is N(handleTyp, row_data_bytes) -> this is rowDataNode (Node_B)
						if len(tableRowVariant.children) == 2 {
							rowDataNode := tableRowVariant.children[1]
							// Confirm if rowDataNode's type is as expected, which is determined by DecodeTableRow's handleTyp.
							if rowDataNode.typ == "index_values" || rowDataNode.typ == "row_id" {
								// Condition 1: Does row data end with non \x00?
								// And we only care about the 9 bytes of the row data.
								if len(rowDataNode.val) != 9 || rowDataNode.val[len(rowDataNode.val)-1] == '\x00' {
									continue
								}
								// Condition 2: Does rowDataNode have extra output?
								for _, rdnVariant := range rowDataNode.variants {
									if rdnVariant.method == "decode index values" {
										return true
									}
								}
							}
						}
					}
				}
			}
		}

		// We need to recursively check all children of the current variant.
		if slices.ContainsFunc(variant.children, hasSpecialPatternRecursive) {
			return true
		}
	}
	return false
}

// extractTableIDRecursive recursively searches the expanded Node tree to try and extract and decode the table ID.
func extractTableIDRecursive(node *Node) (tableID int64, found bool, err error) {
	for _, variant := range node.variants {
		if variant.method == "decode mvcc key" {
			for _, mvccChildNode := range variant.children {
				for _, detailVariant := range mvccChildNode.variants {
					if detailVariant.method == "table prefix" || detailVariant.method == "table row key" {
						// Both of these variant types should have a child Node with typ "table_id".
						// its `.val` contains bytes decodable by `codec.DecodeInt()`.
						for _, childOfDetail := range detailVariant.children {
							if childOfDetail.typ == "table_id" {
								_, id, decodeErr := codec.DecodeInt(childOfDetail.val)
								if decodeErr == nil {
									return id, true, nil
								}
								return 0, false, fmt.Errorf("failed to decode table_id node (type: %s, value_hex: %x): %w",
									childOfDetail.typ, childOfDetail.val, decodeErr)
							}
						}
					}
				}
			}
		}

		for _, childNode := range variant.children {
			id, found, err := extractTableIDRecursive(childNode)
			if err != nil {
				return 0, false, err
			}
			if found {
				return id, true, nil
			}
		}
	}
	return 0, false, nil
}

func getSecurityOpt(cmd *cobra.Command) (opt pd.SecurityOption, err error) {
	caPath, err := cmd.Flags().GetString("cacert")
	if err != nil {
		return opt, err
	}
	if caPath == "" {
		return opt, nil
	}
	certPath, err := cmd.Flags().GetString("cert")
	if err != nil {
		return opt, err
	}
	keyPath, err := cmd.Flags().GetString("key")
	if err != nil {
		return opt, err
	}
	opt.CAPath = caPath
	opt.CertPath = certPath
	opt.KeyPath = keyPath
	return opt, nil
}
