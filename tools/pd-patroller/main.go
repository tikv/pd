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

package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"slices"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/codec"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const defaultLimit = 1048576

var (
	pdAddrs  = flag.String("pd", "127.0.0.1:2379", "pd address")
	limit    = flag.Int("limit", defaultLimit, "the limit of regions")
	caPath   = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs")
	certPath = flag.String("cert", "", "path of file that contains X509 certificate in PEM format")
	keyPath  = flag.String("key", "", "path of file that contains X509 key in PEM format")
)

func main() {
	flag.Parse()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		<-sc
		cancel()
	}()

	pdCli, err := pd.NewClientWithContext(ctx, []string{*pdAddrs},
		pd.SecurityOption{
			CAPath:   *caPath,
			CertPath: *certPath,
			KeyPath:  *keyPath,
		})
	if err != nil {
		log.Error("failed to create pd client", zap.Error(err))
		os.Exit(1)
	}
	defer pdCli.Close()

	if *limit <= 0 {
		log.Info("limit is negative, which means no limit")
	}

	// TODO: if the cluster supports WithAllowFollowerHandle, we will use it.
	startTime := time.Now()
	count, err := patrolRegions(ctx, pdCli, *limit)
	if err != nil {
		log.Error("failed to patrol regions", zap.Error(err))
		os.Exit(1)
	}
	log.Info("patrol regions finished",
		zap.Int("count", count),
		zap.Duration("duration", time.Since(startTime)))
}

func patrolRegions(ctx context.Context, pdCli pd.Client, limit int) (int, error) {
	startKey := []byte{}
	count := 0
	for {
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		default:
		}
		regions, err := pdCli.ScanRegions(ctx, startKey, nil, limit)
		if err != nil {
			return count, err
		}
		for _, region := range regions {
			checkRegion(region)
		}
		endKey := regions[len(regions)-1].Meta.GetEndKey()
		count += len(regions)
		if len(regions) == 0 || bytes.Compare(startKey, endKey) >= 0 {
			return count, nil
		}
		startKey = endKey
	}
}

func checkRegion(region *pd.Region) {
	regionID := region.Meta.GetId()
	key := region.Meta.GetEndKey()
	if len(key) == 0 || hex.EncodeToString(key) == "7800000000000000fb" || hex.EncodeToString(key) == "7800000100000000fb" {
		return
	}
	log.Debug("patrol region",
		zap.Uint64("region_id", regionID),
		zap.String("key", hex.EncodeToString(key)))
	rootNode := N("key", key)
	rootNode.Expand()
	isInvalid := hasInvalidPatternRecursive(rootNode)
	if isInvalid {
		tableID, _, err := extractTableIDRecursive(rootNode)
		if err != nil {
			log.Error("found invalid pattern, but failed to extract table ID",
				zap.Uint64("region_id", regionID),
				zap.String("key", hex.EncodeToString(key)),
				zap.Error(err))
			return
		}
		log.Error("found invalid pattern",
			zap.Uint64("region_id", regionID),
			zap.Int64("table_id", tableID),
			zap.String("key", hex.EncodeToString(key)))
	}
}

// hasInvalidPatternRecursive recursively searches the Node tree for the specific key pattern.
func hasInvalidPatternRecursive(node *Node) bool {
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
								// Condition 1: Does rowDataNode.val (i.e., the row data) end with \x01?
								if !(len(rowDataNode.val) > 0 && rowDataNode.val[len(rowDataNode.val)-1] == '\x01') {
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
		if slices.ContainsFunc(variant.children, hasInvalidPatternRecursive) {
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
