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
	"crypto/tls"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"slices"

	"github.com/docker/go-units"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/codec"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/router"
	pdHttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const msgSize = 16 * units.MiB
const defaultLimit = 8192

var (
	invalidRegions map[uint64]string // regionID -> endKey
)

var (
	pdAddr          = flag.String("pd", "127.0.0.1:2379", "pd address")
	limit           = flag.Int("limit", defaultLimit, "the limit of regions")
	caPath          = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs")
	certPath        = flag.String("cert", "", "path of file that contains X509 certificate in PEM format")
	keyPath         = flag.String("key", "", "path of file that contains X509 key in PEM format")
	logLevel        = flag.String("log-level", "info", "log level (debug, info, warn, error, fatal)")
	logFile         = flag.String("log-file", "", "log file path (empty for stderr)")
	logFormat       = flag.String("log-format", "text", "log format (text or json)")
	enableAutoMerge = flag.Bool("enable-auto-merge", false, "Enable automatic region merge after detecting an invalid key")
)

func main() {
	flag.Parse()

	logCfg := &log.Config{
		Level:  *logLevel,
		Format: *logFormat,
	}
	if *logFile != "" {
		logCfg.File = log.FileLogConfig{Filename: *logFile}
	}
	logger, props, err := log.InitLogger(logCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	log.ReplaceGlobals(logger, props)

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

	pdCli, err := pd.NewClientWithContext(ctx, caller.TestComponent, []string{*pdAddr},
		pd.SecurityOption{
			CAPath:   *caPath,
			CertPath: *certPath,
			KeyPath:  *keyPath,
		},
		opt.WithGRPCDialOptions(grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(msgSize))))
	if err != nil {
		log.Error("failed to create pd client", zap.Error(err))
		os.Exit(1)
	}
	defer pdCli.Close()

	if *limit <= 0 {
		log.Info("limit is negative, which means no limit")
	}

	invalidRegions = make(map[uint64]string)

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

	if *enableAutoMerge {
		if len(invalidRegions) == 0 {
			log.Info("no invalid regions found, skipping automatic merge")
			return
		}
		autoMergeRegions(ctx)
	}
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
		count += len(regions)
		if len(regions) == 0 {
			return count, nil
		}
		endKey := regions[len(regions)-1].Meta.GetEndKey()
		if bytes.Compare(startKey, endKey) >= 0 {
			return count, nil
		}
		startKey = endKey
	}
}

func checkRegion(region *router.Region) {
	// Add a panic recovery to ensure we don't crash the entire program
	defer func() {
		if r := recover(); r != nil {
			regionID := region.Meta.GetId()
			key := region.Meta.GetEndKey()
			hexKeyStr := hex.EncodeToString(key)
			log.Error("recovered from panic in checkRegion",
				zap.Uint64("region_id", regionID),
				zap.String("end_key_hex", hexKeyStr),
				zap.Any("error", r))
		}
	}()

	regionID := region.Meta.GetId()
	key := region.Meta.GetEndKey()
	if len(key) == 0 {
		return
	}
	hexKeyStr := hex.EncodeToString(key)
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
		log.Info("found invalid pattern",
			zap.Uint64("region_id", regionID),
			zap.Int64("table_id", tableID),
			zap.String("key", hex.EncodeToString(key)))
		invalidRegions[regionID] = hexKeyStr
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
								// Condition 1: Does row data end with non \x00?
								// And we only care about the 9 bytes of the row data.
								if !(len(rowDataNode.val) == 9 && rowDataNode.val[len(rowDataNode.val)-1] != '\x00') {
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

func autoMergeRegions(ctx context.Context) {
	pdHttpCli := newPDHttpClient()
	log.Info("starting automatic region merge for invalid regions")
	for regionID, endKeyHex := range invalidRegions {
		rightRegionID, err := getRegionsSiblingByID(ctx, pdHttpCli, regionID, endKeyHex)
		if err != nil {
			log.Error("failed to find right sibling region",
				zap.Uint64("region_id", regionID),
				zap.String("end_key_hex", endKeyHex),
				zap.Error(err))
			continue
		}
		err = pdHttpCli.CreateMergeOperator(ctx, regionID, rightRegionID)
		if err != nil {
			log.Error("failed to create merge request",
				zap.Uint64("region_id", regionID),
				zap.Uint64("right_region_id", rightRegionID),
				zap.Error(err))
			continue
		}
		log.Info("merge request created successfully",
			zap.Uint64("region_id", regionID),
			zap.Uint64("right_region_id", rightRegionID))
	}
}

func getRegionsSiblingByID(ctx context.Context, pdHttpCli pdHttp.Client, regionID uint64, endKeyHex string) (uint64, error) {
	resp, err := pdHttpCli.GetRegionsSiblingByID(ctx, regionID)
	if err != nil {
		return 0, err
	}
	if resp.Count == 0 {
		return 0, fmt.Errorf("no sibling region found for region %d", regionID)
	}
	rigitRegion := resp.Regions[resp.Count-1]
	rightStartKey := strings.ToUpper(rigitRegion.GetStartKey())
	leftEndKey := strings.ToUpper(endKeyHex)
	if strings.Compare(rightStartKey, leftEndKey) == 0 {
		return uint64(rigitRegion.ID), nil
	}
	return 0, fmt.Errorf("right sibling region's start key %s does not match the end key %s", rightStartKey, leftEndKey)
}

func newPDHttpClient() pdHttp.Client {
	var (
		tlsConfig *tls.Config
		err       error
	)
	if *caPath != "" || *certPath != "" || *keyPath != "" {
		tlsInfo := transport.TLSInfo{
			CertFile:      *certPath,
			KeyFile:       *keyPath,
			TrustedCAFile: *caPath,
		}
		tlsConfig, err = tlsInfo.ClientConfig()
		if err != nil {
			log.Error("failed to create TLS config", zap.Error(err))
			os.Exit(1)
		}
	}
	url := ModifyURLScheme(*pdAddr, tlsConfig)
	return pdHttp.NewClient("tools-pd-pattroller", []string{url}, pdHttp.WithTLSConfig(tlsConfig))
}
