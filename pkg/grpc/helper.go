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

package grpc

import (
	"bytes"
	"fmt"

	"go.uber.org/multierr"
	"go.uber.org/zap"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

// GetRegion implements the GetRegion RPC method.
func GetRegion(rc *core.BasicCluster, request *pdpb.GetRegionRequest) (resp *pdpb.GetRegionResponse, err error) {
	defer func() {
		incRegionRequestCounter("GetRegion", request.Header, resp.Header.Error)
	}()

	if rc == nil {
		return &pdpb.GetRegionResponse{Header: NotBootstrappedHeader()}, nil
	}
	region := rc.GetRegionByKey(request.GetRegionKey())
	if region == nil {
		log.Warn("leader get region nil", zap.String("key", string(request.GetRegionKey())))
		return &pdpb.GetRegionResponse{Header: WrapHeader()}, nil
	}

	var buckets *metapb.Buckets
	if request.GetNeedBuckets() {
		buckets = region.GetBuckets()
	}
	return &pdpb.GetRegionResponse{
		Header:       WrapHeader(),
		Region:       region.GetMeta(),
		Leader:       region.GetLeader(),
		DownPeers:    region.GetDownPeers(),
		PendingPeers: region.GetPendingPeers(),
		Buckets:      buckets,
	}, nil
}

// GetPrevRegion implements gRPC PDServer
func GetPrevRegion(rc *core.BasicCluster, request *pdpb.GetRegionRequest) (resp *pdpb.GetRegionResponse, err error) {
	defer func() {
		incRegionRequestCounter("GetPrevRegion", request.Header, resp.Header.Error)
	}()

	if rc == nil {
		return &pdpb.GetRegionResponse{Header: NotBootstrappedHeader()}, nil
	}

	region := rc.GetPrevRegionByKey(request.GetRegionKey())
	if region == nil {
		return &pdpb.GetRegionResponse{Header: WrapHeader()}, nil
	}
	var buckets *metapb.Buckets
	if request.GetNeedBuckets() {
		buckets = region.GetBuckets()
	}
	return &pdpb.GetRegionResponse{
		Header:       WrapHeader(),
		Region:       region.GetMeta(),
		Leader:       region.GetLeader(),
		DownPeers:    region.GetDownPeers(),
		PendingPeers: region.GetPendingPeers(),
		Buckets:      buckets,
	}, nil
}

// GetRegionByID implements gRPC PDServer.
func GetRegionByID(rc *core.BasicCluster, request *pdpb.GetRegionByIDRequest) (resp *pdpb.GetRegionResponse, err error) {
	defer func() {
		incRegionRequestCounter("GetRegionByID", request.Header, resp.Header.Error)
	}()
	if rc == nil {
		return &pdpb.GetRegionResponse{Header: NotBootstrappedHeader()}, nil
	}
	region := rc.GetRegion(request.GetRegionId())
	if region == nil {
		return &pdpb.GetRegionResponse{Header: WrapHeader()}, nil
	}
	var buckets *metapb.Buckets
	if request.GetNeedBuckets() {
		buckets = region.GetBuckets()
	}
	return &pdpb.GetRegionResponse{
		Header:       WrapHeader(),
		Region:       region.GetMeta(),
		Leader:       region.GetLeader(),
		DownPeers:    region.GetDownPeers(),
		PendingPeers: region.GetPendingPeers(),
		Buckets:      buckets,
	}, nil
}

// ScanRegions implements gRPC PDServer.
// Deprecated: use BatchScanRegions instead.
func ScanRegions(rc *core.BasicCluster, request *pdpb.ScanRegionsRequest) (resp *pdpb.ScanRegionsResponse, err error) {
	defer func() {
		incRegionRequestCounter("ScanRegions", request.Header, resp.Header.Error)
	}()
	if rc == nil {
		return &pdpb.ScanRegionsResponse{Header: NotBootstrappedHeader()}, nil
	}
	regions := rc.ScanRegions(request.GetStartKey(), request.GetEndKey(), int(request.GetLimit()))
	if len(regions) == 0 {
		return &pdpb.ScanRegionsResponse{Header: RegionNotFound()}, nil
	}
	resp = &pdpb.ScanRegionsResponse{Header: WrapHeader()}
	for _, r := range regions {
		leader := r.GetLeader()
		if leader == nil {
			leader = &metapb.Peer{}
		}
		// Set RegionMetas and Leaders to make it compatible with old client.
		resp.RegionMetas = append(resp.RegionMetas, r.GetMeta())
		resp.Leaders = append(resp.Leaders, leader)
		resp.Regions = append(resp.Regions, &pdpb.Region{
			Region:       r.GetMeta(),
			Leader:       leader,
			DownPeers:    r.GetDownPeers(),
			PendingPeers: r.GetPendingPeers(),
		})
	}
	return resp, nil
}

// BatchScanRegions implements gRPC PDServer.
func BatchScanRegions(rc *core.BasicCluster, request *pdpb.BatchScanRegionsRequest) (resp *pdpb.BatchScanRegionsResponse, err error) {
	defer func() {
		incRegionRequestCounter("BatchScanRegions", request.Header, resp.Header.Error)
	}()

	if rc == nil {
		return &pdpb.BatchScanRegionsResponse{Header: NotBootstrappedHeader()}, nil
	}
	needBucket := request.GetNeedBuckets()
	limit := request.GetLimit()
	// cast to keyutil.KeyRanges and check the validation.
	keyRanges := keyutil.NewKeyRangesWithSize(len(request.GetRanges()))
	reqRanges := request.GetRanges()
	for i, reqRange := range reqRanges {
		if i > 0 {
			if bytes.Compare(reqRange.StartKey, reqRanges[i-1].EndKey) < 0 {
				return &pdpb.BatchScanRegionsResponse{Header: WrapErrorToHeader(pdpb.ErrorType_UNKNOWN, "invalid key range, ranges overlapped")}, nil
			}
		}
		if len(reqRange.EndKey) > 0 && bytes.Compare(reqRange.StartKey, reqRange.EndKey) > 0 {
			return &pdpb.BatchScanRegionsResponse{Header: WrapErrorToHeader(pdpb.ErrorType_UNKNOWN, "invalid key range, start key > end key")}, nil
		}
		keyRanges.Append(reqRange.StartKey, reqRange.EndKey)
	}

	scanOptions := []core.BatchScanRegionsOptionFunc{core.WithLimit(int(limit))}
	if request.ContainAllKeyRange {
		scanOptions = append(scanOptions, core.WithOutputMustContainAllKeyRange())
	}
	res, err := rc.BatchScanRegions(keyRanges, scanOptions...)
	if err != nil {
		if errs.ErrRegionNotAdjacent.Equal(multierr.Errors(err)[0]) {
			return &pdpb.BatchScanRegionsResponse{
				Header: WrapErrorToHeader(pdpb.ErrorType_REGIONS_NOT_CONTAIN_ALL_KEY_RANGE, err.Error()),
			}, nil
		}
		return &pdpb.BatchScanRegionsResponse{
			Header: WrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}
	regions := make([]*pdpb.Region, 0, len(res))
	for _, r := range res {
		leader := r.GetLeader()
		if leader == nil {
			leader = &metapb.Peer{}
		}
		var buckets *metapb.Buckets
		if needBucket {
			buckets = r.GetBuckets()
		}
		regions = append(regions, &pdpb.Region{
			Region:       r.GetMeta(),
			Leader:       leader,
			DownPeers:    r.GetDownPeers(),
			PendingPeers: r.GetPendingPeers(),
			Buckets:      buckets,
		})
	}
	if len(regions) == 0 {
		return &pdpb.BatchScanRegionsResponse{Header: RegionNotFound()}, nil
	}
	resp = &pdpb.BatchScanRegionsResponse{Header: WrapHeader(), Regions: regions}
	return resp, nil
}

// QueryRegion provides a stream processing of the region query.
func QueryRegion(rc *core.BasicCluster, request *pdpb.QueryRegionRequest) *pdpb.QueryRegionResponse {
	for {
		needBuckets := request.GetNeedBuckets()
		start := time.Now()
		keyIDMap, prevKeyIDMap, regionsByID := rc.QueryRegions(
			request.GetKeys(),
			request.GetPrevKeys(),
			request.GetIds(),
			needBuckets,
		)
		queryRegionDuration.Observe(time.Since(start).Seconds())
		// Build the response and send it to the client.
		response := &pdpb.QueryRegionResponse{
			Header:       WrapHeader(),
			KeyIdMap:     keyIDMap,
			PrevKeyIdMap: prevKeyIDMap,
			RegionsById:  regionsByID,
		}
		incRegionRequestCounter("QueryRegion", request.Header, response.Header.Error)
		regionRequestCounter.WithLabelValues("QueryRegion", request.Header.CallerId,
			request.Header.CallerComponent, "").Inc()
	}
}

// GetStore implements gRPC PDServer.
func GetStore(rc *core.BasicCluster, request *pdpb.GetStoreRequest) (*pdpb.GetStoreResponse, error) {
	if rc == nil {
		return &pdpb.GetStoreResponse{Header: NotBootstrappedHeader()}, nil
	}

	storeID := request.GetStoreId()
	store := rc.GetStore(storeID)
	if store == nil {
		return &pdpb.GetStoreResponse{
			Header: WrapErrorToHeader(pdpb.ErrorType_UNKNOWN,
				fmt.Sprintf("invalid store ID %d, not found", storeID)),
		}, nil
	}
	return &pdpb.GetStoreResponse{
		Header: WrapHeader(),
		Store:  store.GetMeta(),
		Stats:  store.GetStoreStats(),
	}, nil
}

// GetAllStores implements the GetAllStores RPC method.
func GetAllStores(rc *core.BasicCluster, request *pdpb.GetAllStoresRequest) (*pdpb.GetAllStoresResponse, error) {
	if rc == nil {
		return &pdpb.GetAllStoresResponse{Header: NotBootstrappedHeader()}, nil
	}
	var stores []*metapb.Store
	if request.GetExcludeTombstoneStores() {
		for _, store := range rc.GetMetaStores() {
			if store.GetNodeState() != metapb.NodeState_Removed {
				stores = append(stores, store)
			}
		}
	} else {
		stores = rc.GetMetaStores()
	}
	return &pdpb.GetAllStoresResponse{
		Header: WrapHeader(),
		Stores: stores,
	}, nil
}

// WrapHeader creates a response header with the current cluster ID.
func WrapHeader() *pdpb.ResponseHeader {
	clusterID := keypath.ClusterID()
	if clusterID == 0 {
		return WrapErrorToHeader(pdpb.ErrorType_NOT_BOOTSTRAPPED, "cluster id is not ready")
	}
	return &pdpb.ResponseHeader{ClusterId: clusterID}
}

// WrapErrorToHeader creates a response header with the given error type and message.
func WrapErrorToHeader(errorType pdpb.ErrorType, message string) *pdpb.ResponseHeader {
	return ErrorHeader(&pdpb.Error{
		Type:    errorType,
		Message: message,
	})
}

// ErrorHeader creates a response header with the given error.
func ErrorHeader(err *pdpb.Error) *pdpb.ResponseHeader {
	return &pdpb.ResponseHeader{
		ClusterId: keypath.ClusterID(),
		Error:     err,
	}
}

// NotBootstrappedHeader returns a response header indicating the cluster is not bootstrapped.
func NotBootstrappedHeader() *pdpb.ResponseHeader {
	return ErrorHeader(&pdpb.Error{
		Type:    pdpb.ErrorType_NOT_BOOTSTRAPPED,
		Message: "cluster is not bootstrapped",
	})
}

// RegionNotFound returns a response header indicating the region is not found.
func RegionNotFound() *pdpb.ResponseHeader {
	return ErrorHeader(&pdpb.Error{
		Type:    pdpb.ErrorType_REGION_NOT_FOUND,
		Message: "region not found",
	})
}
