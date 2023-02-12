// Copyright 2023 TiKV Project Authors.
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

package tso

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxMergeTSORequests    = 10000
	defaultTSOProxyTimeout = 3 * time.Second
)

// GrpcServer wraps Server to provide TSO grpc service.
type GrpcServer interface {
	ClusterID() uint64
	IsClosed() bool
	IsLocalRequest(forwardedHost string) bool
	GetTSOAllocatorManager() *AllocatorManager
	GetTSODispatcher() *sync.Map
	CreateTsoForwardStream(client *grpc.ClientConn) (pdpb.PD_TsoClient, context.CancelFunc, error)
	GetDelegateClient(ctx context.Context, forwardedHost string) (*grpc.ClientConn, error)
	ValidateInternalRequest(header *pdpb.RequestHeader, onlyAllowLeader bool) error
	ValidateRequest(header *pdpb.RequestHeader) error
	GetExternalTS() uint64
	SetExternalTS(externalTS uint64) error
}

type tsoRequest struct {
	forwardedHost string
	request       *pdpb.TsoRequest
	stream        pdpb.PD_TsoServer
}

// Tso implements gRPC Server.
func Tso(s GrpcServer, stream pdpb.PD_TsoServer) error {
	var (
		doneCh chan struct{}
		errCh  chan error
	)
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	for {
		// Prevent unnecessary performance overhead of the channel.
		if errCh != nil {
			select {
			case err := <-errCh:
				return errors.WithStack(err)
			default:
			}
		}
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}

		streamCtx := stream.Context()
		forwardedHost := grpcutil.GetForwardedHost(streamCtx)
		if !s.IsLocalRequest(forwardedHost) {
			if errCh == nil {
				doneCh = make(chan struct{})
				defer close(doneCh)
				errCh = make(chan error)
			}
			dispatchTSORequest(ctx, s, &tsoRequest{
				forwardedHost,
				request,
				stream,
			}, forwardedHost, doneCh, errCh)
			continue
		}

		start := time.Now()
		// TSO uses leader lease to determine validity. No need to check leader here.
		if s.IsClosed() {
			return status.Errorf(codes.Unknown, "server not started")
		}
		if request.GetHeader().GetClusterId() != s.ClusterID() {
			return status.Errorf(codes.FailedPrecondition, "mismatch cluster id, need %d but got %d", s.ClusterID(), request.GetHeader().GetClusterId())
		}
		count := request.GetCount()
		ts, err := s.GetTSOAllocatorManager().HandleTSORequest(request.GetDcLocation(), count)
		if err != nil {
			return status.Errorf(codes.Unknown, err.Error())
		}
		tsoHandleDuration.Observe(time.Since(start).Seconds())
		response := &pdpb.TsoResponse{
			Header:    Header(s.ClusterID()),
			Timestamp: &ts,
			Count:     count,
		}
		if err := stream.Send(response); err != nil {
			return errors.WithStack(err)
		}
	}
}

// Only used for the TestLocalAllocatorLeaderChange.
var mockLocalAllocatorLeaderChangeFlag = false

// SyncMaxTS will check whether MaxTS is the biggest one among all Local TSOs is holding when skipCheck is set,
// and write it into all Local TSO Allocators then if it's indeed the biggest one.
func SyncMaxTS(_ context.Context, s GrpcServer, request *pdpb.SyncMaxTSRequest) (*pdpb.SyncMaxTSResponse, error) {
	if err := s.ValidateInternalRequest(request.GetHeader(), true); err != nil {
		return nil, err
	}
	tsoAllocatorManager := s.GetTSOAllocatorManager()
	// There is no dc-location found in this server, return err.
	if tsoAllocatorManager.GetClusterDCLocationsNumber() == 0 {
		return &pdpb.SyncMaxTSResponse{
			Header: WrapErrorToHeader(s.ClusterID(), pdpb.ErrorType_UNKNOWN,
				"empty cluster dc-location found, checker may not work properly"),
		}, nil
	}
	// Get all Local TSO Allocator leaders
	allocatorLeaders, err := tsoAllocatorManager.GetHoldingLocalAllocatorLeaders()
	if err != nil {
		return &pdpb.SyncMaxTSResponse{
			Header: WrapErrorToHeader(s.ClusterID(), pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}
	if !request.GetSkipCheck() {
		var maxLocalTS *pdpb.Timestamp
		syncedDCs := make([]string, 0, len(allocatorLeaders))
		for _, allocator := range allocatorLeaders {
			// No longer leader, just skip here because
			// the global allocator will check if all DCs are handled.
			if !allocator.IsAllocatorLeader() {
				continue
			}
			currentLocalTSO, err := allocator.GetCurrentTSO()
			if err != nil {
				return &pdpb.SyncMaxTSResponse{
					Header: WrapErrorToHeader(s.ClusterID(), pdpb.ErrorType_UNKNOWN, err.Error()),
				}, nil
			}
			if tsoutil.CompareTimestamp(currentLocalTSO, maxLocalTS) > 0 {
				maxLocalTS = currentLocalTSO
			}
			syncedDCs = append(syncedDCs, allocator.GetDCLocation())
		}

		failpoint.Inject("mockLocalAllocatorLeaderChange", func() {
			if !mockLocalAllocatorLeaderChangeFlag {
				maxLocalTS = nil
				request.MaxTs = nil
				mockLocalAllocatorLeaderChangeFlag = true
			}
		})

		if maxLocalTS == nil {
			return &pdpb.SyncMaxTSResponse{
				Header: WrapErrorToHeader(s.ClusterID(), pdpb.ErrorType_UNKNOWN,
					"local tso allocator leaders have changed during the sync, should retry"),
			}, nil
		}
		if request.GetMaxTs() == nil {
			return &pdpb.SyncMaxTSResponse{
				Header: WrapErrorToHeader(s.ClusterID(), pdpb.ErrorType_UNKNOWN,
					"empty maxTS in the request, should retry"),
			}, nil
		}
		// Found a bigger or equal maxLocalTS, return it directly.
		cmpResult := tsoutil.CompareTimestamp(maxLocalTS, request.GetMaxTs())
		if cmpResult >= 0 {
			// Found an equal maxLocalTS, plus 1 to logical part before returning it.
			// For example, we have a Global TSO t1 and a Local TSO t2, they have the
			// same physical and logical parts. After being differentiating with suffix,
			// there will be (t1.logical << suffixNum + 0) < (t2.logical << suffixNum + N),
			// where N is bigger than 0, which will cause a Global TSO fallback than the previous Local TSO.
			if cmpResult == 0 {
				maxLocalTS.Logical += 1
			}
			return &pdpb.SyncMaxTSResponse{
				Header:     Header(s.ClusterID()),
				MaxLocalTs: maxLocalTS,
				SyncedDcs:  syncedDCs,
			}, nil
		}
	}
	syncedDCs := make([]string, 0, len(allocatorLeaders))
	for _, allocator := range allocatorLeaders {
		if !allocator.IsAllocatorLeader() {
			continue
		}
		if err := allocator.WriteTSO(request.GetMaxTs()); err != nil {
			return &pdpb.SyncMaxTSResponse{
				Header: WrapErrorToHeader(s.ClusterID(), pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
		syncedDCs = append(syncedDCs, allocator.GetDCLocation())
	}
	return &pdpb.SyncMaxTSResponse{
		Header:    Header(s.ClusterID()),
		SyncedDcs: syncedDCs,
	}, nil
}

// GetDCLocationInfo gets the dc-location info of the given dc-location from the primary's TSO allocator manager.
func GetDCLocationInfo(ctx context.Context, s GrpcServer, request *pdpb.GetDCLocationInfoRequest) (*pdpb.GetDCLocationInfoResponse, error) {
	var err error
	if err = s.ValidateInternalRequest(request.GetHeader(), false); err != nil {
		return nil, err
	}
	am := s.GetTSOAllocatorManager()
	info, ok := am.GetDCLocationInfo(request.GetDcLocation())
	if !ok {
		am.ClusterDCLocationChecker()
		return &pdpb.GetDCLocationInfoResponse{
			Header: WrapErrorToHeader(s.ClusterID(), pdpb.ErrorType_UNKNOWN,
				fmt.Sprintf("dc-location %s is not found", request.GetDcLocation())),
		}, nil
	}
	resp := &pdpb.GetDCLocationInfoResponse{
		Header: Header(s.ClusterID()),
		Suffix: info.Suffix,
	}
	// Because the number of suffix bits is changing dynamically according to the dc-location number,
	// there is a corner case may cause the Local TSO is not unique while member changing.
	// Example:
	//     t1: xxxxxxxxxxxxxxx1 | 11
	//     t2: xxxxxxxxxxxxxxx | 111
	// So we will force the newly added Local TSO Allocator to have a Global TSO synchronization
	// when it becomes the Local TSO Allocator leader.
	// Please take a look at https://github.com/tikv/pd/issues/3260 for more details.
	if resp.MaxTs, err = am.GetMaxLocalTSO(ctx); err != nil {
		return &pdpb.GetDCLocationInfoResponse{
			Header: WrapErrorToHeader(s.ClusterID(), pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}
	return resp, nil
}

type forwardSetExternalTSRequest func(context.Context, *grpc.ClientConn, *pdpb.SetExternalTimestampRequest) (*pdpb.SetExternalTimestampResponse, error)

// SetExternalTimestamp implements gRPC Server.
func SetExternalTimestamp(ctx context.Context, s GrpcServer, request *pdpb.SetExternalTimestampRequest,
	forward forwardSetExternalTSRequest) (*pdpb.SetExternalTimestampResponse, error) {
	forwardedHost := grpcutil.GetForwardedHost(ctx)
	if !s.IsLocalRequest(forwardedHost) {
		client, err := s.GetDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return forward(ctx, client, request)
	}

	if err := s.ValidateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	timestamp := request.GetTimestamp()
	if err := setExternalTS(s, timestamp); err != nil {
		return &pdpb.SetExternalTimestampResponse{Header: invalidValue(s.ClusterID(), err.Error())}, nil
	}
	log.Debug("set external timestamp",
		zap.Uint64("timestamp", timestamp))
	return &pdpb.SetExternalTimestampResponse{
		Header: Header(s.ClusterID()),
	}, nil
}

type forwardGetExternalTSRequest func(context.Context, *grpc.ClientConn, *pdpb.GetExternalTimestampRequest) (*pdpb.GetExternalTimestampResponse, error)

// GetExternalTimestamp implements gRPC Server.
func GetExternalTimestamp(ctx context.Context, s GrpcServer, request *pdpb.GetExternalTimestampRequest,
	forward forwardGetExternalTSRequest) (*pdpb.GetExternalTimestampResponse, error) {
	forwardedHost := grpcutil.GetForwardedHost(ctx)
	if !s.IsLocalRequest(forwardedHost) {
		client, err := s.GetDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return forward(ctx, client, request)
	}

	if err := s.ValidateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	timestamp := s.GetExternalTS()
	return &pdpb.GetExternalTimestampResponse{
		Header:    Header(s.ClusterID()),
		Timestamp: timestamp,
	}, nil
}

// Header is a helper function to wrapper the response header for the given cluster id
func Header(clusterID uint64) *pdpb.ResponseHeader {
	if clusterID == 0 {
		return WrapErrorToHeader(clusterID, pdpb.ErrorType_NOT_BOOTSTRAPPED, "cluster id is not ready")
	}
	return &pdpb.ResponseHeader{ClusterId: clusterID}
}

// WrapErrorToHeader is a helper function to wrapper the response header for the given cluster id and the error info
func WrapErrorToHeader(clusterID uint64, errorType pdpb.ErrorType, message string) *pdpb.ResponseHeader {
	return errorHeader(
		clusterID,
		&pdpb.Error{
			Type:    errorType,
			Message: message,
		})
}

// ErrorHeader is a helper function to wrapper the response header for the given cluster id and the error
func errorHeader(clusterID uint64, err *pdpb.Error) *pdpb.ResponseHeader {
	return &pdpb.ResponseHeader{
		ClusterId: clusterID,
		Error:     err,
	}
}

func invalidValue(clusterID uint64, msg string) *pdpb.ResponseHeader {
	return errorHeader(
		clusterID,
		&pdpb.Error{
			Type:    pdpb.ErrorType_INVALID_VALUE,
			Message: msg,
		})
}

func dispatchTSORequest(ctx context.Context, s GrpcServer, request *tsoRequest, forwardedHost string, doneCh <-chan struct{}, errCh chan<- error) {
	tsoRequestChInterface, loaded := s.GetTSODispatcher().LoadOrStore(forwardedHost, make(chan *tsoRequest, maxMergeTSORequests))
	if !loaded {
		tsDeadlineCh := make(chan deadline, 1)
		go handleDispatcher(ctx, s, forwardedHost, tsoRequestChInterface.(chan *tsoRequest), tsDeadlineCh, doneCh, errCh)
		go watchTSDeadline(ctx, tsDeadlineCh)
	}
	tsoRequestChInterface.(chan *tsoRequest) <- request
}

func handleDispatcher(ctx context.Context, s GrpcServer, forwardedHost string, tsoRequestCh <-chan *tsoRequest, tsDeadlineCh chan<- deadline, doneCh <-chan struct{}, errCh chan<- error) {
	dispatcherCtx, ctxCancel := context.WithCancel(ctx)
	defer ctxCancel()
	defer s.GetTSODispatcher().Delete(forwardedHost)

	var (
		forwardStream pdpb.PD_TsoClient
		cancel        context.CancelFunc
	)
	client, err := s.GetDelegateClient(ctx, forwardedHost)
	if err != nil {
		goto errHandling
	}
	log.Info("create tso forward stream", zap.String("forwarded-host", forwardedHost))
	forwardStream, cancel, err = s.CreateTsoForwardStream(client)
errHandling:
	if err != nil || forwardStream == nil {
		log.Error("create tso forwarding stream error", zap.String("forwarded-host", forwardedHost), errs.ZapError(errs.ErrGRPCCreateStream, err))
		select {
		case <-dispatcherCtx.Done():
			return
		case _, ok := <-doneCh:
			if !ok {
				return
			}
		case errCh <- err:
			close(errCh)
			return
		}
	}
	defer cancel()

	requests := make([]*tsoRequest, maxMergeTSORequests+1)
	for {
		select {
		case first := <-tsoRequestCh:
			pendingTSOReqCount := len(tsoRequestCh) + 1
			requests[0] = first
			for i := 1; i < pendingTSOReqCount; i++ {
				requests[i] = <-tsoRequestCh
			}
			done := make(chan struct{})
			dl := deadline{
				timer:  time.After(defaultTSOProxyTimeout),
				done:   done,
				cancel: cancel,
			}
			select {
			case tsDeadlineCh <- dl:
			case <-dispatcherCtx.Done():
				return
			}
			err = processTSORequests(s, forwardStream, requests[:pendingTSOReqCount])
			close(done)
			if err != nil {
				log.Error("proxy forward tso error", zap.String("forwarded-host", forwardedHost), errs.ZapError(errs.ErrGRPCSend, err))
				select {
				case <-dispatcherCtx.Done():
					return
				case _, ok := <-doneCh:
					if !ok {
						return
					}
				case errCh <- err:
					close(errCh)
					return
				}
			}
		case <-dispatcherCtx.Done():
			return
		}
	}
}

func processTSORequests(s GrpcServer, forwardStream pdpb.PD_TsoClient, requests []*tsoRequest) error {
	start := time.Now()
	// Merge the requests
	count := uint32(0)
	for _, request := range requests {
		count += request.request.GetCount()
	}
	req := &pdpb.TsoRequest{
		Header: requests[0].request.GetHeader(),
		Count:  count,
		// TODO: support Local TSO proxy forwarding.
		DcLocation: requests[0].request.GetDcLocation(),
	}
	// Send to the leader stream.
	if err := forwardStream.Send(req); err != nil {
		return err
	}
	resp, err := forwardStream.Recv()
	if err != nil {
		return err
	}
	tsoProxyHandleDuration.Observe(time.Since(start).Seconds())
	tsoProxyBatchSize.Observe(float64(count))
	// Split the response
	physical, logical, suffixBits := resp.GetTimestamp().GetPhysical(), resp.GetTimestamp().GetLogical(), resp.GetTimestamp().GetSuffixBits()
	// `logical` is the largest ts's logical part here, we need to do the subtracting before we finish each TSO request.
	// This is different from the logic of client batch, for example, if we have a largest ts whose logical part is 10,
	// count is 5, then the splitting results should be 5 and 10.
	firstLogical := addLogical(logical, -int64(count), suffixBits)
	return finishTSORequest(s, requests, physical, firstLogical, suffixBits)
}

// Because of the suffix, we need to shift the count before we add it to the logical part.
func addLogical(logical, count int64, suffixBits uint32) int64 {
	return logical + count<<suffixBits
}

func finishTSORequest(s GrpcServer, requests []*tsoRequest, physical, firstLogical int64, suffixBits uint32) error {
	countSum := int64(0)
	for i := 0; i < len(requests); i++ {
		count := requests[i].request.GetCount()
		countSum += int64(count)
		response := &pdpb.TsoResponse{
			Header: Header(s.ClusterID()),
			Count:  count,
			Timestamp: &pdpb.Timestamp{
				Physical:   physical,
				Logical:    addLogical(firstLogical, countSum, suffixBits),
				SuffixBits: suffixBits,
			},
		}
		// Send back to the client.
		if err := requests[i].stream.Send(response); err != nil {
			return err
		}
	}
	return nil
}

type deadline struct {
	timer  <-chan time.Time
	done   chan struct{}
	cancel context.CancelFunc
}

func watchTSDeadline(ctx context.Context, tsDeadlineCh <-chan deadline) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for {
		select {
		case d := <-tsDeadlineCh:
			select {
			case <-d.timer:
				log.Error("tso proxy request processing is canceled due to timeout", errs.ZapError(errs.ErrProxyTSOTimeout))
				d.cancel()
			case <-d.done:
				continue
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func getGlobalTS(s GrpcServer) (uint64, error) {
	ts, err := s.GetTSOAllocatorManager().GetGlobalTSO()
	if err != nil {
		return 0, err
	}
	return tsoutil.GenerateTS(ts), nil
}

// SetExternalTS returns external timestamp.
func setExternalTS(s GrpcServer, externalTS uint64) error {
	globalTS, err := getGlobalTS(s)
	if err != nil {
		return err
	}
	if tsoutil.CompareTimestampUint64(externalTS, globalTS) == 1 {
		desc := "the external timestamp should not be larger than global ts"
		log.Error(desc, zap.Uint64("request timestamp", externalTS), zap.Uint64("global ts", globalTS))
		return errors.New(desc)
	}
	currentExternalTS := s.GetExternalTS()
	if tsoutil.CompareTimestampUint64(externalTS, currentExternalTS) != 1 {
		desc := "the external timestamp should be larger than now"
		log.Error(desc, zap.Uint64("request timestamp", externalTS), zap.Uint64("current external timestamp", currentExternalTS))
		return errors.New(desc)
	}
	return s.SetExternalTS(externalTS)
}
