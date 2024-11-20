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

package pd

import (
	"context"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/metrics"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// TSO Stream Builder Factory

type tsoStreamBuilderFactory interface {
	makeBuilder(cc *grpc.ClientConn) tsoStreamBuilder
}

type pdTSOStreamBuilderFactory struct{}

func (*pdTSOStreamBuilderFactory) makeBuilder(cc *grpc.ClientConn) tsoStreamBuilder {
	return &pdTSOStreamBuilder{client: pdpb.NewPDClient(cc), serverURL: cc.Target()}
}

type tsoTSOStreamBuilderFactory struct{}

func (*tsoTSOStreamBuilderFactory) makeBuilder(cc *grpc.ClientConn) tsoStreamBuilder {
	return &tsoTSOStreamBuilder{client: tsopb.NewTSOClient(cc), serverURL: cc.Target()}
}

// TSO Stream Builder

type tsoStreamBuilder interface {
	build(context.Context, context.CancelFunc, time.Duration) (*tsoStream, error)
}

type pdTSOStreamBuilder struct {
	serverURL string
	client    pdpb.PDClient
}

func (b *pdTSOStreamBuilder) build(ctx context.Context, cancel context.CancelFunc, timeout time.Duration) (*tsoStream, error) {
	done := make(chan struct{})
	// TODO: we need to handle a conner case that this goroutine is timeout while the stream is successfully created.
	go checkStreamTimeout(ctx, cancel, done, timeout)
	stream, err := b.client.Tso(ctx)
	done <- struct{}{}
	if err == nil {
		return newTSOStream(ctx, b.serverURL, pdTSOStreamAdapter{stream}), nil
	}
	return nil, err
}

type tsoTSOStreamBuilder struct {
	serverURL string
	client    tsopb.TSOClient
}

func (b *tsoTSOStreamBuilder) build(
	ctx context.Context, cancel context.CancelFunc, timeout time.Duration,
) (*tsoStream, error) {
	done := make(chan struct{})
	// TODO: we need to handle a conner case that this goroutine is timeout while the stream is successfully created.
	go checkStreamTimeout(ctx, cancel, done, timeout)
	stream, err := b.client.Tso(ctx)
	done <- struct{}{}
	if err == nil {
		return newTSOStream(ctx, b.serverURL, tsoTSOStreamAdapter{stream}), nil
	}
	return nil, err
}

func checkStreamTimeout(ctx context.Context, cancel context.CancelFunc, done chan struct{}, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return
	case <-timer.C:
		cancel()
	case <-ctx.Done():
	}
	<-done
}

type tsoRequestResult struct {
	physical, logical   int64
	count               uint32
	respKeyspaceGroupID uint32
}

type grpcTSOStreamAdapter interface {
	Send(clusterID uint64, keyspaceID, keyspaceGroupID uint32, count int64) error
	Recv() (tsoRequestResult, error)
}

type pdTSOStreamAdapter struct {
	stream pdpb.PD_TsoClient
}

// Send implements the grpcTSOStreamAdapter interface.
func (s pdTSOStreamAdapter) Send(clusterID uint64, _, _ uint32, count int64) error {
	req := &pdpb.TsoRequest{
		Header: &pdpb.RequestHeader{
			ClusterId: clusterID,
		},
		Count: uint32(count),
	}
	return s.stream.Send(req)
}

// Recv implements the grpcTSOStreamAdapter interface.
func (s pdTSOStreamAdapter) Recv() (tsoRequestResult, error) {
	resp, err := s.stream.Recv()
	if err != nil {
		return tsoRequestResult{}, err
	}
	return tsoRequestResult{
		physical:            resp.GetTimestamp().GetPhysical(),
		logical:             resp.GetTimestamp().GetLogical(),
		count:               resp.GetCount(),
		respKeyspaceGroupID: defaultKeySpaceGroupID,
	}, nil
}

type tsoTSOStreamAdapter struct {
	stream tsopb.TSO_TsoClient
}

// Send implements the grpcTSOStreamAdapter interface.
func (s tsoTSOStreamAdapter) Send(clusterID uint64, keyspaceID, keyspaceGroupID uint32, count int64) error {
	req := &tsopb.TsoRequest{
		Header: &tsopb.RequestHeader{
			ClusterId:       clusterID,
			KeyspaceId:      keyspaceID,
			KeyspaceGroupId: keyspaceGroupID,
		},
		Count: uint32(count),
	}
	return s.stream.Send(req)
}

// Recv implements the grpcTSOStreamAdapter interface.
func (s tsoTSOStreamAdapter) Recv() (tsoRequestResult, error) {
	resp, err := s.stream.Recv()
	if err != nil {
		return tsoRequestResult{}, err
	}
	return tsoRequestResult{
		physical:            resp.GetTimestamp().GetPhysical(),
		logical:             resp.GetTimestamp().GetLogical(),
		count:               resp.GetCount(),
		respKeyspaceGroupID: resp.GetHeader().GetKeyspaceGroupId(),
	}, nil
}

type onFinishedCallback func(result tsoRequestResult, reqKeyspaceGroupID uint32, err error)

type batchedRequests struct {
	startTime          time.Time
	count              int64
	reqKeyspaceGroupID uint32
	callback           onFinishedCallback
}

// tsoStream represents an abstracted stream for requesting TSO.
// This type designed decoupled with users of this type, so tsoDispatcher won't be directly accessed here.
// Also in order to avoid potential memory allocations that might happen when passing closures as the callback,
// we instead use the `batchedRequestsNotifier` as the abstraction, and accepts generic type instead of dynamic interface
// type.
type tsoStream struct {
	serverURL string
	// The internal gRPC stream.
	//   - `pdpb.PD_TsoClient` for a leader/follower in the PD cluster.
	//   - `tsopb.TSO_TsoClient` for a primary/secondary in the TSO cluster.
	stream grpcTSOStreamAdapter
	// An identifier of the tsoStream object for metrics reporting and diagnosing.
	streamID string

	pendingRequests chan batchedRequests

	cancel context.CancelFunc
	wg     sync.WaitGroup

	// For syncing between sender and receiver to guarantee all requests are finished when closing.
	state          atomic.Int32
	stoppedWithErr atomic.Pointer[error]

	estimatedLatencyMicros atomic.Uint64

	ongoingRequestCountGauge prometheus.Gauge
	ongoingRequests          atomic.Int32
}

const (
	streamStateIdle int32 = iota
	streamStateSending
	streamStateClosing
)

var streamIDAlloc atomic.Int32

const (
	invalidStreamID               = "<invalid>"
	maxPendingRequestsInTSOStream = 64
)

func newTSOStream(ctx context.Context, serverURL string, stream grpcTSOStreamAdapter) *tsoStream {
	streamID := fmt.Sprintf("%s-%d", serverURL, streamIDAlloc.Add(1))
	// To make error handling in `tsoDispatcher` work, the internal `cancel` and external `cancel` is better to be
	// distinguished.
	ctx, cancel := context.WithCancel(ctx)
	s := &tsoStream{
		serverURL: serverURL,
		stream:    stream,
		streamID:  streamID,

		pendingRequests: make(chan batchedRequests, maxPendingRequestsInTSOStream),

		cancel: cancel,

		ongoingRequestCountGauge: metrics.OngoingRequestCountGauge.WithLabelValues(streamID),
	}
	s.wg.Add(1)
	go s.recvLoop(ctx)
	return s
}

func (s *tsoStream) getServerURL() string {
	return s.serverURL
}

// processRequests starts an RPC to get a batch of timestamps without waiting for the result. When the result is ready,
// it will be  passed th `notifier.finish`.
//
// This function is NOT thread-safe. Don't call this function concurrently in multiple goroutines.
//
// It's guaranteed that the `callback` will be called, but when the request is failed to be scheduled, the callback
// will be ignored.
func (s *tsoStream) processRequests(
	clusterID uint64, keyspaceID, keyspaceGroupID uint32, count int64, batchStartTime time.Time, callback onFinishedCallback,
) error {
	start := time.Now()

	// Check if the stream is closing or closed, in which case no more requests should be put in.
	// Note that the prevState should be restored very soon, as the receiver may check
	prevState := s.state.Swap(streamStateSending)
	switch prevState {
	case streamStateIdle:
		// Expected case
	case streamStateClosing:
		s.state.Store(prevState)
		err := s.GetRecvError()
		log.Info("sending to closed tsoStream", zap.String("stream", s.streamID), zap.Error(err))
		if err == nil {
			err = errors.WithStack(errs.ErrClientTSOStreamClosed)
		}
		return err
	case streamStateSending:
		log.Fatal("unexpected concurrent sending on tsoStream", zap.String("stream", s.streamID))
	default:
		log.Fatal("unknown tsoStream state", zap.String("stream", s.streamID), zap.Int32("state", prevState))
	}

	select {
	case s.pendingRequests <- batchedRequests{
		startTime:          start,
		count:              count,
		reqKeyspaceGroupID: keyspaceGroupID,
		callback:           callback,
	}:
	default:
		s.state.Store(prevState)
		return errors.New("unexpected channel full")
	}
	s.state.Store(prevState)

	if err := s.stream.Send(clusterID, keyspaceID, keyspaceGroupID, count); err != nil {
		// As the request is already put into `pendingRequests`, the request should finally be canceled by the recvLoop.
		// So skip returning error here to avoid
		// if err == io.EOF {
		// 	return errors.WithStack(errs.ErrClientTSOStreamClosed)
		// }
		// return errors.WithStack(err)
		log.Warn("failed to send RPC request through tsoStream", zap.String("stream", s.streamID), zap.Error(err))
		return nil
	}
	metrics.TSOBatchSendLatency.Observe(time.Since(batchStartTime).Seconds())
	s.ongoingRequestCountGauge.Set(float64(s.ongoingRequests.Add(1)))
	return nil
}

func (s *tsoStream) recvLoop(ctx context.Context) {
	var finishWithErr error
	var currentReq batchedRequests
	var hasReq bool

	defer func() {
		if r := recover(); r != nil {
			log.Fatal("tsoStream.recvLoop internal panic", zap.Stack("stacktrace"), zap.Any("panicMessage", r))
		}

		if finishWithErr == nil {
			// The loop must exit with a non-nil error (including io.EOF and context.Canceled). This should be
			// unreachable code.
			log.Fatal("tsoStream.recvLoop exited without error info", zap.String("stream", s.streamID))
		}

		if hasReq {
			// There's an unfinished request, cancel it, otherwise it will be blocked forever.
			currentReq.callback(tsoRequestResult{}, currentReq.reqKeyspaceGroupID, finishWithErr)
		}

		s.stoppedWithErr.Store(&finishWithErr)
		s.cancel()
		for !s.state.CompareAndSwap(streamStateIdle, streamStateClosing) {
			switch state := s.state.Load(); state {
			case streamStateIdle, streamStateSending:
				// streamStateSending should switch to streamStateIdle very quickly. Spin until successfully setting to
				// streamStateClosing.
				continue
			case streamStateClosing:
				log.Warn("unexpected double closing of tsoStream", zap.String("stream", s.streamID))
			default:
				log.Fatal("unknown tsoStream state", zap.String("stream", s.streamID), zap.Int32("state", state))
			}
		}

		log.Info("tsoStream.recvLoop ended", zap.String("stream", s.streamID), zap.Error(finishWithErr))

		close(s.pendingRequests)

		// Cancel remaining pending requests.
		for req := range s.pendingRequests {
			req.callback(tsoRequestResult{}, req.reqKeyspaceGroupID, errors.WithStack(finishWithErr))
		}

		s.wg.Done()
		s.ongoingRequests.Store(0)
		s.ongoingRequestCountGauge.Set(0)
	}()

	// For calculating the estimated RPC latency.
	const (
		filterCutoffFreq                float64 = 1.0
		filterNewSampleWeightUpperbound float64 = 0.2
	)
	// The filter applies on logarithm of the latency of each TSO RPC in microseconds.
	filter := newRCFilter(filterCutoffFreq, filterNewSampleWeightUpperbound)

	updateEstimatedLatency := func(sampleTime time.Time, latency time.Duration) {
		if latency < 0 {
			// Unreachable
			return
		}
		currentSample := math.Log(float64(latency.Microseconds()))
		filteredValue := filter.update(sampleTime, currentSample)
		micros := math.Exp(filteredValue)
		s.estimatedLatencyMicros.Store(uint64(micros))
		// Update the metrics in seconds.
		metrics.EstimateTSOLatencyGauge.WithLabelValues(s.streamID).Set(micros * 1e-6)
	}

recvLoop:
	for {
		select {
		case <-ctx.Done():
			finishWithErr = context.Canceled
			break recvLoop
		default:
		}

		res, err := s.stream.Recv()

		// Try to load the corresponding `batchedRequests`. If `Recv` is successful, there must be a request pending
		// in the queue.
		select {
		case currentReq = <-s.pendingRequests:
			hasReq = true
		default:
			hasReq = false
		}

		latency := time.Since(currentReq.startTime)
		latencySeconds := latency.Seconds()

		if err != nil {
			// If a request is pending and error occurs, observe the duration it has cost.
			// Note that it's also possible that the stream is broken due to network without being requested. In this
			// case, `Recv` may return an error while no request is pending.
			if hasReq {
				metrics.RequestFailedDurationTSO.Observe(latencySeconds)
			}
			if err == io.EOF {
				finishWithErr = errors.WithStack(errs.ErrClientTSOStreamClosed)
			} else {
				finishWithErr = errors.WithStack(err)
			}
			break recvLoop
		} else if !hasReq {
			finishWithErr = errors.New("tsoStream timing order broken")
			break recvLoop
		}

		metrics.RequestDurationTSO.Observe(latencySeconds)
		metrics.TSOBatchSize.Observe(float64(res.count))
		updateEstimatedLatency(currentReq.startTime, latency)

		if res.count != uint32(currentReq.count) {
			finishWithErr = errors.WithStack(errTSOLength)
			break recvLoop
		}

		currentReq.callback(res, currentReq.reqKeyspaceGroupID, nil)
		// After finishing the requests, unset these variables which will be checked in the defer block.
		currentReq = batchedRequests{}
		hasReq = false

		s.ongoingRequestCountGauge.Set(float64(s.ongoingRequests.Add(-1)))
	}
}

// EstimatedRPCLatency returns an estimation of the duration of each TSO RPC. If the stream has never handled any RPC,
// this function returns 0.
func (s *tsoStream) EstimatedRPCLatency() time.Duration {
	failpoint.Inject("tsoStreamSimulateEstimatedRPCLatency", func(val failpoint.Value) {
		if s, ok := val.(string); ok {
			duration, err := time.ParseDuration(s)
			if err != nil {
				panic(err)
			}
			failpoint.Return(duration)
		} else {
			panic("invalid failpoint value for `tsoStreamSimulateEstimatedRPCLatency`: expected string")
		}
	})
	latencyUs := s.estimatedLatencyMicros.Load()
	// Limit it at least 100us
	if latencyUs < 100 {
		latencyUs = 100
	}
	return time.Microsecond * time.Duration(latencyUs)
}

// GetRecvError returns the error (if any) that has been encountered when receiving response asynchronously.
func (s *tsoStream) GetRecvError() error {
	perr := s.stoppedWithErr.Load()
	if perr == nil {
		return nil
	}
	return *perr
}

// WaitForClosed blocks until the stream is closed and the inner loop exits.
func (s *tsoStream) WaitForClosed() {
	s.wg.Wait()
}

// rcFilter is a simple implementation of a discrete-time low-pass filter.
// Ref: https://en.wikipedia.org/wiki/Low-pass_filter#Simple_infinite_impulse_response_filter
// There are some differences between this implementation and the wikipedia one:
//   - Time-interval between each two samples is not necessarily a constant. We allow non-even sample interval by simply
//     calculating the alpha (which is calculated by `dt / (rc + dt)`) dynamically for each sample, at the expense of
//     losing some mathematical strictness.
//   - Support specifying the upperbound of the new sample when updating. This can be an approach to avoid the output
//     jumps drastically when the samples come in a low frequency.
type rcFilter struct {
	rc                        float64
	newSampleWeightUpperBound float64
	value                     float64
	lastSampleTime            time.Time
	firstSampleArrived        bool
}

// newRCFilter initializes an rcFilter. `cutoff` is the cutoff frequency in Hertz. `newSampleWeightUpperbound` controls
// the upper limit of the weight of each incoming sample (pass 1 for unlimited).
func newRCFilter(cutoff float64, newSampleWeightUpperBound float64) rcFilter {
	rc := 1.0 / (2.0 * math.Pi * cutoff)
	return rcFilter{
		rc:                        rc,
		newSampleWeightUpperBound: newSampleWeightUpperBound,
	}
}

func (f *rcFilter) update(sampleTime time.Time, newSample float64) float64 {
	// Handle the first sample
	if !f.firstSampleArrived {
		f.firstSampleArrived = true
		f.lastSampleTime = sampleTime
		f.value = newSample
		return newSample
	}

	// Delta time.
	dt := sampleTime.Sub(f.lastSampleTime).Seconds()
	// `alpha` is the weight of the new sample, limited with `newSampleWeightUpperBound`.
	alpha := math.Min(dt/(f.rc+dt), f.newSampleWeightUpperBound)
	f.value = (1-alpha)*f.value + alpha*newSample

	f.lastSampleTime = sampleTime
	return f.value
}
