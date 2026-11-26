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

package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/errs"
)

const (
	testCheckInterval     = 5 * time.Millisecond
	testNoProgressTimeout = 50 * time.Millisecond
)

// constApplied returns an appliedIndex getter that never advances, simulating a
// hung etcd that makes no apply progress.
func constApplied(v uint64) func() uint64 {
	return func() uint64 { return v }
}

// advancingApplied returns an appliedIndex getter that advances on every call,
// simulating a healthy etcd that keeps applying its raft log.
func advancingApplied() func() uint64 {
	var idx atomic.Uint64
	return func() uint64 { return idx.Add(1) }
}

// constSnapshotting returns a snapshotting getter that always reports v,
// simulating whether etcd is currently receiving or applying a raft snapshot.
func constSnapshotting(v bool) func() bool {
	return func() bool { return v }
}

func TestWaitEtcdReadyProgress(t *testing.T) {
	t.Run("ready immediately", func(t *testing.T) {
		re := require.New(t)
		ready := make(chan struct{})
		close(ready)
		err := waitEtcdReadyProgress(context.Background(), ready, nil, nil, constApplied(0), constSnapshotting(false),
			testCheckInterval, testNoProgressTimeout)
		re.NoError(err)
	})

	t.Run("fails fast on async etcd error", func(t *testing.T) {
		re := require.New(t)
		// etcd reports a fatal startup error before ever becoming ready.
		errCh := make(chan error, 1)
		errCh <- errors.New("boom")
		start := time.Now()
		err := waitEtcdReadyProgress(context.Background(), make(chan struct{}), nil, errCh, constApplied(0), constSnapshotting(false),
			testCheckInterval, testNoProgressTimeout)
		re.Error(err)
		// The real cause is wrapped in ErrStartEtcd, not swallowed by a timeout.
		re.ErrorContains(err, "PD:etcd:ErrStartEtcd")
		re.ErrorContains(err, "boom")
		// It must not wait out the whole no-progress window.
		re.Less(time.Since(start), testNoProgressTimeout)
	})

	t.Run("fails fast when etcd server stops before ready", func(t *testing.T) {
		re := require.New(t)
		// The server terminates before becoming ready without publishing an error
		// on errCh (e.g. an internal EtcdServer.run failure).
		stopped := make(chan struct{})
		close(stopped)
		start := time.Now()
		err := waitEtcdReadyProgress(context.Background(), make(chan struct{}), stopped, nil, constApplied(0), constSnapshotting(false),
			testCheckInterval, testNoProgressTimeout)
		re.Error(err)
		re.ErrorContains(err, "PD:etcd:ErrStartEtcd")
		re.Less(time.Since(start), testNoProgressTimeout)
	})

	t.Run("keeps waiting while applied index advances", func(t *testing.T) {
		re := require.New(t)
		// ready only fires well after the no-progress timeout would have elapsed;
		// a fixed deadline would have killed this, but steady apply progress must
		// keep the wait alive.
		ready := make(chan struct{})
		go func() {
			time.Sleep(4 * testNoProgressTimeout)
			close(ready)
		}()
		start := time.Now()
		err := waitEtcdReadyProgress(context.Background(), ready, nil, nil, advancingApplied(), constSnapshotting(false),
			testCheckInterval, testNoProgressTimeout)
		re.NoError(err)
		// It must have outlived the no-progress window instead of bailing at it.
		re.GreaterOrEqual(time.Since(start), 3*testNoProgressTimeout)
	})

	t.Run("keeps waiting while a raft snapshot is in progress", func(t *testing.T) {
		re := require.New(t)
		// applied index is stuck throughout, as it would be while etcd is
		// receiving or applying an incoming raft snapshot; ready fires well
		// inside the noProgressTimeout snapshot-credit window.
		ready := make(chan struct{})
		go func() {
			time.Sleep(5 * testCheckInterval)
			close(ready)
		}()
		start := time.Now()
		err := waitEtcdReadyProgress(context.Background(), ready, nil, nil, constApplied(0), constSnapshotting(true),
			testCheckInterval, testNoProgressTimeout)
		re.NoError(err)
		re.Less(time.Since(start), testNoProgressTimeout)
	})

	t.Run("gives up when a raft snapshot install itself stalls", func(t *testing.T) {
		re := require.New(t)
		// snapshotting() stays true for the whole wait (as it would for a
		// snapshot install that hangs, e.g. on stuck disk I/O) and applied
		// index never advances. A liveness bit alone must not be treated as
		// progress forever, or EtcdStartTimeout would never fire.
		start := time.Now()
		err := waitEtcdReadyProgress(context.Background(), make(chan struct{}), nil, nil, constApplied(0), constSnapshotting(true),
			testCheckInterval, testNoProgressTimeout)
		re.Error(err)
		re.True(errors.ErrorEqual(err, errs.ErrCancelStartEtcd))
		// It should give up at roughly one noProgressTimeout window, not
		// wait indefinitely or double that window.
		re.GreaterOrEqual(time.Since(start), testNoProgressTimeout-testCheckInterval)
		re.Less(time.Since(start), 2*testNoProgressTimeout)
	})

	t.Run("a snapshot streak ending within budget does not trip a stale timeout", func(t *testing.T) {
		re := require.New(t)
		// snapshotting() is true for a while (within its own credit window),
		// then flips back to false before applied index resumes advancing.
		// Total elapsed time exceeds one noProgressTimeout window, so this
		// would fail if the transition were judged against a lastProgress
		// mark left over from before the streak began instead of being
		// refreshed at the transition.
		var snapshotting atomic.Bool
		snapshotting.Store(true)
		go func() {
			time.Sleep(5 * testCheckInterval)
			snapshotting.Store(false)
		}()
		ready := make(chan struct{})
		go func() {
			time.Sleep(12 * testCheckInterval)
			close(ready)
		}()
		start := time.Now()
		err := waitEtcdReadyProgress(context.Background(), ready, nil, nil, constApplied(0),
			func() bool { return snapshotting.Load() },
			testCheckInterval, testNoProgressTimeout)
		re.NoError(err)
		re.GreaterOrEqual(time.Since(start), testNoProgressTimeout-5*testCheckInterval)
	})

	t.Run("gives up when no apply progress", func(t *testing.T) {
		re := require.New(t)
		// ready never fires and applied index is stuck: a genuine hang.
		start := time.Now()
		err := waitEtcdReadyProgress(context.Background(), make(chan struct{}), nil, nil, constApplied(7), constSnapshotting(false),
			testCheckInterval, testNoProgressTimeout)
		re.Error(err)
		re.True(errors.ErrorEqual(err, errs.ErrCancelStartEtcd))
		// It should wait roughly the whole no-progress window before giving up.
		re.GreaterOrEqual(time.Since(start), testNoProgressTimeout-testCheckInterval)
	})

	t.Run("honors ctx cancellation", func(t *testing.T) {
		re := require.New(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			time.Sleep(2 * testCheckInterval)
			cancel()
		}()
		// applied index advances, so only ctx cancellation can end the wait.
		err := waitEtcdReadyProgress(ctx, make(chan struct{}), nil, nil, advancingApplied(), constSnapshotting(false),
			testCheckInterval, testNoProgressTimeout)
		re.Error(err)
		re.True(errors.ErrorEqual(err, errs.ErrCancelStartEtcd))
	})

	t.Run("does not panic when errCh is closed without an error", func(t *testing.T) {
		re := require.New(t)
		// embed.Etcd.Err() is closed by Etcd.Close(); a receive from a closed
		// channel yields a nil error. Wrap(nil) returns a nil *errors.Error,
		// and calling GenWithStackByCause on that would panic if not guarded.
		errCh := make(chan error)
		close(errCh)
		re.NotPanics(func() {
			err := waitEtcdReadyProgress(context.Background(), make(chan struct{}), nil, errCh, constApplied(0), constSnapshotting(false),
				testCheckInterval, testNoProgressTimeout)
			re.Error(err)
			re.ErrorContains(err, "PD:etcd:ErrStartEtcd")
		})
	})

	t.Run("does not panic when stopped fires and errCh is closed without an error", func(t *testing.T) {
		re := require.New(t)
		stopped := make(chan struct{})
		close(stopped)
		errCh := make(chan error)
		close(errCh)
		re.NotPanics(func() {
			err := waitEtcdReadyProgress(context.Background(), make(chan struct{}), stopped, errCh, constApplied(0), constSnapshotting(false),
				testCheckInterval, testNoProgressTimeout)
			re.Error(err)
			re.ErrorContains(err, "PD:etcd:ErrStartEtcd")
		})
	})
}

// gatherResult is a fixed prometheus.Gatherer stub that returns the given
// metric families and error on every call.
type gatherResult struct {
	mfs []*dto.MetricFamily
	err error
}

func (g gatherResult) Gather() ([]*dto.MetricFamily, error) {
	return g.mfs, g.err
}

// gaugeFamily builds a single-metric GAUGE MetricFamily, mirroring the shape
// etcd's own snapshot gauges take.
func gaugeFamily(name string, value float64) *dto.MetricFamily {
	typ := dto.MetricType_GAUGE
	return &dto.MetricFamily{
		Name: &name,
		Type: &typ,
		Metric: []*dto.Metric{
			{Gauge: &dto.Gauge{Value: &value}},
		},
	}
}

func TestSnapshotGaugesNonzero(t *testing.T) {
	t.Run("true when a snapshot gauge is nonzero", func(t *testing.T) {
		re := require.New(t)
		mfs := []*dto.MetricFamily{
			gaugeFamily("etcd_server_snapshot_apply_in_progress_total", 1),
		}
		re.True(snapshotGaugesNonzero(gatherResult{mfs: mfs}))
	})

	t.Run("false when every snapshot gauge is zero", func(t *testing.T) {
		re := require.New(t)
		mfs := []*dto.MetricFamily{
			gaugeFamily("etcd_network_snapshot_receive_inflights_total", 0),
			gaugeFamily("etcd_server_snapshot_apply_in_progress_total", 0),
			gaugeFamily("some_unrelated_metric", 1),
		}
		re.False(snapshotGaugesNonzero(gatherResult{mfs: mfs}))
	})

	t.Run("false when Gather returns nothing", func(t *testing.T) {
		re := require.New(t)
		re.False(snapshotGaugesNonzero(gatherResult{err: errors.New("boom")}))
	})

	t.Run("still scans a positive gauge alongside an unrelated collection error", func(t *testing.T) {
		re := require.New(t)
		// Registry.Gather can return a non-nil MultiError alongside metric
		// families collected successfully by unaffected collectors; an
		// unrelated collector failing here must not hide a real positive
		// etcd snapshot gauge.
		mfs := []*dto.MetricFamily{
			gaugeFamily("etcd_network_snapshot_receive_inflights_total", 1),
		}
		re.True(snapshotGaugesNonzero(gatherResult{mfs: mfs, err: errors.New("unrelated collector failed")}))
	})
}

func TestIsEtcdSnapshotting(t *testing.T) {
	re := require.New(t)
	// isEtcdSnapshotting reads the process-wide default registerer; nothing
	// in this test process has set either snapshot gauge, so it must read
	// false rather than error or panic.
	re.False(isEtcdSnapshotting())
}
