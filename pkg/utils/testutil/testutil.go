// Copyright 2016 TiKV Project Authors.
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

package testutil

import (
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
)

const (
	defaultWaitFor      = time.Second * 20
	defaultTickInterval = time.Millisecond * 100
)

// CleanupFunc closes test pd server(s) and deletes any files left behind.
type CleanupFunc func()

// WaitOp represents available options when execute Eventually.
type WaitOp struct {
	waitFor      time.Duration
	tickInterval time.Duration
}

// WaitOption configures WaitOp.
type WaitOption func(op *WaitOp)

// WithWaitFor specify the max wait duration.
func WithWaitFor(waitFor time.Duration) WaitOption {
	return func(op *WaitOp) { op.waitFor = waitFor }
}

// WithTickInterval specify the tick interval to check the condition.
func WithTickInterval(tickInterval time.Duration) WaitOption {
	return func(op *WaitOp) { op.tickInterval = tickInterval }
}

func newWaitOp(opts ...WaitOption) *WaitOp {
	option := &WaitOp{
		waitFor:      defaultWaitFor,
		tickInterval: defaultTickInterval,
	}
	for _, opt := range opts {
		opt(option)
	}
	return option
}

// Eventually asserts that given condition will be met in a period of time.
func Eventually(re *require.Assertions, condition func() bool, opts ...WaitOption) {
	option := newWaitOp(opts...)
	re.Eventually(
		condition,
		option.waitFor,
		option.tickInterval,
	)
}

// EventuallyWithAssert checks that the condition is met without stopping the calling goroutine.
func EventuallyWithAssert(as *assert.Assertions, condition func() bool, opts ...WaitOption) bool {
	option := newWaitOp(opts...)
	return as.Eventually(
		condition,
		option.waitFor,
		option.tickInterval,
	)
}

// NewRequestHeader creates a new request header.
func NewRequestHeader(clusterID uint64) *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: clusterID,
	}
}

// MustNewGrpcClient must create a new PD grpc client.
func MustNewGrpcClient(re *require.Assertions, addr string) (pdpb.PDClient, *grpc.ClientConn) {
	// TODO: use grpc.NewClient instead of grpc.Dial.
	//nolint:staticcheck
	conn, err := grpc.Dial(strings.TrimPrefix(addr, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	re.NoError(err)
	return pdpb.NewPDClient(conn), conn
}

// CleanServer is used to clean data directory.
func CleanServer(dataDir string) {
	// Clean data directory
	os.RemoveAll(dataDir)
}

// InitTempFileLogger initializes the logger and redirects the log output to a temporary file.
func InitTempFileLogger(t testing.TB, level string) (fname string) {
	t.Helper()
	cfg := &log.Config{}
	f, err := os.CreateTemp(t.TempDir(), "pd_tests")
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Errorf("close temporary file logger: %v", err)
		}
	})
	fname = f.Name()
	cfg.File.Filename = fname
	cfg.Level = level
	output := zapcore.AddSync(f)
	lg, p, err := log.InitLoggerWithWriteSyncer(cfg, output, output)
	require.NoError(t, err)
	restoreLogger := log.ReplaceGlobals(lg, p)
	t.Cleanup(func() {
		restoreLogger()
		if err := lg.Sync(); err != nil {
			t.Errorf("sync temporary file logger: %v", err)
		}
	})
	return fname
}

// GenerateTestDataConcurrently generates test data concurrently.
func GenerateTestDataConcurrently(count int, f func(int)) {
	var wg sync.WaitGroup
	tasks := make(chan int, count)
	workers := runtime.NumCPU()
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range tasks {
				f(i)
			}
		}()
	}
	for i := range count {
		tasks <- i
	}
	close(tasks)
	wg.Wait()
}
