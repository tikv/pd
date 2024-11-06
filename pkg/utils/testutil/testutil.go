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
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

// Eventually asserts that given condition will be met in a period of time.
func Eventually(re *require.Assertions, condition func() bool, opts ...WaitOption) {
	option := &WaitOp{
		waitFor:      defaultWaitFor,
		tickInterval: defaultTickInterval,
	}
	for _, opt := range opts {
		opt(option)
	}
	re.Eventually(
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
func MustNewGrpcClient(re *require.Assertions, addr string) pdpb.PDClient {
	conn, err := grpc.Dial(strings.TrimPrefix(addr, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	re.NoError(err)
	return pdpb.NewPDClient(conn)
}

// CleanServer is used to clean data directory.
func CleanServer(dataDir string) {
	// Clean data directory
	os.RemoveAll(dataDir)
}

// InitTempFileLogger initializes the logger and redirects the log output to a temporary file.
func InitTempFileLogger(level string) (fname string) {
	cfg := &log.Config{}
	f, _ := os.CreateTemp("", "pd_tests")
	fname = f.Name()
	f.Close()
	cfg.File.Filename = fname
	cfg.Level = level
	lg, p, _ := log.InitLogger(cfg)
	log.ReplaceGlobals(lg, p)
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
