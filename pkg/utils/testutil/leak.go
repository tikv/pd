// Copyright 2019 TiKV Project Authors.
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
	"fmt"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"
)

/*
support 2 ways to use this package:
1. use LeakOptions to filter the goroutines.
- which is a global variable, so it's not fine-grained, maybe hide some goroutines.
2. use RegisterLeakDetection to register before-and-after code to a test.
- can be more fine-grained than LeakOptions
- we need to add ignore options to interestingGoroutines() if we want to ignore some goroutines
(but we need to give a reason why we ignore it)
*/

// LeakOptions is used to filter the goroutines.
var LeakOptions = []goleak.Option{
	goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
	goleak.IgnoreTopFunction("google.golang.org/grpc.(*ccBalancerWrapper).watcher"),
	goleak.IgnoreTopFunction("google.golang.org/grpc.(*addrConn).resetTransport"),
	goleak.IgnoreTopFunction("go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop"),
	goleak.IgnoreTopFunction("sync.runtime_notifyListWait"),
	// natefinch/lumberjack#56, It's a goroutine leak bug. Another ignore option PR https://github.com/pingcap/tidb/pull/27405/
	goleak.IgnoreTopFunction("gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun"),
}

/*
CheckLeakedGoroutine verifies tests do not leave any leaky
goroutines. It returns true when there are goroutines still
running(leaking) after all tests.

	func TestMain(m *testing.M) {
		testutil.MustTestMainWithLeakDetection(m)
	}

	func TestSample(t *testing.T) {
		testutil.RegisterLeakDetection(t)
		...
	}
*/
func CheckLeakedGoroutine() bool {
	gs := interestingGoroutines()
	if len(gs) == 0 {
		return false
	}

	stackCount := make(map[string]int)
	re := regexp.MustCompile(`\(0[0-9a-fx, ]*\)`)
	for _, g := range gs {
		// strip out pointer arguments in first function of stack dump
		normalized := string(re.ReplaceAll([]byte(g), []byte("(...)")))
		stackCount[normalized]++
	}

	fmt.Fprint(os.Stderr, "Unexpected goroutines running after all test(s).\n")
	for stack, count := range stackCount {
		fmt.Fprintf(os.Stderr, "%d instances of:\n%s\n", count, stack)
	}
	return true
}

// MustTestMainWithLeakDetection expands standard m.Run with leaked
// goroutines detection.
func MustTestMainWithLeakDetection(m *testing.M) {
	v := m.Run()
	if v == 0 {
		http.DefaultTransport.(*http.Transport).CloseIdleConnections()

		CheckAfterTest(1 * time.Second)

		// Let the other goroutines finalize.
		runtime.Gosched()

		if CheckLeakedGoroutine() {
			os.Exit(1)
		}
	}
	os.Exit(v)
}

// RegisterLeakDetection is a convenient way to register before-and-after code to a test.
func RegisterLeakDetection(t *testing.T) {
	t.Cleanup(func() {
		// If test-failed the leaked goroutines list is hidding the real
		// source of problem.
		if err := CheckAfterTest(1 * time.Second); err != nil {
			t.Errorf("Test %v", err)
		}
	})
}

// CheckAfterTest returns an error when find leaked goroutines not in ignore list.
// Waits for go-routines shutdown for 'd'.
func CheckAfterTest(d time.Duration) error {
	http.DefaultTransport.(*http.Transport).CloseIdleConnections()

	var stacks string
	begin := time.Now()
	for time.Since(begin) < d {
		goroutines := interestingGoroutines()
		if len(goroutines) == 0 {
			return nil
		}
		stacks = strings.Join(goroutines, "\n\n")

		// Undesired goroutines found, but goroutines might just still be
		// shutting down, so give it some time.
		runtime.Gosched()
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("appears to have leaked %s", stacks)
}

// Some goroutines are known to leak, so ignore them.
func interestingGoroutines() (gs []string) {
	buf := make([]byte, 2<<20)
	buf = buf[:runtime.Stack(buf, true)]
	for _, g := range strings.Split(string(buf), "\n\n") {
		sl := strings.SplitN(g, "\n", 2)
		if len(sl) != 2 {
			continue
		}
		stack := strings.TrimSpace(sl[1])
		if stack == "" ||
			strings.Contains(stack, "interestingGoroutines") ||
			strings.Contains(stack, "github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain") ||
			strings.Contains(stack, "google.golang.org/grpc.(*ccBalancerWrapper).watcher") ||
			// for TestAllocatorLeader
			strings.Contains(stack, "google.golang.org/grpc.(*addrConn).resetTransport") ||
			strings.Contains(stack, "go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop") ||
			strings.Contains(stack, "sync.runtime_notifyListWait") ||
			strings.Contains(stack, "gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun") ||
			// wait for etcd
			strings.Contains(stack, "go.etcd.io/etcd/pkg/transport.timeoutConn.Read") ||
			strings.Contains(stack, "net/http.(*persistConn).writeLoop") ||
			strings.Contains(stack, "testing.(*T).Run") {
			continue
		}
		gs = append(gs, stack)
	}
	sort.Strings(gs)
	return gs
}
