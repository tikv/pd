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
	"strings"
	"time"

	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.uber.org/goleak"
)

const etcdTimeoutConnRead = "go.etcd.io/etcd/client/pkg/v3/transport.timeoutConn.Read"

// LeakOptions is used to filter goroutines that cannot be synchronously
// stopped by their owning dependencies.
var LeakOptions = []goleak.Option{
	// leveldb.DB.Close does not wait for mpoolDrain, which exits at most one
	// second later after draining the memory pool.
	goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
	// The dashboard dependency does not close the listeners owned by its TiDB
	// forwarder when the service stops. Keep this filter specific to that known
	// dependency instead of hiding every goroutine blocked in runtime_pollWait.
	goleak.IgnoreAnyFunction("github.com/pingcap/tidb-dashboard/pkg/tidb.(*proxy).run"),
	// lumberjack v2 never closes millCh, including from Logger.Close, so the
	// rotation worker cannot be stopped by callers.
	goleak.IgnoreTopFunction("gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun"),
}

type etcdWaitingTestMain struct {
	goleak.TestingM
}

// Run executes the wrapped test main and waits for transient etcd connections.
func (m etcdWaitingTestMain) Run() int {
	exitCode := m.TestingM.Run()
	if exitCode != 0 {
		return exitCode
	}
	if err := goleak.Find(LeakOptions...); err != nil && strings.Contains(err.Error(), etcdTimeoutConnRead) {
		time.Sleep(rafthttp.ConnReadTimeout)
	}
	return exitCode
}

// WaitForEtcdConnections wraps a test main and waits for transient etcd peer
// HTTP connections to reach their read deadline before goleak verifies them.
func WaitForEtcdConnections(m goleak.TestingM) goleak.TestingM {
	return etcdWaitingTestMain{TestingM: m}
}
