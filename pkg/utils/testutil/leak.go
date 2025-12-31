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

import "go.uber.org/goleak"

// LeakOptions is used to filter the goroutines.
var LeakOptions = []goleak.Option{
	goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
	goleak.IgnoreTopFunction("google.golang.org/grpc.(*ccBalancerWrapper).watcher"),
	goleak.IgnoreTopFunction("google.golang.org/grpc.(*addrConn).resetTransport"),
	goleak.IgnoreTopFunction("google.golang.org/grpc/internal/grpcsync.(*CallbackSerializer).run"),
	goleak.IgnoreTopFunction("go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop"),
	goleak.IgnoreTopFunction("sync.runtime_notifyListWait"),
	// TODO: remove the below options once we fixed the http connection leak problems
	goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
	goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
	// natefinch/lumberjack#56, It's a goroutine leak bug. Another ignore option PR https://github.com/pingcap/tidb/pull/27405/
	goleak.IgnoreTopFunction("gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun"),
}
