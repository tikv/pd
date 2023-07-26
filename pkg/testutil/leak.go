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
// See the License for the specific language governing permissions and
// limitations under the License.

package testutil

import "go.uber.org/goleak"

// LeakOptions is used to filter the goroutines.
var LeakOptions = []goleak.Option{
	goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
	goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).tCompaction"),
	goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb/util.(*BufferPool).drain"),
	goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mCompaction"),
	goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).compactionError"),
	goleak.IgnoreTopFunction("google.golang.org/grpc.(*ccBalancerWrapper).watcher"),
	goleak.IgnoreTopFunction("google.golang.org/grpc.(*ccResolverWrapper).watcher"),
	goleak.IgnoreTopFunction("google.golang.org/grpc.(*addrConn).createTransport"),
	goleak.IgnoreTopFunction("google.golang.org/grpc.(*addrConn).resetTransport"),
	goleak.IgnoreTopFunction("google.golang.org/grpc.(*Server).handleRawConn"),
	goleak.IgnoreTopFunction("go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop"),
	// TODO: remove the below options once we fixed the http connection leak problems
	goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
	goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
	goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
	goleak.IgnoreTopFunction("runtime.goparkunlock"),
}
