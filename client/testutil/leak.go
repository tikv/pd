// Copyright 2022 TiKV Project Authors.
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
	goleak.IgnoreTopFunction("google.golang.org/grpc.(*ccBalancerWrapper).watcher"),
	goleak.IgnoreTopFunction("google.golang.org/grpc.(*ccResolverWrapper).watcher"),
	goleak.IgnoreTopFunction("google.golang.org/grpc.(*addrConn).createTransport"),
	goleak.IgnoreTopFunction("google.golang.org/grpc.(*addrConn).resetTransport"),
	goleak.IgnoreTopFunction("google.golang.org/grpc.(*Server).handleRawConn"),
	goleak.IgnoreTopFunction("google.golang.org/grpc/internal/grpcsync.(*CallbackSerializer).run"),
}
