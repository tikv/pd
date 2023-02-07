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

package basicsvr

import (
	"context"
	"net/http"

	"github.com/tikv/pd/pkg/member"
	"go.etcd.io/etcd/clientv3"
)

// Server defines the common basic behaviors of a server
type Server interface {
	// Name returns the unique Name for this server in the cluster.
	Name() string
	// Context returns the context of server.
	Context() context.Context
	// Run runs the server.
	Run() error
	// Close closes the server.
	Close()
	// GetClient returns builtin etcd client.
	GetClient() *clientv3.Client
	// GetHTTPClient returns builtin etcd client.
	GetHTTPClient() *http.Client
	// AddStartCallback adds a callback in the startServer phase.
	AddStartCallback(callbacks ...func())
	// GetMember returns the member information.
	GetMember() *member.Member
	// AddLeaderCallback adds a callback in the leader campaign phase.
	AddLeaderCallback(callbacks ...func(context.Context))
}
