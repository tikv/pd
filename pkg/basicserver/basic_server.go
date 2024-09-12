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

package server

import (
	"context"
	"net/http"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Server defines the common basic behaviors of a server
type Server interface {
	// Name returns the unique name for this server in the cluster.
	Name() string
	// GetAddr returns the address of the server.
	GetAddr() string
	// Context returns the context of server.
	Context() context.Context
	// Run runs the server.
	Run() error
	// Close closes the server.
	Close()
	// GetLeaderListenUrls gets service endpoints from the leader in election group.
	GetLeaderListenUrls() []string
	// GetClient returns builtin etcd client.
	GetClient() *clientv3.Client
	// GetHTTPClient returns builtin http client.
	GetHTTPClient() *http.Client
	// AddStartCallback adds a callback in the startServer phase.
	AddStartCallback(callbacks ...func())
	// IsServing returns whether the server is the leader, if there is embedded etcd, or the primary otherwise.
	IsServing() bool
	// AddServiceReadyCallback adds callbacks when the server becomes the leader, if there is embedded etcd, or the primary otherwise.
	AddServiceReadyCallback(callbacks ...func(context.Context) error)
}
