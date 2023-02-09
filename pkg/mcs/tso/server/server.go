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

	basicsvr "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/member"
	"go.etcd.io/etcd/clientv3"
)

// If server doesn't implement all methods of basicsvr.Server, this line will result in a clear
// error message like "*Server does not implement basicsvr.Server (missing Method method)"
var _ basicsvr.Server = (*Server)(nil)

// Server is the TSO server, and it implements basicsvr.Server.
type Server struct {
	ctx    context.Context
	name   string
	client *clientv3.Client
	member *member.Member
	// Callback functions for different stages
	// startCallbacks will be called after the server is started.
	startCallbacks []func()
	// leaderCallbacks will be called after the server becomes leader.
	leaderCallbacks []func(context.Context)
}

// NewServer creates a new TSO server.
func NewServer(ctx context.Context, client *clientv3.Client) *Server {
	return &Server{
		ctx:    ctx,
		name:   "TSO",
		client: client,
	}
}

// TODO: Implement the following methods defined in bs.Server

// Name returns the unique etcd Name for this server in etcd cluster.
func (s *Server) Name() string {
	return s.name
}

// Context returns the context of server.
func (s *Server) Context() context.Context {
	return s.ctx
}

// Run runs the pd server.
func (s *Server) Run() error {
	return nil
}

// Close closes the server.
func (s *Server) Close() {
}

// GetClient returns builtin etcd client.
func (s *Server) GetClient() *clientv3.Client {
	return s.client
}

// GetHTTPClient returns builtin http client.
func (s *Server) GetHTTPClient() *http.Client {
	return nil
}

// AddStartCallback adds a callback in the startServer phase.
func (s *Server) AddStartCallback(callbacks ...func()) {
	s.startCallbacks = append(s.startCallbacks, callbacks...)
}

// GetMember returns the member.
func (s *Server) GetMember() *member.Member {
	return s.member
}

// AddLeaderCallback adds the callback function when the server becomes leader.
func (s *Server) AddLeaderCallback(callbacks ...func(context.Context)) {
	s.leaderCallbacks = append(s.leaderCallbacks, callbacks...)
}
