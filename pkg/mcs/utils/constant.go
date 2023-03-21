// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import "time"

const (
	// TCPNetworkStr is the string of tcp network
	TCPNetworkStr = "tcp"

	// DefaultEnableGRPCGateway is the default value of EnableGRPCGateway
	DefaultEnableGRPCGateway = true
	// DefaultGRPCGracefulStopTimeout is the default timeout to wait for grpc server to gracefully stop
	DefaultGRPCGracefulStopTimeout = 5 * time.Second
	// DefaultHTTPGracefulShutdownTimeout is the default timeout to wait for http server to gracefully shutdown
	DefaultHTTPGracefulShutdownTimeout = 5 * time.Second
	// DefaultLogFormat is the default log format
	DefaultLogFormat = "text"
	// DefaultDisableErrorVerbose is the default value of DisableErrorVerbose
	DefaultDisableErrorVerbose = true
	// DefaultLeaderLease is the default value of LeaderLease
	DefaultLeaderLease = 3
	// LeaderTickInterval is the interval to check leader
	LeaderTickInterval = 50 * time.Millisecond

	// DefaultKeyspaceID is the default key space id.
	// Valid keyspace id range is [0, 0xFFFFFF](uint24max, or 16777215)
	// ​0 is reserved for default keyspace with the name "DEFAULT", It's initialized when PD bootstrap and reserved for users who haven't been assigned keyspace.
	DefaultKeyspaceID = uint32(0)

	// DefaultKeySpaceGroupID is the default key space group id.
	// We also reserved 0 for the keyspace group for the same purpose.
	DefaultKeySpaceGroupID = 0

	// APIServiceName is the name of api server.
	APIServiceName = "api"
	// TSOServiceName is the name of tso server.
	TSOServiceName = "tso"
	// ResourceManagerServiceName is the name of resource manager server.
	ResourceManagerServiceName = "resource_manager"
)
