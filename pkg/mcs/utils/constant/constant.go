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

package constant

import (
	"time"
)

const (
	// RetryInterval is the interval to retry.
	// Note: the interval must be less than the timeout of tidb and tikv, which is 2s by default in tikv.
	RetryInterval = 500 * time.Millisecond

	// TCPNetworkStr is the string of tcp network
	TCPNetworkStr = "tcp"

	// DefaultGRPCGracefulStopTimeout is the default timeout to wait for grpc server to gracefully stop
	DefaultGRPCGracefulStopTimeout = 5 * time.Second
	// DefaultHTTPGracefulShutdownTimeout is the default timeout to wait for http server to gracefully shutdown
	DefaultHTTPGracefulShutdownTimeout = 5 * time.Second
	// DefaultLogFormat is the default log format
	DefaultLogFormat = "text"
	// DefaultLogLevel is the default log level
	DefaultLogLevel = "info"
	// DefaultDisableErrorVerbose is the default value of DisableErrorVerbose
	DefaultDisableErrorVerbose = true
	// DefaultLease is the default value of lease
	DefaultLease = int64(5)
	// TransferPrimaryLeaseMultiplier scales the leader lease to derive the TTL of the
	// expected primary flag written by the `{service}/primary/transfer` API. The flag
	// only needs to outlive the re-election window so the target member can win the
	// campaign; if the target never comes up within this window the flag expires and
	// the cluster falls back to a free election. Tying it to the leader lease keeps the
	// window proportional to how fast leadership turns over.
	//
	// Kept at 1 (rather than a larger margin) on purpose: this multiplier directly
	// bounds the cluster's worst-case unavailable window after a transfer whose target
	// never wins a single campaign (down, unreachable, or stuck) - see TransferPrimary's
	// doc comment. A bigger multiplier buys the target more slack but linearly extends
	// that worst case; 1 lease already matches how long the cluster tolerates a primary
	// being unreachable everywhere else (losing its own leader lease), so there is no
	// reason for a transfer-induced outage to be allowed to run longer than that.
	TransferPrimaryLeaseMultiplier = int64(1)
	// MinConsecutiveReadFailuresForCampaign is how many expected-primary-flag reads
	// must fail in a row before an election loop falls back to campaigning without
	// having read the marker. A short run of failures is usually a transient blip;
	// requiring several in a row avoids escalating every glitch into a full guarded
	// campaign (a lease grant plus a txn commit, heavier than the read that just
	// failed) - this matters most where a node runs many independent election loops
	// (one per TSO keyspace group, up to MaxKeyspaceGroupCountInUse of them), where
	// escalating on every failed read can turn a real etcd degradation into a
	// fleet-wide write storm against the same already-struggling etcd.
	MinConsecutiveReadFailuresForCampaign = 3
	// InitialReadFailureBackoff and MaxReadFailureBackoff bound the backoff an
	// election loop sleeps after a run of expected-primary-flag read failures: it
	// doubles from the initial value and caps at the max, so a sustained run of
	// failures backs off instead of retrying at a fixed rate against an etcd that is
	// already struggling.
	InitialReadFailureBackoff = 200 * time.Millisecond
	MaxReadFailureBackoff     = 6400 * time.Millisecond
	// PrimaryTickInterval is the interval to check primary
	PrimaryTickInterval = 50 * time.Millisecond
	// LeaderTickInterval is the interval to check leader
	LeaderTickInterval = 50 * time.Millisecond

	// MicroserviceRootPath is the root path of microservice in etcd.
	MicroserviceRootPath = "/ms"
	// PDServiceName is the name of pd server.
	PDServiceName = "pd"
	// TSOServiceName is the name of tso server.
	TSOServiceName = "tso"
	// ResourceManagerServiceName is the name of resource manager server.
	ResourceManagerServiceName = "resource_manager"
	// SchedulingServiceName is the name of scheduling server.
	SchedulingServiceName = "scheduling"
	// RouterServiceName is the name of router server.
	RouterServiceName = "router"

	// MaxKeyspaceGroupCount is the max count of keyspace groups. keyspace group in tso
	// is the sharding unit, i.e., by the definition here, the max count of the shards
	// that we support is MaxKeyspaceGroupCount. The keyspace group id is in the range
	// [0, 99999], which explains we use five-digits number (%05d) to render the keyspace
	// group id in the storage endpoint path.
	MaxKeyspaceGroupCount = uint32(100000)
	// MaxKeyspaceGroupCountInUse is the max count of keyspace groups in use, which should
	// never exceed MaxKeyspaceGroupCount defined above. Compared to MaxKeyspaceGroupCount,
	// MaxKeyspaceGroupCountInUse is a much more reasonable value of the max count in the
	// foreseen future, and the former is just for extensibility in theory.
	MaxKeyspaceGroupCountInUse = uint32(4096)

	// DefaultKeyspaceGroupReplicaCount is the default replica count of keyspace group.
	DefaultKeyspaceGroupReplicaCount = 2

	// DefaultKeyspaceGroupReplicaPriority is the default priority of a keyspace group replica.
	// It's used in keyspace group primary weighted-election to balance primaries' distribution.
	// Among multiple replicas of a keyspace group, the higher the priority, the more likely
	// the replica is to be elected as primary.
	DefaultKeyspaceGroupReplicaPriority = 0
)
