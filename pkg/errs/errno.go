// Copyright 2020 PingCAP, Inc.
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

package errs

import "github.com/pingcap/errors"

const (
	// NotLeaderErr indicates the non-leader member received the requests which should be received by leader.
	// Note: keep the same as the ones defined on the client side, because the client side checks if an error message
	// contains this string to judge whether the leader is changed.
	NotLeaderErr = "is not leader"
	// MismatchLeaderErr indicates the non-leader member received the requests which should be received by leader.
	// Note: keep the same as the ones defined on the client side, because the client side checks if an error message
	// contains this string to judge whether the leader is changed.
	MismatchLeaderErr = "mismatch leader id"
	// NotServedErr indicates an tso node/pod received the requests for the keyspace groups which are not served by it.
	// Note: keep the same as the ones defined on the client side, because the client side checks if an error message
	// contains this string to judge whether the leader is changed.
	NotServedErr = "is not served"
)

// common error in multiple packages
var (
	ErrGetSourceStore      = errors.Normalize("failed to get the source store", errors.RFCCodeText("PD:common:ErrGetSourceStore"))
	ErrGetTargetStore      = errors.Normalize("failed to get the target store", errors.RFCCodeText("PD:common:ErrGetTargetStore"))
	ErrIncorrectSystemTime = errors.Normalize("incorrect system time", errors.RFCCodeText("PD:common:ErrIncorrectSystemTime"))
)

// tso errors
var (
	ErrGetAllocator                     = errors.Normalize("get allocator failed, %s", errors.RFCCodeText("PD:tso:ErrGetAllocator"))
	ErrGetLocalAllocator                = errors.Normalize("get local allocator failed, %s", errors.RFCCodeText("PD:tso:ErrGetLocalAllocator"))
	ErrResetUserTimestamp               = errors.Normalize("reset user timestamp failed, %s", errors.RFCCodeText("PD:tso:ErrResetUserTimestamp"))
	ErrGenerateTimestamp                = errors.Normalize("generate timestamp failed, %s", errors.RFCCodeText("PD:tso:ErrGenerateTimestamp"))
	ErrUpdateTimestamp                  = errors.Normalize("update timestamp failed, %s", errors.RFCCodeText("PD:tso:ErrUpdateTimestamp"))
	ErrLogicOverflow                    = errors.Normalize("logic part overflow", errors.RFCCodeText("PD:tso:ErrLogicOverflow"))
	ErrProxyTSOTimeout                  = errors.Normalize("proxy tso timeout", errors.RFCCodeText("PD:tso:ErrProxyTSOTimeout"))
	ErrKeyspaceGroupIDInvalid           = errors.Normalize("the keyspace group id is invalid, %s", errors.RFCCodeText("PD:tso:ErrKeyspaceGroupIDInvalid"))
	ErrGetAllocatorManager              = errors.Normalize("get allocator manager failed, %s", errors.RFCCodeText("PD:tso:ErrGetAllocatorManager"))
	ErrLoadKeyspaceGroupsTimeout        = errors.Normalize("load keyspace groups timeout", errors.RFCCodeText("PD:tso:ErrLoadKeyspaceGroupsTimeout"))
	ErrLoadKeyspaceGroupsTerminated     = errors.Normalize("load keyspace groups terminated", errors.RFCCodeText("PD:tso:ErrLoadKeyspaceGroupsTerminated"))
	ErrLoadKeyspaceGroupsRetryExhausted = errors.Normalize("load keyspace groups retry exhausted, %s", errors.RFCCodeText("PD:tso:ErrLoadKeyspaceGroupsRetryExhausted"))
	ErrKeyspaceGroupNotInitialized      = errors.Normalize("the keyspace group %d isn't initialized", errors.RFCCodeText("PD:tso:ErrKeyspaceGroupNotInitialized"))
	ErrKeyspaceNotAssigned              = errors.Normalize("the keyspace %d isn't assigned to any keyspace group", errors.RFCCodeText("PD:tso:ErrKeyspaceNotAssigned"))
	ErrGetMinTS                         = errors.Normalize("get min ts failed, %s", errors.RFCCodeText("PD:tso:ErrGetMinTS"))
	ErrKeyspaceGroupIsMerging           = errors.Normalize("the keyspace group %d is merging", errors.RFCCodeText("PD:tso:ErrKeyspaceGroupIsMerging"))
)

// member errors
var (
	ErrEtcdLeaderNotFound = errors.Normalize("etcd leader not found", errors.RFCCodeText("PD:member:ErrEtcdLeaderNotFound"))
	ErrMarshalLeader      = errors.Normalize("marshal leader failed", errors.RFCCodeText("PD:member:ErrMarshalLeader"))
	ErrCheckCampaign      = errors.Normalize("check campaign failed", errors.RFCCodeText("PD:member:ErrCheckCampaign"))
)

// core errors
var (
	ErrWrongRangeKeys         = errors.Normalize("wrong range keys", errors.RFCCodeText("PD:core:ErrWrongRangeKeys"))
	ErrStoreNotFound          = errors.Normalize("store %v not found", errors.RFCCodeText("PD:core:ErrStoreNotFound"))
	ErrPauseLeaderTransferIn  = errors.Normalize("store %v is paused for leader transfer in", errors.RFCCodeText("PD:core:ErrPauseLeaderTransferIn"))
	ErrPauseLeaderTransferOut = errors.Normalize("store %v is paused for leader transfer out", errors.RFCCodeText("PD:core:ErrPauseLeaderTransferOut"))
	ErrStoreRemoved           = errors.Normalize("store %v has been removed", errors.RFCCodeText("PD:core:ErrStoreRemoved"))
	ErrStoreDestroyed         = errors.Normalize("store %v has been physically destroyed", errors.RFCCodeText("PD:core:ErrStoreDestroyed"))
	ErrStoreUnhealthy         = errors.Normalize("store %v is unhealthy", errors.RFCCodeText("PD:core:ErrStoreUnhealthy"))
	ErrStoreServing           = errors.Normalize("store %v has been serving", errors.RFCCodeText("PD:core:ErrStoreServing"))
	ErrSlowStoreEvicted       = errors.Normalize("store %v is evicted as a slow store", errors.RFCCodeText("PD:core:ErrSlowStoreEvicted"))
	ErrSlowTrendEvicted       = errors.Normalize("store %v is evicted as a slow store by trend", errors.RFCCodeText("PD:core:ErrSlowTrendEvicted"))
	ErrStoresNotEnough        = errors.Normalize("can not remove store %v since the number of up stores would be %v while need %v", errors.RFCCodeText("PD:core:ErrStoresNotEnough"))
	ErrNoStoreForRegionLeader = errors.Normalize("can not remove store %d since there are no extra up store to store the leader", errors.RFCCodeText("PD:core:ErrNoStoreForRegionLeader"))
)

// client errors
var (
	ErrClientCreateTSOStream = errors.Normalize("create TSO stream failed, %s", errors.RFCCodeText("PD:client:ErrClientCreateTSOStream"))
	ErrClientGetTSOTimeout   = errors.Normalize("get TSO timeout", errors.RFCCodeText("PD:client:ErrClientGetTSOTimeout"))
	ErrClientGetTSO          = errors.Normalize("get TSO failed", errors.RFCCodeText("PD:client:ErrClientGetTSO"))
	ErrClientGetLeader       = errors.Normalize("get leader failed, %v", errors.RFCCodeText("PD:client:ErrClientGetLeader"))
	ErrClientGetMember       = errors.Normalize("get member failed", errors.RFCCodeText("PD:client:ErrClientGetMember"))
	ErrClientGetMinTSO       = errors.Normalize("get min TSO failed, %v", errors.RFCCodeText("PD:client:ErrClientGetMinTSO"))
)

// operator errors
var (
	// ErrOperatorNotFound is error info for operator not found.
	ErrOperatorNotFound = errors.Normalize("operator not found", errors.RFCCodeText("PD:operator:ErrOperatorNotFound"))
	// ErrAddOperator is error info for already have an operator when adding operator.
	ErrAddOperator = errors.Normalize("failed to add operator, maybe already have one", errors.RFCCodeText("PD:operator:ErrAddOperator"))
)

// region errors
var (
	// ErrRegionInvalidID is error info for region id invalid.
	ErrRegionInvalidID = errors.Normalize("invalid region id", errors.RFCCodeText("PD:region:ErrRegionInvalidID"))
	// ErrRegionNotAdjacent is error info for region not adjacent.
	ErrRegionNotAdjacent = errors.Normalize("two regions are not adjacent", errors.RFCCodeText("PD:region:ErrRegionNotAdjacent"))
	// ErrRegionNotFound is error info for region not found.
	ErrRegionNotFound = errors.Normalize("region %v not found", errors.RFCCodeText("PD:region:ErrRegionNotFound"))
	// ErrRegionAbnormalPeer is error info for region has abnormal peer.
	ErrRegionAbnormalPeer = errors.Normalize("region %v has abnormal peer", errors.RFCCodeText("PD:region:ErrRegionAbnormalPeer"))
)

// plugin errors
var (
	// ErrPluginNotFound is error info for plugin not found.
	ErrPluginNotFound = errors.Normalize("plugin is not found: %s", errors.RFCCodeText("PD:operator:ErrPluginNotFound"))
)

// schedule errors
var (
	ErrUnexpectedOperatorStatus = errors.Normalize("operator with unexpected status", errors.RFCCodeText("PD:schedule:ErrUnexpectedOperatorStatus"))
	ErrUnknownOperatorStep      = errors.Normalize("unknown operator step found", errors.RFCCodeText("PD:schedule:ErrUnknownOperatorStep"))
	ErrMergeOperator            = errors.Normalize("merge operator error, %s", errors.RFCCodeText("PD:schedule:ErrMergeOperator"))
	ErrCreateOperator           = errors.Normalize("unable to create operator, %s", errors.RFCCodeText("PD:schedule:ErrCreateOperator"))
)

// scheduler errors
var (
	ErrSchedulerExisted                 = errors.Normalize("scheduler existed", errors.RFCCodeText("PD:scheduler:ErrSchedulerExisted"))
	ErrSchedulerDuplicated              = errors.Normalize("scheduler duplicated", errors.RFCCodeText("PD:scheduler:ErrSchedulerDuplicated"))
	ErrSchedulerNotFound                = errors.Normalize("scheduler not found", errors.RFCCodeText("PD:scheduler:ErrSchedulerNotFound"))
	ErrScheduleConfigNotExist           = errors.Normalize("the config does not exist", errors.RFCCodeText("PD:scheduler:ErrScheduleConfigNotExist"))
	ErrSchedulerConfig                  = errors.Normalize("wrong scheduler config %s", errors.RFCCodeText("PD:scheduler:ErrSchedulerConfig"))
	ErrCacheOverflow                    = errors.Normalize("cache overflow", errors.RFCCodeText("PD:scheduler:ErrCacheOverflow"))
	ErrInternalGrowth                   = errors.Normalize("unknown interval growth type error", errors.RFCCodeText("PD:scheduler:ErrInternalGrowth"))
	ErrSchedulerCreateFuncNotRegistered = errors.Normalize("create func of %v is not registered", errors.RFCCodeText("PD:scheduler:ErrSchedulerCreateFuncNotRegistered"))
	ErrSchedulerTiKVSplitDisabled       = errors.Normalize("tikv split region disabled", errors.RFCCodeText("PD:scheduler:ErrSchedulerTiKVSplitDisabled"))
)

// checker errors
var (
	ErrCheckerNotFound     = errors.Normalize("checker not found", errors.RFCCodeText("PD:checker:ErrCheckerNotFound"))
	ErrCheckerMergeAgain   = errors.Normalize("region will be merged again, %s", errors.RFCCodeText("PD:checker:ErrCheckerMergeAgain"))
	ErrNoStoreToAdd        = errors.Normalize("no store to add peer", errors.RFCCodeText("PD:checker:ErrNoStoreToAdd"))
	ErrNoStoreToReplace    = errors.Normalize("no store to replace peer", errors.RFCCodeText("PD:checker:ErrNoStoreToReplace"))
	ErrPeerCannotBeLeader  = errors.Normalize("peer cannot be leader", errors.RFCCodeText("PD:checker:ErrPeerCannotBeLeader"))
	ErrPeerCannotBeWitness = errors.Normalize("peer cannot be witness", errors.RFCCodeText("PD:checker:ErrPeerCannotBeWitness"))
	ErrNoNewLeader         = errors.Normalize("no new leader", errors.RFCCodeText("PD:checker:ErrNoNewLeader"))
	ErrRegionNoLeader      = errors.Normalize("region no leader", errors.RFCCodeText("PD:checker:ErrRegionNoLeader"))
)

// scatter errors
var (
	ErrEmptyRegion = errors.Normalize("empty region", errors.RFCCodeText("PD:scatter:ErrEmptyRegion"))
)

// keyspace errors
var (
	// ErrKeyspaceNotFound is used to indicate target keyspace does not exist.
	ErrKeyspaceNotFound = errors.Normalize("keyspace does not exist", errors.RFCCodeText("PD:keyspace:ErrKeyspaceNotFound"))
	// ErrRegionSplitTimeout indices to split region timeout
	ErrRegionSplitTimeout = errors.Normalize("region split timeout", errors.RFCCodeText("PD:keyspace:ErrRegionSplitTimeout"))
	// ErrRegionSplitFailed indices to split region failed
	ErrRegionSplitFailed = errors.Normalize("region split failed", errors.RFCCodeText("PD:keyspace:ErrRegionSplitFailed"))
	// ErrKeyspaceExists indicates target keyspace already exists.
	// It's used when creating a new keyspace.
	ErrKeyspaceExists = errors.Normalize("keyspace already exists", errors.RFCCodeText("PD:keyspace:ErrKeyspaceExists"))
	// ErrKeyspaceGroupExists indicates target keyspace group already exists.
	ErrKeyspaceGroupExists = errors.Normalize("keyspace group already exists", errors.RFCCodeText("PD:keyspace:ErrKeyspaceGroupExists"))
	// ErrKeyspaceNotInKeyspaceGroup is used to indicate target keyspace is not in this keyspace group.
	ErrKeyspaceNotInKeyspaceGroup = errors.Normalize("keyspace is not in this keyspace group", errors.RFCCodeText("PD:keyspace:ErrKeyspaceNotInKeyspaceGroup"))
	// ErrKeyspaceNotInAnyKeyspaceGroup is used to indicate target keyspace is not in any keyspace group.
	ErrKeyspaceNotInAnyKeyspaceGroup = errors.Normalize("keyspace is not in any keyspace group", errors.RFCCodeText("PD:keyspace:ErrKeyspaceNotInAnyKeyspaceGroup"))
	// ErrNodeNotInKeyspaceGroup is used to indicate the tso node is not in this keyspace group.
	ErrNodeNotInKeyspaceGroup = errors.Normalize("the tso node is not in this keyspace group", errors.RFCCodeText("PD:keyspace:ErrNodeNotInKeyspaceGroup"))
	// ErrKeyspaceGroupNotEnoughReplicas is used to indicate not enough replicas in the keyspace group.
	ErrKeyspaceGroupNotEnoughReplicas = errors.Normalize("not enough replicas in the keyspace group", errors.RFCCodeText("PD:keyspace:ErrKeyspaceGroupNotEnoughReplicas"))
	// ErrKeyspaceGroupWithEmptyKeyspace is used to indicate keyspace group with empty keyspace.
	ErrKeyspaceGroupWithEmptyKeyspace = errors.Normalize("keyspace group with empty keyspace", errors.RFCCodeText("PD:keyspace:ErrKeyspaceGroupWithEmptyKeyspace"))
	// ErrModifyDefaultKeyspaceGroup is used to indicate that default keyspace group cannot be modified.
	ErrModifyDefaultKeyspaceGroup = errors.Normalize("default keyspace group cannot be modified", errors.RFCCodeText("PD:keyspace:ErrModifyDefaultKeyspaceGroup"))
	// ErrNoAvailableNode is used to indicate no available node in the keyspace group.
	ErrNoAvailableNode = errors.Normalize("no available node", errors.RFCCodeText("PD:keyspace:ErrNoAvailableNode"))
	// ErrExceedMaxEtcdTxnOps is used to indicate the number of etcd txn operations exceeds the limit.
	ErrExceedMaxEtcdTxnOps = errors.Normalize("exceed max etcd txn operations", errors.RFCCodeText("PD:keyspace:ErrExceedMaxEtcdTxnOps"))
	// ErrModifyDefaultKeyspace is used to indicate that default keyspace cannot be modified.
	ErrModifyDefaultKeyspace = errors.Normalize("cannot modify default keyspace's state", errors.RFCCodeText("PD:keyspace:ErrModifyDefaultKeyspace"))
	// ErrIllegalOperation is used to indicate this is an illegal operation.
	ErrIllegalOperation = errors.Normalize("unknown operation", errors.RFCCodeText("PD:keyspace:ErrIllegalOperation"))
	// ErrUnsupportedOperationInKeyspace is used to indicate this is an unsupported operation.
	ErrUnsupportedOperationInKeyspace = errors.Normalize("it's a unsupported operation", errors.RFCCodeText("PD:keyspace:ErrUnsupportedOperationInKeyspace"))
	// ErrKeyspaceGroupPrimaryNotFound is used to indicate primary of target keyspace group does not exist.
	ErrKeyspaceGroupPrimaryNotFound = errors.Normalize("primary of keyspace group does not exist", errors.RFCCodeText("PD:keyspace:ErrKeyspaceGroupPrimaryNotFound"))
	// ErrKeyspaceGroupNotExists is used to indicate target keyspace group does not exist.
	ErrKeyspaceGroupNotExists = errors.Normalize("keyspace group %v does not exist", errors.RFCCodeText("PD:keyspace:ErrKeyspaceGroupNotExists"))
	// ErrKeyspaceGroupInSplit is used to indicate target keyspace group is in split state.
	ErrKeyspaceGroupInSplit = errors.Normalize("keyspace group %v is in split state", errors.RFCCodeText("PD:keyspace:ErrKeyspaceGroupInSplit"))
	// ErrKeyspaceGroupNotInSplit is used to indicate target keyspace group is not in split state.
	ErrKeyspaceGroupNotInSplit = errors.Normalize("keyspace group %v is not in split state", errors.RFCCodeText("PD:keyspace:ErrKeyspaceGroupNotInSplit"))
	// ErrKeyspaceGroupInMerging is used to indicate target keyspace group is in merging state.
	ErrKeyspaceGroupInMerging = errors.Normalize("keyspace group %v is in merging state", errors.RFCCodeText("PD:keyspace:ErrKeyspaceGroupInMerging"))
	// ErrKeyspaceGroupNotInMerging is used to indicate target keyspace group is not in merging state.
	ErrKeyspaceGroupNotInMerging = errors.Normalize("keyspace group %v is not in merging state", errors.RFCCodeText("PD:keyspace:ErrKeyspaceGroupNotInMerging"))
	// errKeyspaceGroupNotInMerging is used to indicate target keyspace group is not in merging state.
)

// diagnostic errors
var (
	ErrDiagnosticDisabled     = errors.Normalize("diagnostic is disabled", errors.RFCCodeText("PD:diagnostic:ErrDiagnosticDisabled"))
	ErrSchedulerUndiagnosable = errors.Normalize("%v hasn't supported diagnostic", errors.RFCCodeText("PD:diagnostic:ErrSchedulerUndiagnosable"))
	ErrNoDiagnosticResult     = errors.Normalize("%v has no diagnostic result", errors.RFCCodeText("PD:diagnostic:ErrNoDiagnosticResult"))
	ErrDiagnosticLoadPlan     = errors.Normalize("load plan failed", errors.RFCCodeText("PD:diagnostic:ErrDiagnosticLoadPlan"))
)

// placement errors
var (
	ErrRuleContent       = errors.Normalize("invalid rule content, %s", errors.RFCCodeText("PD:placement:ErrRuleContent"))
	ErrLoadRule          = errors.Normalize("load rule failed", errors.RFCCodeText("PD:placement:ErrLoadRule"))
	ErrLoadRuleGroup     = errors.Normalize("load rule group failed", errors.RFCCodeText("PD:placement:ErrLoadRuleGroup"))
	ErrBuildRuleList     = errors.Normalize("build rule list failed, %s", errors.RFCCodeText("PD:placement:ErrBuildRuleList"))
	ErrPlacementDisabled = errors.Normalize("placement rules feature is disabled", errors.RFCCodeText("PD:placement:ErrPlacementDisabled"))
	ErrKeyFormat         = errors.Normalize("key should be in hex format, %s", errors.RFCCodeText("PD:placement:ErrKeyFormat"))
	ErrRuleNotFound      = errors.Normalize("rule not found", errors.RFCCodeText("PD:placement:ErrRuleNotFound"))
)

// region label errors
var (
	ErrRegionRuleContent  = errors.Normalize("invalid region rule content, %s", errors.RFCCodeText("PD:region:ErrRegionRuleContent"))
	ErrRegionRuleNotFound = errors.Normalize("region label rule not found for id %s", errors.RFCCodeText("PD:region:ErrRegionRuleNotFound"))
)

// cluster errors
var (
	ErrNotBootstrapped    = errors.Normalize("TiKV cluster not bootstrapped, please start TiKV first", errors.RFCCodeText("PD:cluster:ErrNotBootstrapped"))
	ErrStoreIsUp          = errors.Normalize("store is still up, please remove store gracefully", errors.RFCCodeText("PD:cluster:ErrStoreIsUp"))
	ErrInvalidStoreID     = errors.Normalize("invalid store id %d, not found", errors.RFCCodeText("PD:cluster:ErrInvalidStoreID"))
	ErrSchedulingIsHalted = errors.Normalize("scheduling is halted", errors.RFCCodeText("PD:cluster:ErrSchedulingIsHalted"))
)

// versioninfo errors
var (
	ErrFeatureNotExisted = errors.Normalize("feature not existed", errors.RFCCodeText("PD:versioninfo:ErrFeatureNotExisted"))
)

// autoscaling errors
var (
	ErrUnsupportedMetricsType   = errors.Normalize("unsupported metrics type %v", errors.RFCCodeText("PD:autoscaling:ErrUnsupportedMetricsType"))
	ErrUnsupportedComponentType = errors.Normalize("unsupported component type %v", errors.RFCCodeText("PD:autoscaling:ErrUnsupportedComponentType"))
	ErrUnexpectedType           = errors.Normalize("unexpected type %v", errors.RFCCodeText("PD:autoscaling:ErrUnexpectedType"))
	ErrTypeConversion           = errors.Normalize("type conversion error", errors.RFCCodeText("PD:autoscaling:ErrTypeConversion"))
	ErrEmptyMetricsResponse     = errors.Normalize("metrics response from Prometheus is empty", errors.RFCCodeText("PD:autoscaling:ErrEmptyMetricsResponse"))
	ErrEmptyMetricsResult       = errors.Normalize("result from Prometheus is empty, %s", errors.RFCCodeText("PD:autoscaling:ErrEmptyMetricsResult"))
)

// apiutil errors
var (
	ErrRedirect             = errors.Normalize("redirect failed", errors.RFCCodeText("PD:apiutil:ErrRedirect"))
	ErrOptionNotExist       = errors.Normalize("the option %s does not exist", errors.RFCCodeText("PD:apiutil:ErrOptionNotExist"))
	ErrRedirectNoLeader     = errors.Normalize("redirect finds no leader", errors.RFCCodeText("PD:apiutil:ErrRedirectNoLeader"))
	ErrRedirectToNotLeader  = errors.Normalize("redirect to not leader", errors.RFCCodeText("PD:apiutil:ErrRedirectToNotLeader"))
	ErrRedirectToNotPrimary = errors.Normalize("redirect to not primary", errors.RFCCodeText("PD:apiutil:ErrRedirectToNotPrimary"))
)

// grpcutil errors
var (
	ErrSecurityConfig = errors.Normalize("security config error: %s", errors.RFCCodeText("PD:grpcutil:ErrSecurityConfig"))
)

// server errors
var (
	ErrServiceRegistered      = errors.Normalize("service with path [%s] already registered", errors.RFCCodeText("PD:server:ErrServiceRegistered"))
	ErrAPIInformationInvalid  = errors.Normalize("invalid api information, group %s version %s", errors.RFCCodeText("PD:server:ErrAPIInformationInvalid"))
	ErrClientURLEmpty         = errors.Normalize("client url empty", errors.RFCCodeText("PD:server:ErrClientEmpty"))
	ErrLeaderNil              = errors.Normalize("leader is nil", errors.RFCCodeText("PD:server:ErrLeaderNil"))
	ErrCancelStartEtcd        = errors.Normalize("etcd start canceled", errors.RFCCodeText("PD:server:ErrCancelStartEtcd"))
	ErrConfigItem             = errors.Normalize("cannot set invalid configuration", errors.RFCCodeText("PD:server:ErrConfiguration"))
	ErrServerNotStarted       = errors.Normalize("server not started", errors.RFCCodeText("PD:server:ErrServerNotStarted"))
	ErrRateLimitExceeded      = errors.Normalize("rate limit exceeded", errors.RFCCodeText("PD:server:ErrRateLimitExceeded"))
	ErrLeaderFrequentlyChange = errors.Normalize("leader %s frequently changed, leader-key is [%s]", errors.RFCCodeText("PD:server:ErrLeaderFrequentlyChange"))
)

// logutil errors
var (
	ErrInitFileLog = errors.Normalize("init file log error, %s", errors.RFCCodeText("PD:logutil:ErrInitFileLog"))
)

// typeutil errors
var (
	ErrBytesToUint64 = errors.Normalize("invalid data, must 8 bytes, but %d", errors.RFCCodeText("PD:typeutil:ErrBytesToUint64"))
)

// cgroup errors
var (
	ErrNoCPUControllerDetected = errors.Normalize("no cpu controller detected", errors.RFCCodeText("PD:cgroup:ErrNoCPUControllerDetected"))
)

// ratelimit errors
var (
	ErrMaxWaitingTasksExceeded = errors.Normalize("max waiting tasks exceeded", errors.RFCCodeText("PD:ratelimit:ErrMaxWaitingTasksExceeded"))
)

// The third-party project error.
// url errors
var (
	ErrURLParse      = errors.Normalize("parse url error", errors.RFCCodeText("PD:url:ErrURLParse"))
	ErrQueryUnescape = errors.Normalize("inverse transformation of QueryEscape error", errors.RFCCodeText("PD:url:ErrQueryUnescape"))
)

// grpc errors
var (
	ErrGRPCDial         = errors.Normalize("dial error", errors.RFCCodeText("PD:grpc:ErrGRPCDial"))
	ErrCloseGRPCConn    = errors.Normalize("close gRPC connection failed", errors.RFCCodeText("PD:grpc:ErrCloseGRPCConn"))
	ErrGRPCSend         = errors.Normalize("send request error", errors.RFCCodeText("PD:grpc:ErrGRPCSend"))
	ErrGRPCRecv         = errors.Normalize("receive response error", errors.RFCCodeText("PD:grpc:ErrGRPCRecv"))
	ErrGRPCCloseSend    = errors.Normalize("close send error", errors.RFCCodeText("PD:grpc:ErrGRPCCloseSend"))
	ErrGRPCCreateStream = errors.Normalize("create stream error", errors.RFCCodeText("PD:grpc:ErrGRPCCreateStream"))
)

// proto errors
var (
	ErrProtoUnmarshal = errors.Normalize("failed to unmarshal proto", errors.RFCCodeText("PD:proto:ErrProtoUnmarshal"))
	ErrProtoMarshal   = errors.Normalize("failed to marshal proto", errors.RFCCodeText("PD:proto:ErrProtoMarshal"))
)

// etcd errors
var (
	ErrNewEtcdClient     = errors.Normalize("new etcd client failed", errors.RFCCodeText("PD:etcd:ErrNewEtcdClient"))
	ErrStartEtcd         = errors.Normalize("start etcd failed", errors.RFCCodeText("PD:etcd:ErrStartEtcd"))
	ErrEtcdURLMap        = errors.Normalize("etcd url map error", errors.RFCCodeText("PD:etcd:ErrEtcdURLMap"))
	ErrEtcdGrantLease    = errors.Normalize("etcd lease failed", errors.RFCCodeText("PD:etcd:ErrEtcdGrantLease"))
	ErrEtcdTxnInternal   = errors.Normalize("internal etcd transaction error occurred", errors.RFCCodeText("PD:etcd:ErrEtcdTxnInternal"))
	ErrEtcdTxnConflict   = errors.Normalize("etcd transaction failed, conflicted and rolled back", errors.RFCCodeText("PD:etcd:ErrEtcdTxnConflict"))
	ErrEtcdKVPut         = errors.Normalize("etcd KV put failed", errors.RFCCodeText("PD:etcd:ErrEtcdKVPut"))
	ErrEtcdKVDelete      = errors.Normalize("etcd KV delete failed", errors.RFCCodeText("PD:etcd:ErrEtcdKVDelete"))
	ErrEtcdKVGet         = errors.Normalize("etcd KV get failed", errors.RFCCodeText("PD:etcd:ErrEtcdKVGet"))
	ErrEtcdKVGetResponse = errors.Normalize("etcd invalid get value response %v, must only one", errors.RFCCodeText("PD:etcd:ErrEtcdKVGetResponse"))
	ErrEtcdGetCluster    = errors.Normalize("etcd get cluster from remote peer failed", errors.RFCCodeText("PD:etcd:ErrEtcdGetCluster"))
	ErrEtcdMoveLeader    = errors.Normalize("etcd move leader error", errors.RFCCodeText("PD:etcd:ErrEtcdMoveLeader"))
	ErrEtcdTLSConfig     = errors.Normalize("etcd TLS config error", errors.RFCCodeText("PD:etcd:ErrEtcdTLSConfig"))
	ErrEtcdWatcherCancel = errors.Normalize("watcher canceled", errors.RFCCodeText("PD:etcd:ErrEtcdWatcherCancel"))
	ErrCloseEtcdClient   = errors.Normalize("close etcd client failed", errors.RFCCodeText("PD:etcd:ErrCloseEtcdClient"))
	ErrEtcdMemberList    = errors.Normalize("etcd member list failed", errors.RFCCodeText("PD:etcd:ErrEtcdMemberList"))
	ErrEtcdMemberRemove  = errors.Normalize("etcd remove member failed", errors.RFCCodeText("PD:etcd:ErrEtcdMemberRemove"))
)

// dashboard errors
var (
	ErrDashboardStart = errors.Normalize("start dashboard failed", errors.RFCCodeText("PD:dashboard:ErrDashboardStart"))
	ErrDashboardStop  = errors.Normalize("stop dashboard failed", errors.RFCCodeText("PD:dashboard:ErrDashboardStop"))
)

// strconv errors
var (
	ErrStrconvParseBool  = errors.Normalize("parse bool error", errors.RFCCodeText("PD:strconv:ErrStrconvParseBool"))
	ErrStrconvParseInt   = errors.Normalize("parse int error", errors.RFCCodeText("PD:strconv:ErrStrconvParseInt"))
	ErrStrconvParseUint  = errors.Normalize("parse uint error", errors.RFCCodeText("PD:strconv:ErrStrconvParseUint"))
	ErrStrconvParseFloat = errors.Normalize("parse float error", errors.RFCCodeText("PD:strconv:ErrStrconvParseFloat"))
)

// prometheus errors
var (
	ErrPrometheusPushMetrics  = errors.Normalize("push metrics to gateway failed", errors.RFCCodeText("PD:prometheus:ErrPrometheusPushMetrics"))
	ErrPrometheusCreateClient = errors.Normalize("create client error", errors.RFCCodeText("PD:prometheus:ErrPrometheusCreateClient"))
	ErrPrometheusQuery        = errors.Normalize("query error", errors.RFCCodeText("PD:prometheus:ErrPrometheusQuery"))
)

// http errors
var (
	ErrSendRequest    = errors.Normalize("send HTTP request failed", errors.RFCCodeText("PD:http:ErrSendRequest"))
	ErrWriteHTTPBody  = errors.Normalize("write HTTP body failed", errors.RFCCodeText("PD:http:ErrWriteHTTPBody"))
	ErrNewHTTPRequest = errors.Normalize("new HTTP request failed", errors.RFCCodeText("PD:http:ErrNewHTTPRequest"))
)

// ioutil error
var (
	ErrIORead = errors.Normalize("IO read error", errors.RFCCodeText("PD:ioutil:ErrIORead"))
)

// os error
var (
	ErrOSOpen = errors.Normalize("open error", errors.RFCCodeText("PD:os:ErrOSOpen"))
)

// dir error
var (
	ErrReadDirName = errors.Normalize("read dir name error", errors.RFCCodeText("PD:dir:ErrReadDirName"))
)

// netstat error
var (
	ErrNetstatTCPSocks = errors.Normalize("TCP socks error", errors.RFCCodeText("PD:netstat:ErrNetstatTCPSocks"))
)

// hex error
var (
	ErrHexDecodingString = errors.Normalize("decode string %s error", errors.RFCCodeText("PD:hex:ErrHexDecodingString"))
)

// filepath errors
var (
	ErrFilePathAbs = errors.Normalize("failed to convert a path to absolute path", errors.RFCCodeText("PD:filepath:ErrFilePathAbs"))
)

// plugin errors
var (
	ErrLoadPlugin       = errors.Normalize("failed to load plugin", errors.RFCCodeText("PD:plugin:ErrLoadPlugin"))
	ErrLookupPluginFunc = errors.Normalize("failed to lookup plugin function", errors.RFCCodeText("PD:plugin:ErrLookupPluginFunc"))
)

// json errors
var (
	ErrJSONMarshal   = errors.Normalize("failed to marshal json", errors.RFCCodeText("PD:json:ErrJSONMarshal"))
	ErrJSONUnmarshal = errors.Normalize("failed to unmarshal json", errors.RFCCodeText("PD:json:ErrJSONUnmarshal"))
)

// leveldb errors
var (
	ErrLevelDBClose = errors.Normalize("close leveldb error", errors.RFCCodeText("PD:leveldb:ErrLevelDBClose"))
	ErrLevelDBWrite = errors.Normalize("leveldb write error", errors.RFCCodeText("PD:leveldb:ErrLevelDBWrite"))
	ErrLevelDBOpen  = errors.Normalize("leveldb open file error", errors.RFCCodeText("PD:leveldb:ErrLevelDBOpen"))
)

// semver
var (
	ErrSemverNewVersion = errors.Normalize("new version error", errors.RFCCodeText("PD:semver:ErrSemverNewVersion"))
)

// log
var (
	ErrInitLogger = errors.Normalize("init logger error", errors.RFCCodeText("PD:log:ErrInitLogger"))
)

// encryption
var (
	ErrEncryptionInvalidMethod      = errors.Normalize("invalid encryption method", errors.RFCCodeText("PD:encryption:ErrEncryptionInvalidMethod"))
	ErrEncryptionInvalidConfig      = errors.Normalize("invalid config", errors.RFCCodeText("PD:encryption:ErrEncryptionInvalidConfig"))
	ErrEncryptionGenerateIV         = errors.Normalize("fail to generate iv", errors.RFCCodeText("PD:encryption:ErrEncryptionGenerateIV"))
	ErrEncryptionGCMEncrypt         = errors.Normalize("GCM encryption fail", errors.RFCCodeText("PD:encryption:ErrEncryptionGCMEncrypt"))
	ErrEncryptionGCMDecrypt         = errors.Normalize("GCM decryption fail", errors.RFCCodeText("PD:encryption:ErrEncryptionGCMDecrypt"))
	ErrEncryptionCTREncrypt         = errors.Normalize("CTR encryption fail", errors.RFCCodeText("PD:encryption:ErrEncryptionCTREncrypt"))
	ErrEncryptionCTRDecrypt         = errors.Normalize("CTR decryption fail", errors.RFCCodeText("PD:encryption:ErrEncryptionCTRDecrypt"))
	ErrEncryptionEncryptRegion      = errors.Normalize("encrypt region fail", errors.RFCCodeText("PD:encryption:ErrEncryptionEncryptRegion"))
	ErrEncryptionDecryptRegion      = errors.Normalize("decrypt region fail", errors.RFCCodeText("PD:encryption:ErrEncryptionDecryptRegion"))
	ErrEncryptionNewDataKey         = errors.Normalize("fail to generate data key", errors.RFCCodeText("PD:encryption:ErrEncryptionNewDataKey"))
	ErrEncryptionNewMasterKey       = errors.Normalize("fail to get master key", errors.RFCCodeText("PD:encryption:ErrEncryptionNewMasterKey"))
	ErrEncryptionCurrentKeyNotFound = errors.Normalize("current data key not found", errors.RFCCodeText("PD:encryption:ErrEncryptionCurrentKeyNotFound"))
	ErrEncryptionKeyNotFound        = errors.Normalize("data key not found", errors.RFCCodeText("PD:encryption:ErrEncryptionKeyNotFound"))
	ErrEncryptionKeysWatcher        = errors.Normalize("data key watcher error", errors.RFCCodeText("PD:encryption:ErrEncryptionKeysWatcher"))
	ErrEncryptionLoadKeys           = errors.Normalize("load data keys error", errors.RFCCodeText("PD:encryption:ErrEncryptionLoadKeys"))
	ErrEncryptionRotateDataKey      = errors.Normalize("failed to rotate data key", errors.RFCCodeText("PD:encryption:ErrEncryptionRotateDataKey"))
	ErrEncryptionSaveDataKeys       = errors.Normalize("failed to save data keys", errors.RFCCodeText("PD:encryption:ErrEncryptionSaveDataKeys"))
	ErrEncryptionKMS                = errors.Normalize("KMS error", errors.RFCCodeText("PD:ErrEncryptionKMS"))
)

// crypto
var (
	ErrCryptoX509KeyPair        = errors.Normalize("x509 keypair error", errors.RFCCodeText("PD:crypto:ErrCryptoX509KeyPair"))
	ErrCryptoAppendCertsFromPEM = errors.Normalize("cert pool append certs error", errors.RFCCodeText("PD:crypto:ErrCryptoAppendCertsFromPEM"))
)

// gin errors
var (
	ErrBindJSON = errors.Normalize("bind JSON error", errors.RFCCodeText("PD:gin:ErrBindJSON"))
)

// unsafe recovery errors
var (
	ErrUnsafeRecoveryIsRunning    = errors.Normalize("unsafe recovery is running", errors.RFCCodeText("PD:unsaferecovery:ErrUnsafeRecoveryIsRunning"))
	ErrUnsafeRecoveryInvalidInput = errors.Normalize("invalid input %s", errors.RFCCodeText("PD:unsaferecovery:ErrUnsafeRecoveryInvalidInput"))
)

// progress errors
var (
	ErrProgressWrongStatus = errors.Normalize("progress status is wrong", errors.RFCCodeText("PD:progress:ErrProgressWrongStatus"))
	ErrProgressNotFound    = errors.Normalize("no progress found for %s", errors.RFCCodeText("PD:progress:ErrProgressNotFound"))
)

// Resource Manager errors
var (
	ErrResourceGroupNotExists = errors.Normalize("the %s resource group does not exist", errors.RFCCodeText("PD:resourcemanager:ErrGroupNotExists"))
	ErrDeleteReservedGroup    = errors.Normalize("cannot delete reserved group", errors.RFCCodeText("PD:resourcemanager:ErrDeleteReservedGroup"))
	ErrInvalidGroup           = errors.Normalize("invalid group settings, please check the group name, priority and the number of resources", errors.RFCCodeText("PD:resourcemanager:ErrInvalidGroup"))
)

// Micro service errors
var (
	ErrNotFoundSchedulingAddr = errors.Normalize("cannot find scheduling address", errors.RFCCodeText("PD:mcs:ErrNotFoundSchedulingAddr"))
	ErrSchedulingServer       = errors.Normalize("scheduling server meets %v", errors.RFCCodeText("PD:mcs:ErrSchedulingServer"))
)
