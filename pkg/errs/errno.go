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
// See the License for the specific language governing permissions and
// limitations under the License.

package errs

import "github.com/pingcap/errors"

// tso errors
var (
	ErrInvalidTimestamp    = errors.Normalize("invalid timestamp", errors.RFCCodeText("PD:tso:ErrInvalidTimestamp"))
	ErrLogicOverflow       = errors.Normalize("logic part overflow", errors.RFCCodeText("PD:tso:ErrLogicOverflow"))
	ErrIncorrectSystemTime = errors.Normalize("incorrect system time", errors.RFCCodeText("PD:tso:ErrIncorrectSystemTime"))
)

// adapter errors
var (
	ErrStartDashboard = errors.Normalize("start dashboard failed", errors.RFCCodeText("PD:adapter:ErrStartDashboard"))
	ErrStopDashboard  = errors.Normalize("stop dashboard failed", errors.RFCCodeText("PD:adapter:ErrStopDashboard"))
)

// member errors
var (
	ErretcdLeaderNotFound     = errors.Normalize("etcd leader not found", errors.RFCCodeText("PD:member:ErretcdLeaderNotFound"))
	ErrGetLeader              = errors.Normalize("get leader failed", errors.RFCCodeText("PD:member:ErrGetLeader"))
	ErrDeleteLeaderKey        = errors.Normalize("delete leader key failed", errors.RFCCodeText("PD:member:ErrDeleteLeaderKey"))
	ErrLoadLeaderPriority     = errors.Normalize("load leader priority failed", errors.RFCCodeText("PD:member:ErrLoadLeaderPriority"))
	ErrLoadetcdLeaderPriority = errors.Normalize("load etcd leader priority failed", errors.RFCCodeText("PD:member:ErrLoadetcdLeaderPriority"))
	ErrTransferetcdLeader     = errors.Normalize("transfer etcd leader failed", errors.RFCCodeText("PD:member:ErrTransferetcdLeader"))
	ErrWatcherCancel          = errors.Normalize("watcher canceled", errors.RFCCodeText("PD:member:ErrWatcherCancel"))
	ErrMarshalLeader          = errors.Normalize("marshal leader failed", errors.RFCCodeText("PD:member:ErrMarshalLeader"))
)

// client errors
var (
	ErrCloseGRPCConn   = errors.Normalize("close gRPC connection failed", errors.RFCCodeText("PD:client:ErrCloseGRPCConn"))
	ErrUpdateLeader    = errors.Normalize("update leader failed", errors.RFCCodeText("PD:client:ErrUpdateLeader"))
	ErrCreateTSOStream = errors.Normalize("create TSO stream failed", errors.RFCCodeText("PD:client:ErrCreateTSOStream"))
	ErrGetTSO          = errors.Normalize("get TSO failed", errors.RFCCodeText("PD:client:ErrGetTSO"))
	ErrGetClusterID    = errors.Normalize("get cluster ID failed", errors.RFCCodeText("PD:client:ErrGetClusterID"))
)

// placement errors
var (
	ErrRuleContent   = errors.Normalize("invalid rule content, %s", errors.RFCCodeText("PD:placement:ErrRuleContent"))
	ErrLoadRule      = errors.Normalize("load rule failed", errors.RFCCodeText("PD:placement:ErrLoadRule"))
	ErrLoadRuleGroup = errors.Normalize("load rule group failed", errors.RFCCodeText("PD:placement:ErrLoadRuleGroup"))
	ErrBuildRuleList = errors.Normalize("build rule list failed, %s", errors.RFCCodeText("PD:placement:ErrBuildRuleList"))
)

// kv errors
var (
	ErrEtcdKVSave   = errors.Normalize("etcd KV save failed", errors.RFCCodeText("PD:kv:ErrEtcdKVSave"))
	ErrEtcdKVRemove = errors.Normalize("etcd KV remove failed", errors.RFCCodeText("PD:kv:ErrEtcdKVRemove"))
)

// apiutil errors
var (
	ErrRequestHTTP   = errors.Normalize("send HTTP request failed", errors.RFCCodeText("PD:apiutil:ErrRequestHTTP"))
	ErrReadHTTPBody  = errors.Normalize("read HTTP body failed", errors.RFCCodeText("PD:apiutil:ErrReadHTTPBody"))
	ErrWriteHTTPBody = errors.Normalize("write HTTP body failed", errors.RFCCodeText("PD:apiutil:ErrWriteHTTPBody"))
)

// cluster errors
var (
	ErrPersistStore          = errors.Normalize("failed to persist store", errors.RFCCodeText("PD:cluster:ErrPersistStore"))
	ErrDeleteRegion          = errors.Normalize("failed to delete region from storage", errors.RFCCodeText("PD:cluster:ErrDeleteRegion"))
	ErrSaveRegion            = errors.Normalize("failed to save region from storage", errors.RFCCodeText("PD:cluster:ErrSaveRegion"))
	ErrBuryStore             = errors.Normalize("failed to bury store", errors.RFCCodeText("PD:cluster:ErrBuryStore"))
	ErrDeleteStore           = errors.Normalize("failed to delete store", errors.RFCCodeText("PD:cluster:ErrDeleteStore"))
	ErrPersistClusterVersion = errors.Normalize("persist cluster version meet error", errors.RFCCodeText("PD:cluster:ErrPersistClusterVersion"))
	ErrGetMembers            = errors.Normalize("get members failed", errors.RFCCodeText("PD:cluster:ErrGetMembers"))
	// TODO: ErrNewHTTPRequest may not be suitable to put in cluster category
	ErrNewHTTPRequest = errors.Normalize("new HTTP request failed", errors.RFCCodeText("PD:cluster:ErrNewHTTPRequest"))
)

// metricutil errors
var (
	ErrPushGateway = errors.Normalize("push metrics to gateway failed", errors.RFCCodeText("PD:metricutil:ErrPushGateway"))
)

// etcdutil errors
var (
	ErrLoadValue  = errors.Normalize("load value from etcd failed", errors.RFCCodeText("PD:etcdutil:ErrLoadValue"))
	ErrGetCluster = errors.Normalize("get cluster from remote peer failed", errors.RFCCodeText("PD:etcdutil:ErrGetCluster"))
)

// etcd errors
var (
	ErrCloseEtcdClient = errors.Normalize("close etcd client failed", errors.RFCCodeText("PD:etcd:ErrCloseEtcdClient"))
	ErrStartEtcd       = errors.Normalize("start etcd failed", errors.RFCCodeText("PD:etcd:ErrStartEtcd"))
)

// server errors
var (
	ErrDecodeSchedulerConfig  = errors.Normalize("decode scheduler config failed", errors.RFCCodeText("PD:server:ErrDecodeSchedulerConfig"))
	ErrServiceRegistered      = errors.Normalize("service with path [%s] already registered", errors.RFCCodeText("PD:server:ErrServiceRegistered"))
	ErrAPIInformationInvalid  = errors.Normalize("invalid api information, group %s version %s", errors.RFCCodeText("PD:server:ErrAPIInformationInvalid"))
	ErrBootstrapCluster       = errors.Normalize("bootstrap cluster failed", errors.RFCCodeText("PD:server:ErrBootstrapCluster"))
	ErrDisablePlacementRules  = errors.Normalize("cannot disable placement rules with TiFlash nodes", errors.RFCCodeText("PD:server:ErrDisablePlacementRules"))
	ErrClientURL              = errors.Normalize("%s is not the client url of any member", errors.RFCCodeText("PD:server:ErrClientURL"))
	ErrLogLevelIllegal        = errors.Normalize("illegal log level %s", errors.RFCCodeText("PD:server:ErrLogLevelIllegal"))
	ErrReplicationModeInvalid = errors.Normalize("invalid replication mode: %v", errors.RFCCodeText("PD:server:ErrReplicationModeInvalid"))
	ErrReplicateFile          = errors.Normalize("replicate to member %s failed", errors.RFCCodeText("PD:server:ErrReplicateFile"))
	ErrStoreNotFound          = errors.Normalize("store %d not found", errors.RFCCodeText("PD:server:ErrStoreNotFound"))
	ErrLeaderNil              = errors.Normalize("leader is nil", errors.RFCCodeText("PD:server:ErrLeaderNil"))
)
