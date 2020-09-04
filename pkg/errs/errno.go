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

// common error in multiple packages
var (
	ErrGetSourceStore      = errors.Normalize("failed to get the source store", errors.RFCCodeText("PD:common:ErrGetSourceStore"))
	ErrIncorrectSystemTime = errors.Normalize("incorrect system time", errors.RFCCodeText("PD:common:ErrIncorrectSystemTime"))
)

// The internal error which is generated in PD project.
// tso errors
var (
	ErrGetAllocator       = errors.Normalize("get allocator failed, %s", errors.RFCCodeText("PD:tso:ErrGetAllocator"))
	ErrResetUserTimestamp = errors.Normalize("reset user timestamp failed, %s", errors.RFCCodeText("PD:tso:ErrResetUserTimestamp"))
	ErrGenerateTimestamp  = errors.Normalize("generate timestamp failed, %s", errors.RFCCodeText("PD:tso:ErrGenerateTimestamp"))
	ErrInvalidTimestamp   = errors.Normalize("invalid timestamp", errors.RFCCodeText("PD:tso:ErrInvalidTimestamp"))
	ErrLogicOverflow      = errors.Normalize("logic part overflow", errors.RFCCodeText("PD:tso:ErrLogicOverflow"))
)

// member errors
var (
	ErrEtcdLeaderNotFound = errors.Normalize("etcd leader not found", errors.RFCCodeText("PD:member:ErrEtcdLeaderNotFound"))
	ErrMarshalLeader      = errors.Normalize("marshal leader failed", errors.RFCCodeText("PD:member:ErrMarshalLeader"))
)

// client errors
var (
	ErrClientCreateTSOStream = errors.Normalize("create TSO stream failed", errors.RFCCodeText("PD:client:ErrClientCreateTSOStream"))
	ErrClientGetTSOTimeout   = errors.Normalize("get TSO timeout", errors.RFCCodeText("PD:client:ErrClientGetTSOTimeout"))
	ErrClientGetTSO          = errors.Normalize("get TSO failed", errors.RFCCodeText("PD:client:ErrClientGetTSO"))
	ErrClientGetLeader       = errors.Normalize("get leader from %v error", errors.RFCCodeText("PD:client:ErrClientGetLeader"))
	ErrClientGetMember       = errors.Normalize("get member failed", errors.RFCCodeText("PD:client:ErrClientGetMember"))
)

// scheduler errors
var (
	ErrSchedulerExisted                 = errors.Normalize("scheduler existed", errors.RFCCodeText("PD:scheduler:ErrSchedulerExisted"))
	ErrSchedulerNotFound                = errors.Normalize("scheduler not found", errors.RFCCodeText("PD:scheduler:ErrSchedulerNotFound"))
	ErrScheduleConfigNotExist           = errors.Normalize("the config does not exist", errors.RFCCodeText("PD:scheduler:ErrScheduleConfigNotExist"))
	ErrSchedulerConfig                  = errors.Normalize("wrong scheduler config %s", errors.RFCCodeText("PD:scheduler:ErrSchedulerConfig"))
	ErrCacheOverflow                    = errors.Normalize("cache overflow", errors.RFCCodeText("PD:scheduler:ErrCacheOverflow"))
	ErrInternalGrowth                   = errors.Normalize("unknown interval growth type error", errors.RFCCodeText("PD:scheduler:ErrInternalGrowth"))
	ErrSchedulerCreateFuncNotRegistered = errors.Normalize("create func of %v is not registered", errors.RFCCodeText("PD:scheduler:ErrSchedulerCreateFuncNotRegistered"))
)

// placement errors
var (
	ErrRuleContent   = errors.Normalize("invalid rule content, %s", errors.RFCCodeText("PD:placement:ErrRuleContent"))
	ErrLoadRule      = errors.Normalize("load rule failed", errors.RFCCodeText("PD:placement:ErrLoadRule"))
	ErrLoadRuleGroup = errors.Normalize("load rule group failed", errors.RFCCodeText("PD:placement:ErrLoadRuleGroup"))
	ErrBuildRuleList = errors.Normalize("build rule list failed, %s", errors.RFCCodeText("PD:placement:ErrBuildRuleList"))
)

// cluster errors
var (
	ErrNotBootstrapped = errors.Normalize("TiKV cluster not bootstrapped, please start TiKV first", errors.RFCCodeText("PD:cluster:ErrNotBootstrapped"))
	ErrStoreIsUp       = errors.Normalize("store is still up, please remove store gracefully", errors.RFCCodeText("PD:cluster:ErrStoreIsUp"))
)

// grpcutil errors
var (
	ErrSecurityConfig = errors.Normalize("security config error: %s", errors.RFCCodeText("PD:grpcutil:ErrSecurityConfig"))
)

// server errors
var (
	ErrServiceRegistered     = errors.Normalize("service with path [%s] already registered", errors.RFCCodeText("PD:server:ErrServiceRegistered"))
	ErrAPIInformationInvalid = errors.Normalize("invalid api information, group %s version %s", errors.RFCCodeText("PD:server:ErrAPIInformationInvalid"))
	ErrClientURLEmpty        = errors.Normalize("client url empty", errors.RFCCodeText("PD:server:ErrClientEmpty"))
	ErrLeaderNil             = errors.Normalize("leader is nil", errors.RFCCodeText("PD:server:ErrLeaderNil"))
	ErrCancelStartEtcd       = errors.Normalize("etcd start canceled", errors.RFCCodeText("PD:server:ErrCancelStartEtcd"))
)

// The third-party project error.
// url errors
var (
	ErrURLParse      = errors.Normalize("parse url error", errors.RFCCodeText("PD:url:ErrURLParse"))
	ErrQueryUnescape = errors.Normalize("inverse transformation of QueryEscape error", errors.RFCCodeText("PD:url:ErrQueryUnescape"))
)

// grpc errors
var (
	ErrGRPCDial      = errors.Normalize("dial error", errors.RFCCodeText("PD:grpc:ErrGRPCDial"))
	ErrGRPCSend      = errors.Normalize("dial error", errors.RFCCodeText("PD:grpc:ErrGRPCSend"))
	ErrCloseGRPCConn = errors.Normalize("close gRPC connection failed", errors.RFCCodeText("PD:grpc:ErrCloseGRPCConn"))
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
	ErrEtcdTxn           = errors.Normalize("etcd Txn failed", errors.RFCCodeText("PD:etcd:ErrEtcdTxn"))
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
)

// dashboard errors
var (
	ErrDashboardStart = errors.Normalize("start dashboard failed", errors.RFCCodeText("PD:dashboard:ErrDashboardStart"))
	ErrDashboardStop  = errors.Normalize("stop dashboard failed", errors.RFCCodeText("PD:dashboard:ErrDashboardStop"))
)

// strconv errors
var (
	ErrStrconvParseInt   = errors.Normalize("parse int error", errors.RFCCodeText("PD:strconv:ErrStrconvParseInt"))
	ErrStrconvParseUint  = errors.Normalize("parse uint error", errors.RFCCodeText("PD:strconv:ErrStrconvParseUint"))
	ErrStrconvParseFloat = errors.Normalize("parse float error", errors.RFCCodeText("PD:strconv:ErrStrconvParseFloat"))
)

// prometheus errors
var (
	ErrPrometheusPushMetrics = errors.Normalize("push metrics to gateway failed", errors.RFCCodeText("PD:prometheus:ErrPrometheusPushMetrics"))
)

// http errors
var (
	ErrSendRequest    = errors.Normalize("send HTTP request failed", errors.RFCCodeText("PD:http:ErrSendRequest"))
	ErrWriteHTTPBody  = errors.Normalize("write HTTP body failed", errors.RFCCodeText("PD:http:ErrWriteHTTPBody"))
	ErrNewHTTPRequest = errors.Normalize("new HTTP request failed", errors.RFCCodeText("PD:http:ErrNewHTTPRequest"))
)

// leveldb errors
var (
	ErrLeveldbClose = errors.Normalize("failed to close leveldb", errors.RFCCodeText("PD:leveldb:ErrLeveldbClose"))
	ErrLeveldbWrite = errors.Normalize("failed to write leveldb", errors.RFCCodeText("PD:leveldb:ErrLeveldbWrite"))
)

// ioutil error
var (
	ErrIORead = errors.Normalize("IO read error", errors.RFCCodeText("PD:ioutil:ErrIORead"))
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
