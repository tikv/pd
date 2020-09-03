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

// The internal error which is generated in PD project.
// tso errors
var (
	ErrGetAllocator        = errors.Normalize("get allocator failed, %s", errors.RFCCodeText("PD:tso:ErrGetAllocator"))
	ErrResetUserTimestamp  = errors.Normalize("reset user timestamp failed, %s", errors.RFCCodeText("PD:tso:ErrResetUserTimestamp"))
	ErrGenerateTimestamp   = errors.Normalize("generate timestamp failed, %s", errors.RFCCodeText("PD:tso:ErrGenerateTimestamp"))
	ErrInvalidTimestamp    = errors.Normalize("invalid timestamp", errors.RFCCodeText("PD:tso:ErrInvalidTimestamp"))
	ErrLogicOverflow       = errors.Normalize("logic part overflow", errors.RFCCodeText("PD:tso:ErrLogicOverflow"))
	ErrIncorrectSystemTime = errors.Normalize("incorrect system time", errors.RFCCodeText("PD:tso:ErrIncorrectSystemTime"))
)

// member errors
var (
	ErrEtcdLeaderNotFound = errors.Normalize("etcd leader not found", errors.RFCCodeText("PD:member:ErrEtcdLeaderNotFound"))
	ErrMarshalLeader      = errors.Normalize("marshal leader failed", errors.RFCCodeText("PD:member:ErrMarshalLeader"))
)

// core errors
var (
	ErrWrongRangeKeys      = errors.Normalize("wrong range keys", errors.RFCCodeText("PD:core:ErrWrongRangeKeys"))
	ErrStoreNotFound       = errors.Normalize("store %v not found", errors.RFCCodeText("PD:core:ErrStoreNotFound"))
	ErrPauseLeaderTransfer = errors.Normalize("store %v is paused for leader transfer", errors.RFCCodeText("PD:core:ErrPauseLeaderTransfer"))
	ErrStoreTombstone      = errors.Normalize("store %v has been removed", errors.RFCCodeText("PD:core:ErrStoreTombstone"))
)

// client errors
var (
	ErrClientCreateTSOStream = errors.Normalize("create TSO stream failed", errors.RFCCodeText("PD:client:ErrClientCreateTSOStream"))
	ErrClientGetTSOTimeout   = errors.Normalize("get TSO timeout", errors.RFCCodeText("PD:client:ErrClientGetTSOTimeout"))
	ErrClientGetTSO          = errors.Normalize("get TSO failed", errors.RFCCodeText("PD:client:ErrClientGetTSO"))
	ErrClientGetLeader       = errors.Normalize("get leader from %v error", errors.RFCCodeText("PD:client:ErrClientGetLeader"))
	ErrClientGetMember       = errors.Normalize("get member failed", errors.RFCCodeText("PD:client:ErrClientGetMember"))
)

// schedule errors
var (
	ErrUnexpectedOperatorStatus = errors.Normalize("operator with unexpected status", errors.RFCCodeText("PD:schedule:ErrUnexpectedOperatorStatus"))
	ErrUnknownOperatorStep      = errors.Normalize("unknown operator step found", errors.RFCCodeText("PD:schedule:ErrUnknownOperatorStep"))
	ErrMergeOperator            = errors.Normalize("merge operator error, %s", errors.RFCCodeText("PD:schedule:ErrMergeOperator"))
)

// scheduler errors
var (
	ErrGetSourceStore                   = errors.Normalize("failed to get the source store", errors.RFCCodeText("PD:scheduler:ErrGetSourceStore"))
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

// autoscaling errors
var (
	ErrUnsupportedMetricsType   = errors.Normalize("unsupported metrics type %v", errors.RFCCodeText("PD:autoscaling:ErrUnsupportedMetricsType"))
	ErrUnsupportedComponentType = errors.Normalize("unsupported component type %v", errors.RFCCodeText("PD:autoscaling:ErrUnsupportedComponentType"))
	ErrUnexpectedType           = errors.Normalize("unexpected type %v", errors.RFCCodeText("PD:autoscaling:ErrUnexpectedType"))
	ErrTypeConversion           = errors.Normalize(("type conversion error"), errors.RFCCodeText("PD:autoscaling:ErrTypeConversion"))
	ErrEmptyMetricsResponse     = errors.Normalize("metrics response from Prometheus is empty", errors.RFCCodeText("PD:autoscaling:ErrEmptyMetricsResponse"))
	ErrEmptyMetricsResult       = errors.Normalize("result from Prometheus is empty, %s", errors.RFCCodeText("PD:autoscaling:ErrEmptyMetricsResult"))
)

// apiutil errors
var (
	ErrRedirect = errors.Normalize("redirect failed", errors.RFCCodeText("PD:apiutil:ErrRedirect"))
)

// grpcutil errors
var (
	ErrSecurityConfig = errors.Normalize("security config error: %s", errors.RFCCodeText("PD:grpcutil:ErrSecurityConfig"))
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
	ErrStrconvParseInt  = errors.Normalize("parse int error", errors.RFCCodeText("PD:strconv:ErrStrconvParseInt"))
	ErrStrconvParseUint = errors.Normalize("parse uint error", errors.RFCCodeText("PD:strconv:ErrStrconvParseUint"))
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
	ErrDirReadName = errors.Normalize("read dir name error", errors.RFCCodeText("PD:dir:ErrDirReadName"))
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
	ErrLevelDBWrite = errors.Normalize("leveldb write error", errors.RFCCodeText("PD:leveldb:ErrLevelDBWrite"))
)
