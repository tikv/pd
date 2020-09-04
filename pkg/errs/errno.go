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
	ErrInvalidTimestamp = errors.Normalize("invalid timestamp", errors.RFCCodeText("PD:tso:ErrInvalidTimestamp"))
)

// adapter errors
var (
	ErrStartDashboard = errors.Normalize("start dashboard failed", errors.RFCCodeText("PD:adapter:ErrStartDashboard"))
	ErrStopDashboard  = errors.Normalize("stop dashboard failed", errors.RFCCodeText("PD:adapter:ErrStopDashboard"))
)

// member errors
var (
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

// kv errors
var (
	ErrEtcdKVSave   = errors.Normalize("etcd KV save failed", errors.RFCCodeText("PD:kv:ErrEtcdKVSave"))
	ErrEtcdKVRemove = errors.Normalize("etcd KV remove failed", errors.RFCCodeText("PD:kv:ErrEtcdKVRemove"))
)

// scheduler errors
var (
	ErrGetSourceStore         = errors.Normalize("failed to get the source store", errors.RFCCodeText("PD:scheduler:ErrGetSourceStore"))
	ErrSchedulerExisted       = errors.Normalize("scheduler existed", errors.RFCCodeText("PD:scheduler:ErrSchedulerExisted"))
	ErrSchedulerNotFound      = errors.Normalize("scheduler not found", errors.RFCCodeText("PD:scheduler:ErrSchedulerNotFound"))
	ErrScheduleConfigNotExist = errors.Normalize("the config does not exist", errors.RFCCodeText("PD:scheduler:ErrScheduleConfigNotExist"))
	ErrSchedulerConfig        = errors.Normalize("wrong scheduler config %s", errors.RFCCodeText("PD:scheduler:ErrSchedulerConfig"))
	ErrCacheOverflow          = errors.Normalize("cache overflow", errors.RFCCodeText("PD:scheduler:ErrCacheOverflow"))
	ErrInternalGrowth         = errors.Normalize("unknown interval growth type error", errors.RFCCodeText("PD:scheduler:ErrInternalGrowth"))
)

// strconv errors
var (
	ErrStrconvParseUint = errors.Normalize("parse uint error", errors.RFCCodeText("PD:strconv:ErrStrconvParseUint"))
)

// url errors
var (
	ErrQueryUnescape = errors.Normalize("inverse transformation of QueryEscape error", errors.RFCCodeText("PD:url:ErrQueryUnescape"))
)
