// Copyright 2020 TiKV Project Authors.
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

var (
	reg = errors.NewRegistry("PD")
	// ClassTSO defines tso error class
	ClassTSO = reg.RegisterErrorClass(1, "tso")
	// ClassAdaptor defines adapter error class
	ClassAdaptor = reg.RegisterErrorClass(2, "adapter")
	// ClassMember defines member error class
	ClassMember = reg.RegisterErrorClass(3, "member")
	// ClassClient defines client error class
	ClassClient = reg.RegisterErrorClass(4, "client")
	// ClassPlacement defines placement error class
	ClassPlacement = reg.RegisterErrorClass(5, "placement")
	// ClassKV defines KV error class
	ClassKV = reg.RegisterErrorClass(6, "kv")
)

// tso errors
var (
	ErrInvalidTimestamp    = ClassTSO.DefineError().TextualCode("ErrInvalidTimestamp").MessageTemplate("invalid timestamp").Build()
	ErrLogicOverflow       = ClassTSO.DefineError().TextualCode("ErrLogicOverflow").MessageTemplate("logic part overflow").Build()
	ErrIncorrectSystemTime = ClassTSO.DefineError().TextualCode("ErrIncorrectSystemTime").MessageTemplate("incorrect system time").Build()
)

// adapter errors
var (
	ErrStartDashboard = ClassAdaptor.DefineError().TextualCode("ErrStartDashboard").MessageTemplate("fail to start dashboard").Build()
	ErrStopDashboard  = ClassAdaptor.DefineError().TextualCode("ErrStopDashboard").MessageTemplate("fail to stop dashboard").Build()
)

// member errors
var (
	ErretcdLeaderNotFound     = ClassMember.DefineError().TextualCode("ErretcdLeaderNotFound").MessageTemplate("etcd leader not found").Build()
	ErrGetLeader              = ClassMember.DefineError().TextualCode("ErrGetLeader").MessageTemplate("fail to get leader").Build()
	ErrDeleteLeaderKey        = ClassMember.DefineError().TextualCode("ErrDeleteLeaderKey").MessageTemplate("fail to delete leader key").Build()
	ErrLoadLeaderPriority     = ClassMember.DefineError().TextualCode("ErrLoadLeaderPriority").MessageTemplate("fail to load leader priority").Build()
	ErrLoadetcdLeaderPriority = ClassMember.DefineError().TextualCode("ErrLoadetcdLeaderPriority").MessageTemplate("fail to load etcd leader priority").Build()
	ErrTransferetcdLeader     = ClassMember.DefineError().TextualCode("ErrTransferetcdLeader").MessageTemplate("fail to transfer etcd leader").Build()
	ErrWatcherCancel          = ClassMember.DefineError().TextualCode("ErrWatcherCancel").MessageTemplate("watcher canceled").Build()
	ErrMarshalLeader          = ClassMember.DefineError().TextualCode("ErrMarshalLeader").MessageTemplate("fail to marshal leader").Build()
)

// client errors
var (
	ErrCloseGRPCConn   = ClassClient.DefineError().TextualCode("ErrCloseGRPCConn").MessageTemplate("fail to close gRPC connection").Build()
	ErrUpdateLeader    = ClassClient.DefineError().TextualCode("ErrUpdateLeader").MessageTemplate("fail to update leader").Build()
	ErrCreateTSOStream = ClassClient.DefineError().TextualCode("ErrCreateTSOStream").MessageTemplate("fail to create TSO stream").Build()
	ErrGetTSO          = ClassClient.DefineError().TextualCode("ErrGetTSO").MessageTemplate("fail to get TSO").Build()
	ErrGetClusterID    = ClassClient.DefineError().TextualCode("ErrGetClusterID").MessageTemplate("fail to get cluster id").Build()
)

// placement errors
var (
	ErrRuleContent   = ClassPlacement.DefineError().TextualCode("ErrRuleContent").MessageTemplate("invalid rule content, %s").Build()
	ErrLoadRule      = ClassPlacement.DefineError().TextualCode("ErrLoadRule").MessageTemplate("fail to load rule").Build()
	ErrLoadRuleGroup = ClassPlacement.DefineError().TextualCode("ErrLoadRuleGroup").MessageTemplate("fail to load rule group").Build()
	ErrBuildRuleList = ClassPlacement.DefineError().TextualCode("ErrBuildRuleList").MessageTemplate("fail to build rule list, %s").Build()
)

// kv errors
var (
	ErrEtcdKvSave   = ClassKV.DefineError().TextualCode("ErrEtcdKvSave").MessageTemplate("etcd kv save error, key is %s, value is %s").Build()
	ErrEtcdKvRemove = ClassKV.DefineError().TextualCode("ErrEtcdKvRemove").MessageTemplate("etcd kv remove error, key is %s").Build()
)
