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

package errors

import "github.com/pingcap/errors"

var (
	reg = errors.NewRegistry("PD")
	// ClassIO is IO error class
	ClassIO = reg.RegisterErrorClass(1, "io")
	// ClassNetwork is network error class
	ClassNetwork = reg.RegisterErrorClass(2, "network")
	// ClassStorage is storage error class
	ClassStorage = reg.RegisterErrorClass(3, "storage")
	// ClassInternal is internal error class
	ClassInternal = reg.RegisterErrorClass(4, "internal")
	// ClassFormat is format error class
	ClassFormat = reg.RegisterErrorClass(5, "format")
	// ClassOther is other error class
	ClassOther = reg.RegisterErrorClass(6, "other")
)

var (
	// ErrIORead is io error
	ErrIORead = ClassIO.DefineError().TextualCode("ErrIORead").MessageTemplate("io read error").Done()
	// ErrHTTPErrorResponse is http error response error
	ErrHTTPErrorResponse = ClassNetwork.DefineError().TextualCode("ErrHTTPErrorResponse").MessageTemplate("make http error response fail").Done()
	// ErrHTTPRedirect is http redirect error
	ErrHTTPRedirect = ClassNetwork.DefineError().TextualCode("ErrHTTPRedirect").MessageTemplate("http redirect many times").Done()
	// ErrHTTPRequestURL is http request url error
	ErrHTTPRequestURL = ClassNetwork.DefineError().TextualCode("ErrHTTPRequestURL").MessageTemplate("wrong url in user http request").Done()
	// ErrGRPCTso is grpc tso error
	ErrGRPCTso = ClassNetwork.DefineError().TextualCode("ErrGRPCTso").MessageTemplate("grpc tso request fail").Done()
	// ErrGRPCHeartbeat is grpc heartbeat error
	ErrGRPCHeartbeat = ClassNetwork.DefineError().TextualCode("ErrGRPCHeartbeat").MessageTemplate("grpc heartbeat request fail").Done()
	// ErrGRPCClose is grpc close error
	ErrGRPCClose = ClassNetwork.DefineError().TextualCode("ErrGRPCClose").MessageTemplate("grpc close connection fail").Done()
	// ErrGRPCSend is grpc send error
	ErrGRPCSend = ClassNetwork.DefineError().TextualCode("ErrGRPCSend").MessageTemplate("grpc send message fail").Done()
	// ErrStorageLoad is storage load error
	ErrStorageLoad = ClassStorage.DefineError().TextualCode("ErrStorageLoad").MessageTemplate("load config from storage error").Done()
	// ErrStorageSave is storage save error
	ErrStorageSave = ClassStorage.DefineError().TextualCode("ErrStorageSave").MessageTemplate("save config to storage error").Done()
	// ErrStorageDelete is storage delete error
	ErrStorageDelete = ClassStorage.DefineError().TextualCode("ErrStorageDelete").MessageTemplate("delete config from storage error").Done()
	// ErrStorageEtcdLoad is etcd storage load error
	ErrStorageEtcdLoad = ClassStorage.DefineError().TextualCode("ErrStorageEtcdLoad").MessageTemplate("load config from storage etcd error").Done()
	// ErrStorageEtcdSave is etcd storage save error
	ErrStorageEtcdSave = ClassStorage.DefineError().TextualCode("ErrStorageEtcdSave").MessageTemplate("save config to storage etcd error").Done()
	// ErrStorageEtcdDelete is etcd storage delete error
	ErrStorageEtcdDelete = ClassStorage.DefineError().TextualCode("ErrStorageEtcdDelete").MessageTemplate("delete config from storage etcd error").Done()
	// ErrInternalSchedulerDuplicate is scheduler duplicate
	ErrInternalSchedulerDuplicate = ClassInternal.DefineError().TextualCode("ErrInternalSchedulerDuplicate").MessageTemplate("duplicate scheduler found").Done()
	// ErrInternalSchedulerNotFound is scheduler not found
	ErrInternalSchedulerNotFound = ClassInternal.DefineError().TextualCode("ErrInternalSchedulerNotFound").MessageTemplate("scheduler not found").Done()
	// ErrInternalSchedulerConfig is scheduler config error
	ErrInternalSchedulerConfig = ClassInternal.DefineError().TextualCode("ErrInternalSchedulerConfig").MessageTemplate("wrong scheduler config").Done()
	// InternalRuleInvalid is invalid rule
	ErrInternalRuleInvalid = ClassInternal.DefineError().TextualCode("ErrInternalRuleInvalid").MessageTemplate("invalid rule found").Done()
	// ErrInternalRuleDuplicate is duplicate rule
	ErrInternalRuleDuplicate = ClassInternal.DefineError().TextualCode("ErrInternalRuleDuplicate").MessageTemplate("duplicate rule found").Done()
	// ErrInternalRuleMismatch is rule mismatch
	ErrInternalRuleMismatch = ClassInternal.DefineError().TextualCode("ErrInternalRuleMismatch").MessageTemplate("rule key mismatch").Done()
	// ErrInternalOperatorNotFound is operator not found
	ErrInternalOperatorNotFound = ClassInternal.DefineError().TextualCode("ErrInternalOperatorNotFound").MessageTemplate("operator not found").Done()
	// ErrInternalOperatorMerge is operator merge error
	ErrInternalOperatorMerge = ClassInternal.DefineError().TextualCode("ErrInternalOperatorMerge").MessageTemplate("merge operator should be pair").Done()
	// ErrInternalOperatorNotStart is operator not start
	ErrInternalOperatorNotStart = ClassInternal.DefineError().TextualCode("ErrInternalOperatorNotStart").MessageTemplate("operator not start").Done()
	// ErrInternalOperatorNotEnd is operator not end
	ErrInternalOperatorNotEnd = ClassInternal.DefineError().TextualCode("ErrInternalOperatorNotEnd").MessageTemplate("operator not end").Done()
	// ErrInternalOperatorStepUnknown is operator step unknown
	ErrInternalOperatorStepUnknown = ClassInternal.DefineError().TextualCode("ErrInternalStepUnknown").MessageTemplate("operator step is unknown").Done()
	// ErrInternalStoreNotFound is store not found
	ErrInternalStoreNotFound = ClassInternal.DefineError().TextualCode("ErrInternalStoreNotFound").MessageTemplate("store id %d not found").Done()
	// ErrInternalClusterVersionChange is cluster version change error
	ErrInternalClusterVersionChange = ClassInternal.DefineError().TextualCode("ErrInternalClusterVersionChange").MessageTemplate("cluster version change same time").Done()
	// ErrInternalRegionKey is region key error
	ErrInternalRegionKey = ClassInternal.DefineError().TextualCode("ErrInternalRegionKey").MessageTemplate("wrong region key range").Done()
	// ErrInternalCacheRegionOverflow is cache region overflow
	ErrInternalCacheRegionOverflow = ClassInternal.DefineError().TextualCode("ErrInternalCacheRegionOverflow").MessageTemplate("cache region overflow").Done()
	// ErrInternalVersionFeatureNotExist is version feature not exist
	ErrInternalVersionFeatureNotExist = ClassInternal.DefineError().TextualCode("ErrInternalVersionFeatureNotExist").MessageTemplate("version feature not exist").Done()
	// ErrFormatParseCmd is parse cmd error
	ErrFormatParseCmd = ClassFormat.DefineError().TextualCode("ErrFormatParseCmd").MessageTemplate("parse cmd error").Done()
	// ErrFormatParseClusterVersion is parse cluster version error
	ErrFormatParseClusterVersion = ClassFormat.DefineError().TextualCode("ErrFormatParseClusterVersion").MessageTemplate("parse cluster version error").Done()
	// ErrFormatParseURL is parse url error
	ErrFormatParseURL = ClassFormat.DefineError().TextualCode("ErrFormatParseURL").MessageTemplate("parse url error").Done()
	// ErrFormatParseHistoryIndex is parse history index error
	ErrFormatParseHistoryIndex = ClassFormat.DefineError().TextualCode("ErrFormatParseHistoryIndex").MessageTemplate("parse history index error").Done()
	// ErrOtherInitLog is init log error
	ErrOtherInitLog = ClassOther.DefineError().TextualCode("ErrOtherInitLog").MessageTemplate("init log fail").Done()
	// ErrOtherDashboardServer is dashboard server error
	ErrOtherDashboardServer = ClassOther.DefineError().TextualCode("ErrOtherDashboardServer").MessageTemplate("dashboard server error").Done()
	// ErrOtherPrometheusPush is prometheus push error
	ErrOtherPrometheusPush = ClassOther.DefineError().TextualCode("ErrOtherPrometheusPush").MessageTemplate("push to prometheus error").Done()
	// ErrOtherPluginLoadActionUnknown is plugin action unknown
	ErrOtherPluginLoadActionUnknown = ClassOther.DefineError().TextualCode("ErrOtherPluginLoadActionUnknown").MessageTemplate("unknown action to load plugin").Done()
	// ErrOtherPluginFuncNotFound is plugin func not found
	ErrOtherPluginFuncNotFound = ClassOther.DefineError().TextualCode("ErrOtherPluginFuncNotFound").MessageTemplate("plugin function not found").Done()
	// ErrOtherSystemTime is system time error
	ErrOtherSystemTime = ClassOther.DefineError().TextualCode("ErrOtherSystemTime").MessageTemplate("system time error").Done()
)
