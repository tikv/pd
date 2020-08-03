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
	ErrIORead = ClassIO.DefineError().TextualCode("ErrIORead").MessageTemplate("io read error").Build()
	// ErrHTTPErrorResponse is http error response error
	ErrHTTPErrorResponse = ClassNetwork.DefineError().TextualCode("ErrHTTPErrorResponse").MessageTemplate("make http error response fail").Build()
	// ErrHTTPRedirect is http redirect error
	ErrHTTPRedirect = ClassNetwork.DefineError().TextualCode("ErrHTTPRedirect").MessageTemplate("http redirect many times").Build()
	// ErrHTTPRequestURL is http request url error
	ErrHTTPRequestURL = ClassNetwork.DefineError().TextualCode("ErrHTTPRequestURL").MessageTemplate("wrong url in user http request").Build()
	// ErrGRPCTso is grpc tso error
	ErrGRPCTso = ClassNetwork.DefineError().TextualCode("ErrGRPCTso").MessageTemplate("grpc tso request fail").Build()
	// ErrGRPCHeartbeat is grpc heartbeat error
	ErrGRPCHeartbeat = ClassNetwork.DefineError().TextualCode("ErrGRPCHeartbeat").MessageTemplate("grpc heartbeat request fail").Build()
	// ErrGRPCClose is grpc close error
	ErrGRPCClose = ClassNetwork.DefineError().TextualCode("ErrGRPCClose").MessageTemplate("grpc close connection fail").Build()
	// ErrGRPCSend is grpc send error
	ErrGRPCSend = ClassNetwork.DefineError().TextualCode("ErrGRPCSend").MessageTemplate("grpc send message fail").Build()
	// ErrStorageLoad is storage load error
	ErrStorageLoad = ClassStorage.DefineError().TextualCode("ErrStorageLoad").MessageTemplate("load config from storage error").Build()
	// ErrStorageSave is storage save error
	ErrStorageSave = ClassStorage.DefineError().TextualCode("ErrStorageSave").MessageTemplate("save config to storage error").Build()
	// ErrStorageDelete is storage delete error
	ErrStorageDelete = ClassStorage.DefineError().TextualCode("ErrStorageDelete").MessageTemplate("delete config from storage error").Build()
	// ErrStorageEtcdLoad is etcd storage load error
	ErrStorageEtcdLoad = ClassStorage.DefineError().TextualCode("ErrStorageEtcdLoad").MessageTemplate("load config from storage etcd error").Build()
	// ErrStorageEtcdSave is etcd storage save error
	ErrStorageEtcdSave = ClassStorage.DefineError().TextualCode("ErrStorageEtcdSave").MessageTemplate("save config to storage etcd error").Build()
	// ErrStorageEtcdDelete is etcd storage delete error
	ErrStorageEtcdDelete = ClassStorage.DefineError().TextualCode("ErrStorageEtcdDelete").MessageTemplate("delete config from storage etcd error").Build()
	// ErrInternalRuleInvalid is invalid rule
	ErrInternalRuleInvalid = ClassInternal.DefineError().TextualCode("ErrInternalRuleInvalid").MessageTemplate("invalid rule found").Build()
	// ErrInternalRuleDuplicate is duplicate rule
	ErrInternalRuleDuplicate = ClassInternal.DefineError().TextualCode("ErrInternalRuleDuplicate").MessageTemplate("duplicate rule found").Build()
	// ErrInternalRuleMismatch is rule mismatch
	ErrInternalRuleMismatch = ClassInternal.DefineError().TextualCode("ErrInternalRuleMismatch").MessageTemplate("rule key mismatch").Build()
	// ErrInternalOperatorNotFound is operator not found
	ErrInternalOperatorNotFound = ClassInternal.DefineError().TextualCode("ErrInternalOperatorNotFound").MessageTemplate("operator not found").Build()
	// ErrInternalOperatorMerge is operator merge error
	ErrInternalOperatorMerge = ClassInternal.DefineError().TextualCode("ErrInternalOperatorMerge").MessageTemplate("merge operator should be pair").Build()
	// ErrInternalOperatorNotStart is operator not start
	ErrInternalOperatorNotStart = ClassInternal.DefineError().TextualCode("ErrInternalOperatorNotStart").MessageTemplate("operator not start").Build()
	// ErrInternalOperatorNotEnd is operator not end
	ErrInternalOperatorNotEnd = ClassInternal.DefineError().TextualCode("ErrInternalOperatorNotEnd").MessageTemplate("operator not end").Build()
	// ErrInternalOperatorStepUnknown is operator step unknown
	ErrInternalOperatorStepUnknown = ClassInternal.DefineError().TextualCode("ErrInternalStepUnknown").MessageTemplate("operator step is unknown").Build()
	// ErrInternalStoreNotFound is store not found
	ErrInternalStoreNotFound = ClassInternal.DefineError().TextualCode("ErrInternalStoreNotFound").MessageTemplate("store id %d not found").Build()
	// ErrInternalClusterVersionChange is cluster version change error
	ErrInternalClusterVersionChange = ClassInternal.DefineError().TextualCode("ErrInternalClusterVersionChange").MessageTemplate("cluster version change same time").Build()
	// ErrInternalRegionKey is region key error
	ErrInternalRegionKey = ClassInternal.DefineError().TextualCode("ErrInternalRegionKey").MessageTemplate("wrong region key range").Build()
	// ErrInternalCacheRegionOverflow is cache region overflow
	ErrInternalCacheRegionOverflow = ClassInternal.DefineError().TextualCode("ErrInternalCacheRegionOverflow").MessageTemplate("cache region overflow").Build()
	// ErrInternalVersionFeatureNotExist is version feature not exist
	ErrInternalVersionFeatureNotExist = ClassInternal.DefineError().TextualCode("ErrInternalVersionFeatureNotExist").MessageTemplate("version feature not exist").Build()
	// ErrFormatParseCmd is parse cmd error
	ErrFormatParseCmd = ClassFormat.DefineError().TextualCode("ErrFormatParseCmd").MessageTemplate("parse cmd error").Build()
	// ErrFormatParseClusterVersion is parse cluster version error
	ErrFormatParseClusterVersion = ClassFormat.DefineError().TextualCode("ErrFormatParseClusterVersion").MessageTemplate("parse cluster version error").Build()
	// ErrFormatParseURL is parse url error
	ErrFormatParseURL = ClassFormat.DefineError().TextualCode("ErrFormatParseURL").MessageTemplate("parse url error").Build()
	// ErrFormatParseHistoryIndex is parse history index error
	ErrFormatParseHistoryIndex = ClassFormat.DefineError().TextualCode("ErrFormatParseHistoryIndex").MessageTemplate("parse history index error").Build()
	// ErrOtherInitLog is init log error
	ErrOtherInitLog = ClassOther.DefineError().TextualCode("ErrOtherInitLog").MessageTemplate("init log fail").Build()
	// ErrOtherDashboardServer is dashboard server error
	ErrOtherDashboardServer = ClassOther.DefineError().TextualCode("ErrOtherDashboardServer").MessageTemplate("dashboard server error").Build()
	// ErrOtherPrometheusPush is prometheus push error
	ErrOtherPrometheusPush = ClassOther.DefineError().TextualCode("ErrOtherPrometheusPush").MessageTemplate("push to prometheus error").Build()
	// ErrOtherPluginLoadActionUnknown is plugin action unknown
	ErrOtherPluginLoadActionUnknown = ClassOther.DefineError().TextualCode("ErrOtherPluginLoadActionUnknown").MessageTemplate("unknown action to load plugin").Build()
	// ErrOtherPluginFuncNotFound is plugin func not found
	ErrOtherPluginFuncNotFound = ClassOther.DefineError().TextualCode("ErrOtherPluginFuncNotFound").MessageTemplate("plugin function not found").Build()
	// ErrOtherSystemTime is system time error
	ErrOtherSystemTime = ClassOther.DefineError().TextualCode("ErrOtherSystemTime").MessageTemplate("system time error").Build()
)

// scheduler
var (
	// ErrGetSourceStore is the error that scheduler get source store failed.
	ErrGetSourceStore  = ClassInternal.DefineError().TextualCode("ErrGetSourceStore").MessageTemplate("failed to get the source store, store id is %s").Build()
	// ErrSchedulerExisted is scheduler duplicate
	ErrSchedulerExisted = ClassInternal.DefineError().TextualCode("ErrSchedulerExisted").MessageTemplate("scheduler existed").Build()
	// ErrSchedulerNotFound is scheduler not found
	ErrSchedulerNotFound = ClassInternal.DefineError().TextualCode("ErrSchedulerNotFound").MessageTemplate("scheduler not found").Build()
	// ErrScheduleConfigNotExist is scheduler config not found
	ErrScheduleConfigNotExist = ClassInternal.DefineError().TextualCode("ErrScheduleConfigNotExist").MessageTemplate("the config does not exist").Build()
	// ErrSchedulerConfig is scheduler config error
	ErrSchedulerConfig = ClassInternal.DefineError().TextualCode("ErrSchedulerConfig").MessageTemplate("wrong scheduler config %s").Build()
	// ErrCacheOverflow is cache overflow error
	ErrCacheOverflow = ClassInternal.DefineError().TextualCode("ErrCacheOverflow").MessageTemplate("cache overflow, scheduler is %s").Build()
	// ErrInternalGrowth is unknown interval growth type error
	ErrInternalGrowth = ClassInternal.DefineError().TextualCode("ErrInternalGrowth").MessageTemplate("unknown interval growth type error").Build()
	// ErrCreateOperator is create operator error
	ErrCreateOperator = ClassInternal.DefineError().TextualCode("ErrCreateOperator").MessageTemplate("fail to create %s operator").Build()
)
