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
	// ErrIOJoinConfig is io join config error
	ErrIOJoinConfig = ClassIO.DefineError().TextualCode("ErrIOJoinConfig").MessageTemplate("config io error during join").Done()
	// ErrHTTPContentConfig is http content config error
	ErrHTTPContentConfig = ClassIO.DefineError().TextualCode("ErrIOHTTPContent").MessageTemplate("http content io error").Done()
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
	// InternalSchedulerDuplicate is scheduler duplicate
	InternalSchedulerDuplicate = ClassInternal.DefineError().TextualCode("InternalSchedulerDuplicate").MessageTemplate("duplicate scheduler found").Done()
	// InternalSchedulerNotFound is scheduler not found
	InternalSchedulerNotFound = ClassInternal.DefineError().TextualCode("InternalSchedulerNotFound").MessageTemplate("scheduler not found").Done()
	// ErrInternalSchedulerConfig is scheduler config error
	ErrInternalSchedulerConfig = ClassInternal.DefineError().TextualCode("ErrInternalSchedulerConfig").MessageTemplate("wrong scheduler config").Done()
	// InternalRuleInvalid is invalid rule
	InternalRuleInvalid = ClassInternal.DefineError().TextualCode("InternalRuleInvalid").MessageTemplate("invalid rule found").Done()
	// InternalRuleDuplicate is duplicate rule
	InternalRuleDuplicate = ClassInternal.DefineError().TextualCode("InternalRuleDuplicate").MessageTemplate("duplicate rule found").Done()
	// InternalRuleMismatch is rule mismatch
	InternalRuleMismatch = ClassInternal.DefineError().TextualCode("InternalRuleMismatch").MessageTemplate("rule key mismatch").Done()
	// InternalOperatorNotFound is operator not found
	InternalOperatorNotFound = ClassInternal.DefineError().TextualCode("InternalOperatorNotFound").MessageTemplate("operator not found").Done()
	// InternalOperatorOrphan is operator orphan
	InternalOperatorOrphan = ClassInternal.DefineError().TextualCode("InternalOperatorOrphan").MessageTemplate("operator is orphan").Done()
	// ErrInternalOperatorMerge is operator merge error
	ErrInternalOperatorMerge = ClassInternal.DefineError().TextualCode("ErrInternalOperatorMerge").MessageTemplate("operator cannot merge").Done()
	// InternalOperatorNotStart is operator not start
	InternalOperatorNotStart = ClassInternal.DefineError().TextualCode("InternalOperatorNotStart").MessageTemplate("operator not start").Done()
	// InternalOperatorNotEnd is operator not end
	InternalOperatorNotEnd = ClassInternal.DefineError().TextualCode("InternalOperatorNotEnd").MessageTemplate("operator not end").Done()
	// InternalOperatorStepUnknown is operator step unknown
	InternalOperatorStepUnknown = ClassInternal.DefineError().TextualCode("InternalStepUnknown").MessageTemplate("operator step is unknown").Done()
	// InternalStoreNotFound is store not found
	InternalStoreNotFound = ClassInternal.DefineError().TextualCode("InternalStoreNotFound").MessageTemplate("store id %d not found").Done()
	// ErrInternalClusterVersionChange is cluster version change error
	ErrInternalClusterVersionChange = ClassInternal.DefineError().TextualCode("InternalClusterVersionChange").MessageTemplate("cluster version change same time").Done()
	// ErrInternalRegionKey is region key error
	ErrInternalRegionKey = ClassInternal.DefineError().TextualCode("ErrInternalRegionKey").MessageTemplate("wrong region key range").Done()
	// InternalCacheRegionOverflow is cache region overflow
	InternalCacheRegionOverflow = ClassInternal.DefineError().TextualCode("InternalCacheRegionOverflow").MessageTemplate("cache region overflow").Done()
	// InternalVersionFeatureNotExist is version feature not exist
	InternalVersionFeatureNotExist = ClassInternal.DefineError().TextualCode("InternalVersionFeatureNotExist").MessageTemplate("version feature not exist").Done()
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
	// OtherPluginLoadActionUnknown is plugin action unknown
	OtherPluginLoadActionUnknown = ClassOther.DefineError().TextualCode("OtherPluginLoadActionUnknown").MessageTemplate("unknown action to load plugin").Done()
	// OtherPluginFuncNotFound is plugin func not found
	OtherPluginFuncNotFound = ClassOther.DefineError().TextualCode("OtherPluginFuncNotFound").MessageTemplate("plugin function not found").Done()
	// ErrOtherSystemTime is system time error
	ErrOtherSystemTime = ClassOther.DefineError().TextualCode("ErrOtherSystemTime").MessageTemplate("system time error").Done()
)
