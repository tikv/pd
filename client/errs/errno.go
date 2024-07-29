// Copyright 2022 TiKV Project Authors.
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

import (
	"fmt"

	"github.com/pingcap/errors"
)

// Note: keep the same as the ones defined on the server side to ensure the client can use them correctly.
const (
	// NoLeaderErr indicates there is no leader in the cluster currently.
	NoLeaderErr = "no leader"
	// NotLeaderErr indicates the non-leader member received the requests which should be received by leader.
	NotLeaderErr = "not leader"
	// MismatchLeaderErr indicates the non-leader member received the requests which should be received by leader.
	MismatchLeaderErr = "mismatch leader id"
	// NotServedErr indicates an tso node/pod received the requests for the keyspace groups which are not served by it.
	NotServedErr = "is not served"
	// RetryTimeoutErr indicates the server is busy.
	RetryTimeoutErr = "retry timeout"
	// NotPrimaryErr indicates the non-primary member received the requests which should be received by primary.
	NotPrimaryErr = "not primary"
)

// client errors
var (
	ErrClientGetProtoClient           = errors.Normalize("failed to get proto client", errors.RFCCodeText("PD:client:ErrClientGetProtoClient"))
	ErrClientGetMetaStorageClient     = errors.Normalize("failed to get meta storage client", errors.RFCCodeText("PD:client:ErrClientGetMetaStorageClient"))
	ErrClientCreateTSOStream          = errors.Normalize("create TSO stream failed, %s", errors.RFCCodeText("PD:client:ErrClientCreateTSOStream"))
	ErrClientTSOStreamClosed          = errors.Normalize("encountered TSO stream being closed unexpectedly", errors.RFCCodeText("PD:client:ErrClientTSOStreamClosed"))
	ErrClientGetTSOTimeout            = errors.Normalize("get TSO timeout", errors.RFCCodeText("PD:client:ErrClientGetTSOTimeout"))
	ErrClientGetTSO                   = errors.Normalize("get TSO failed, %v", errors.RFCCodeText("PD:client:ErrClientGetTSO"))
	ErrClientGetMinTSO                = errors.Normalize("get min TSO failed, %v", errors.RFCCodeText("PD:client:ErrClientGetMinTSO"))
	ErrClientGetLeader                = errors.Normalize("get leader failed, %v", errors.RFCCodeText("PD:client:ErrClientGetLeader"))
	ErrClientGetMember                = errors.Normalize("get member failed", errors.RFCCodeText("PD:client:ErrClientGetMember"))
	ErrClientGetClusterInfo           = errors.Normalize("get cluster info failed", errors.RFCCodeText("PD:client:ErrClientGetClusterInfo"))
	ErrClientUpdateMember             = errors.Normalize("update member failed, %v", errors.RFCCodeText("PD:client:ErrUpdateMember"))
	ErrClientNoAvailableMember        = errors.Normalize("no available member", errors.RFCCodeText("PD:client:ErrClientNoAvailableMember"))
	ErrClientNoTargetMember           = errors.Normalize("no target member", errors.RFCCodeText("PD:client:ErrClientNoTargetMember"))
	ErrClientProtoUnmarshal           = errors.Normalize("failed to unmarshal proto", errors.RFCCodeText("PD:proto:ErrClientProtoUnmarshal"))
	ErrClientGetMultiResponse         = errors.Normalize("get invalid value response %v, must only one", errors.RFCCodeText("PD:client:ErrClientGetMultiResponse"))
	ErrClientGetServingEndpoint       = errors.Normalize("get serving endpoint failed", errors.RFCCodeText("PD:client:ErrClientGetServingEndpoint"))
	ErrClientFindGroupByKeyspaceID    = errors.Normalize("can't find keyspace group by keyspace id", errors.RFCCodeText("PD:client:ErrClientFindGroupByKeyspaceID"))
	ErrClientWatchGCSafePointV2Stream = errors.Normalize("watch gc safe point v2 stream failed", errors.RFCCodeText("PD:client:ErrClientWatchGCSafePointV2Stream"))
)

// grpcutil errors
var (
	ErrSecurityConfig = errors.Normalize("security config error: %s", errors.RFCCodeText("PD:grpcutil:ErrSecurityConfig"))
)

// The third-party project error.
// url errors
var (
	ErrURLParse = errors.Normalize("parse url error", errors.RFCCodeText("PD:url:ErrURLParse"))
)

// grpc errors
var (
	ErrGRPCDial      = errors.Normalize("dial error", errors.RFCCodeText("PD:grpc:ErrGRPCDial"))
	ErrCloseGRPCConn = errors.Normalize("close gRPC connection failed", errors.RFCCodeText("PD:grpc:ErrCloseGRPCConn"))
)

// etcd errors
var (
	ErrEtcdTLSConfig = errors.Normalize("etcd TLS config error", errors.RFCCodeText("PD:etcd:ErrEtcdTLSConfig"))
)

// crypto
var (
	ErrCryptoX509KeyPair        = errors.Normalize("x509 keypair error", errors.RFCCodeText("PD:crypto:ErrCryptoX509KeyPair"))
	ErrCryptoAppendCertsFromPEM = errors.Normalize("cert pool append certs error", errors.RFCCodeText("PD:crypto:ErrCryptoAppendCertsFromPEM"))
)

// resource group errors
var (
	ErrClientListResourceGroup              = errors.Normalize("get all resource group failed, %v", errors.RFCCodeText("PD:client:ErrClientListResourceGroup"))
	ErrClientResourceGroupConfigUnavailable = errors.Normalize("resource group config is unavailable, %v", errors.RFCCodeText("PD:client:ErrClientResourceGroupConfigUnavailable"))
	ErrClientResourceGroupThrottled         = errors.Normalize("exceeded resource group quota limitation, estimated wait time %s, ltb state is %.2f:%.2f", errors.RFCCodeText("PD:client:ErrClientResourceGroupThrottled"))
)

// ErrClientGetResourceGroup is the error type for getting resource group.
type ErrClientGetResourceGroup struct {
	ResourceGroupName string
	Cause             string
}

func (e *ErrClientGetResourceGroup) Error() string {
	return fmt.Sprintf("get resource group %s failed, %s", e.ResourceGroupName, e.Cause)
}
