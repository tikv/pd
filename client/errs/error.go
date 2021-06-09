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

import (
	"github.com/pingcap/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ZapError is used to make the log output easier.
func ZapError(err error, causeError ...error) zap.Field {
	if err == nil {
		return zap.Skip()
	}
	if e, ok := err.(*errors.Error); ok {
		if len(causeError) >= 1 {
			err = e.Wrap(causeError[0]).FastGenWithCause()
		} else {
			err = e.FastGenByArgs()
		}
	}
	return zap.Field{Key: "error", Type: zapcore.ErrorType, Interface: err}
}

const (
	// NotLeaderErr indicates the the non-leader member received the requests which should be received by leader.
	NotLeaderErr = "is not leader"
	// MismatchLeaderErr indicates the the non-leader member received the requests which should be received by leader.
	MismatchLeaderErr = "mismatch leader id"
)

var (
	ErrClientGetLeader       = errors.Normalize("get leader from %v error", errors.RFCCodeText("PD:client:ErrClientGetLeader"))
	ErrClientGetMember       = errors.Normalize("get member failed", errors.RFCCodeText("PD:client:ErrClientGetMember"))
	ErrURLParse              = errors.Normalize("parse url error", errors.RFCCodeText("PD:url:ErrURLParse"))
	ErrGRPCDial              = errors.Normalize("dial error", errors.RFCCodeText("PD:grpc:ErrGRPCDial"))
	ErrEtcdTLSConfig         = errors.Normalize("etcd TLS config error", errors.RFCCodeText("PD:etcd:ErrEtcdTLSConfig"))
	ErrSecurityConfig        = errors.Normalize("security config error: %s", errors.RFCCodeText("PD:grpcutil:ErrSecurityConfig"))
	ErrClientGetTSOTimeout   = errors.Normalize("get TSO timeout", errors.RFCCodeText("PD:client:ErrClientGetTSOTimeout"))
	ErrClientCreateTSOStream = errors.Normalize("create TSO stream failed", errors.RFCCodeText("PD:client:ErrClientCreateTSOStream"))
	ErrClientGetTSO          = errors.Normalize("get TSO failed, %v", errors.RFCCodeText("PD:client:ErrClientGetTSO"))
	ErrCloseGRPCConn         = errors.Normalize("close gRPC connection failed", errors.RFCCodeText("PD:grpc:ErrCloseGRPCConn"))
)
