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
	"strings"

	"github.com/pingcap/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/codes"
)

// IsLeaderChange will determine whether there is a leader/primary change.
func IsLeaderChange(err error) bool {
	if err == nil {
		return false
	}
	if err == ErrClientTSOStreamClosed {
		return true
	}
	errMsg := err.Error()
	return strings.Contains(errMsg, NoLeaderErr) ||
		strings.Contains(errMsg, NotLeaderErr) ||
		strings.Contains(errMsg, MismatchLeaderErr) ||
		strings.Contains(errMsg, NotServedErr) ||
		strings.Contains(errMsg, NotPrimaryErr)
}

// IsNetworkError returns true if the error is a network error.
func IsNetworkError(code codes.Code) bool {
	return code == codes.Unavailable || code == codes.DeadlineExceeded
}

// ZapError is used to make the log output easier.
func ZapError(err error, causeError ...error) zap.Field {
	if err == nil {
		return zap.Skip()
	}
	if e, ok := err.(*errors.Error); ok {
		if len(causeError) >= 1 {
			err = e.Wrap(causeError[0])
		} else {
			err = e.FastGenByArgs()
		}
	}
	return zap.Field{Key: "error", Type: zapcore.ErrorType, Interface: err}
}
