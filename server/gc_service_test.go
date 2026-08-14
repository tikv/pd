// Copyright 2026 TiKV Project Authors.
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

package server

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tikv/pd/pkg/errs"
)

func TestContextErrorToGRPCStatus(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	deadlineCtx, deadlineCancel := context.WithTimeout(context.Background(), 0)
	defer deadlineCancel()

	testCases := []struct {
		name string
		ctx  context.Context
		err  error
		code codes.Code
	}{
		{
			name: "canceled-request-matching-wrapped-error",
			ctx:  canceledCtx,
			err:  fmt.Errorf("read GC state: %w", context.Canceled),
			code: codes.Canceled,
		},
		{
			name: "expired-request-matching-wrapped-error",
			ctx:  deadlineCtx,
			err:  fmt.Errorf("read GC state: %w", context.DeadlineExceeded),
			code: codes.DeadlineExceeded,
		},
		{
			name: "canceled-request-mismatched-error",
			ctx:  canceledCtx,
			err:  fmt.Errorf("read GC state: %w", context.DeadlineExceeded),
			code: codes.OK,
		},
		{
			name: "canceled-request-unrelated-error",
			ctx:  canceledCtx,
			err:  errors.New("storage failed"),
			code: codes.OK,
		},
		{
			name: "active-request-internal-etcd-get-deadline",
			ctx:  context.Background(),
			err: errs.ErrEtcdKVGet.
				Wrap(context.DeadlineExceeded).
				GenWithStackByCause(),
			code: codes.OK,
		},
		{
			name: "active-request-internal-etcd-txn-deadline",
			ctx:  context.Background(),
			err: errs.ErrEtcdTxnInternal.
				Wrap(context.DeadlineExceeded).
				GenWithStackByArgs(),
			code: codes.OK,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			statusErr := contextErrorToGRPCStatus(testCase.ctx, testCase.err)
			if testCase.code == codes.OK {
				require.NoError(t, statusErr)
				return
			}
			require.Equal(t, testCase.code, status.Code(statusErr))
			require.Equal(t, testCase.ctx.Err().Error(), status.Convert(statusErr).Message())
		})
	}
}
