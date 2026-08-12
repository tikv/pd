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
)

func TestContextErrorToGRPCStatus(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		code codes.Code
	}{
		{
			name: "canceled",
			err:  context.Canceled,
			code: codes.Canceled,
		},
		{
			name: "wrapped-canceled",
			err:  fmt.Errorf("read GC state: %w", context.Canceled),
			code: codes.Canceled,
		},
		{
			name: "deadline",
			err:  context.DeadlineExceeded,
			code: codes.DeadlineExceeded,
		},
		{
			name: "wrapped-deadline",
			err:  fmt.Errorf("read GC state: %w", context.DeadlineExceeded),
			code: codes.DeadlineExceeded,
		},
		{
			name: "storage-error",
			err:  errors.New("storage failed"),
			code: codes.OK,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			statusErr := contextErrorToGRPCStatus(testCase.err)
			if testCase.code == codes.OK {
				require.NoError(t, statusErr)
				return
			}
			require.Equal(t, testCase.code, status.Code(statusErr))
		})
	}
}
