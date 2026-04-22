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
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/utils/grpcutil"
)

type gcResponseWithHeader interface {
	GetHeader() *pdpb.ResponseHeader
}

func recordGCRequest(method string, requestHeader *pdpb.RequestHeader, response gcResponseWithHeader, err error) {
	var respErr *pdpb.Error
	if response != nil && response.GetHeader() != nil {
		respErr = response.GetHeader().GetError()
	}
	if respErr == nil && err != nil {
		respErr = &pdpb.Error{
			Type:    pdpb.ErrorType_UNKNOWN,
			Message: err.Error(),
		}
	}
	grpcutil.RequestCounter(method, requestHeader, respErr, gcRequestCounter)
}
