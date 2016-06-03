// Copyright 2016 PingCAP, Inc.
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

package server

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

// NewError returns a Response with go error.
func NewError(err error) *pdpb.Response {
	r := &pdpb.Response{
		Header: &pdpb.ResponseHeader{},
	}

	r.Header.Error = &pdpb.Error{
		Message: proto.String(err.Error()),
	}

	return r
}

// NewErrorf returns a Response with special format message.
func NewErrorf(format string, args ...interface{}) *pdpb.Response {
	return NewError(fmt.Errorf(format, args...))
}

// NewBootstrappedError returns a BootstrappedError response.
func NewBootstrappedError() *pdpb.Response {
	r := &pdpb.Response{
		Header: &pdpb.ResponseHeader{},
	}

	r.Header.Error = &pdpb.Error{
		Message:      proto.String("cluster is already bootstrapped"),
		Bootstrapped: &pdpb.BootstrappedError{},
	}

	return r
}
