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

// newError returns a Response with go error.
func newError(err error) *pdpb.Response {
	r := &pdpb.Response{
		Header: &pdpb.ResponseHeader{},
	}

	r.Header.Error = &pdpb.Error{
		Message: proto.String(err.Error()),
	}

	return r
}

// newErrorf returns a Response with special format message.
func newErrorf(format string, args ...interface{}) *pdpb.Response {
	return newError(fmt.Errorf(format, args...))
}

// newBootstrappedError returns a BootstrappedError response.
func newBootstrappedError() *pdpb.Response {
	r := &pdpb.Response{
		Header: &pdpb.ResponseHeader{},
	}

	r.Header.Error = &pdpb.Error{
		Message:      proto.String("cluster is already bootstrapped"),
		Bootstrapped: &pdpb.BootstrappedError{},
	}

	return r
}

func newStoreIsTombstoneError() *pdpb.Response {
	r := &pdpb.Response{
		Header: &pdpb.ResponseHeader{},
	}

	r.Header.Error = &pdpb.Error{
		Message:     proto.String("store is tombstone"),
		IsTombstone: &pdpb.StoreIsTombstoneError{},
	}

	return r
}

func newRegionIsShutdownError() *pdpb.Response {
	r := &pdpb.Response{
		Header: &pdpb.ResponseHeader{},
	}

	r.Header.Error = &pdpb.Error{
		Message:    proto.String("region is shutdown"),
		IsShutdown: &pdpb.RegionIsShutdownError{},
	}

	return r
}
