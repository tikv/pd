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

package servicediscovery

import (
	"google.golang.org/grpc"

	"github.com/tikv/pd/client/pkg/utils/grpcutil"
)

// Each discovery only checks the service which every endpoint it connects to
// must provide, so a connection to an address reused by another kind of
// service is closed and recreated instead of being reused forever.
var (
	pdExpectedServiceDialOptions              = grpcutil.ExpectedServiceDialOptions("pdpb.PD")
	tsoExpectedServiceDialOptions             = grpcutil.ExpectedServiceDialOptions("tsopb.TSO")
	routerExpectedServiceDialOptions          = grpcutil.ExpectedServiceDialOptions("routerpb.Router")
	resourceManagerExpectedServiceDialOptions = grpcutil.ExpectedServiceDialOptions("resource_manager.ResourceManager")
)

func withExpectedService(dialOpts, expectedServiceOpts []grpc.DialOption) []grpc.DialOption {
	opts := make([]grpc.DialOption, 0, len(dialOpts)+len(expectedServiceOpts))
	opts = append(opts, dialOpts...)
	return append(opts, expectedServiceOpts...)
}
