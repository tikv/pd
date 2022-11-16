// Copyright 2020 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"log"
	"strings"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func trimHTTPPrefix(str string) string {
	str = strings.TrimPrefix(str, "http://")
	str = strings.TrimPrefix(str, "https://")
	return str
}

func newClient(addr string) rmpb.ResourceManagerClient {
	addr = trimHTTPPrefix(addr)
	cc, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatal("failed to create gRPC connection", zap.Error(err))
	}
	return rmpb.NewResourceManagerClient(cc)
}

// TODO: implement the client and integrate it with the pd client.
