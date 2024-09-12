// Copyright 2023 TiKV Project Authors.
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

package registry_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/mcs/registry"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/interop/grpc_testing"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type testServiceRegistry struct {
}

func (*testServiceRegistry) RegisterGRPCService(g *grpc.Server) {
	grpc_testing.RegisterTestServiceServer(g, &grpc_testing.UnimplementedTestServiceServer{})
}

func (*testServiceRegistry) RegisterRESTHandler(userDefineHandlers map[string]http.Handler) error {
	group := apiutil.APIServiceGroup{
		Name:       "my-http-service",
		Version:    "v1alpha1",
		IsCore:     false,
		PathPrefix: "/my-service",
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Hello World!"))
	})
	return apiutil.RegisterUserDefinedHandlers(userDefineHandlers, &group, handler)
}

func newTestServiceRegistry(_ bs.Server) registry.RegistrableService {
	return &testServiceRegistry{}
}

func TestRegistryService(t *testing.T) {
	re := require.New(t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/useGlobalRegistry", "return(true)"))
	registry.ServerServiceRegistry.RegisterService("test", newTestServiceRegistry)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)

	re.NotEmpty(cluster.WaitLeader())
	leader := cluster.GetLeaderServer()

	// Test registered GRPC Service
	cc, err := grpc.DialContext(ctx, strings.TrimPrefix(leader.GetAddr(), "http://"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	re.NoError(err)
	defer cc.Close()
	grpcClient := grpc_testing.NewTestServiceClient(cc)
	resp, err := grpcClient.EmptyCall(context.Background(), &grpc_testing.Empty{})
	re.ErrorContains(err, "Unimplemented")
	re.Nil(resp)

	// Test registered REST HTTP Handler
	resp1, err := http.Get(leader.GetAddr() + "/my-service")
	re.NoError(err)
	defer resp1.Body.Close()
	re.Equal(http.StatusOK, resp1.StatusCode)
	respString, err := io.ReadAll(resp1.Body)
	re.NoError(err)
	re.Equal("Hello World!", string(respString))
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/useGlobalRegistry"))
}
