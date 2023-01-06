package registry_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/mcs/registry"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/tests"
	"go.uber.org/goleak"
	"google.golang.org/grpc"

	"google.golang.org/grpc/test/grpc_testing"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type testServiceRegistry struct {
}

func (t *testServiceRegistry) RegisterGRPCService(g *grpc.Server) {
	grpc_testing.RegisterTestServiceServer(g, &grpc_testing.UnimplementedTestServiceServer{})
}

func (t *testServiceRegistry) RegisterRESTHandler(userDefineHandlers map[string]http.Handler) {
	group := server.APIServiceGroup{
		Name:       "my-http-service",
		Version:    "v1alpha1",
		IsCore:     false,
		PathPrefix: "/my-service",
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Hello World!"))
	})
	server.RegisterUserDefinedHandlers(userDefineHandlers, &group, handler)
}

func newTestServiceRegistry(_ *server.Server) registry.RegistrableService {
	return &testServiceRegistry{}
}

func install(register *registry.ServiceRegistry) {
	register.RegisterService("test", newTestServiceRegistry)
	server.NewServiceRegistry = func() server.ServiceRegistry {
		return register
	}
}

func TestRegistryService(t *testing.T) {
	install(registry.ServerServiceRegistry)
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)

	leaderName := cluster.WaitLeader()
	leader := cluster.GetServer(leaderName)

	// Test registered GRPC Service
	cc, err := grpc.DialContext(ctx, strings.TrimPrefix(leader.GetAddr(), "http://"), grpc.WithInsecure())
	re.NoError(err)
	defer cc.Close()
	grpclient := grpc_testing.NewTestServiceClient(cc)
	resp, err := grpclient.EmptyCall(context.Background(), &grpc_testing.Empty{})
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
}
