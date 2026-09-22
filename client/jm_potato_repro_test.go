package pd

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/pingcap/kvproto/pkg/pdpb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/stretchr/testify/require"
	sd "github.com/tikv/pd/client/servicediscovery"
	"google.golang.org/grpc"
)

// JmPotato's repro: a synchronous membership check during a service mode
// switch observes a new PD leader; the registered leader callback must not
// reenter the service-mode write lock it is running under.
type manualDiscovery struct{ sd.ServiceDiscovery }

func (*manualDiscovery) Init() error { return nil }

type membersServer struct {
	pdpb.UnimplementedPDServer
	leader atomic.Pointer[pdpb.Member]
}

func (s *membersServer) GetMembers(context.Context, *pdpb.GetMembersRequest) (*pdpb.GetMembersResponse, error) {
	m := s.leader.Load()
	return &pdpb.GetMembersResponse{Header: &pdpb.ResponseHeader{}, Members: []*pdpb.Member{m}, Leader: m}, nil
}

// TestBakeModeSwitchObservesNewPDLeader reproduces the P1 deadlock
// reported on PR #11227: a PD leader change observed synchronously during a
// service-mode switch must not reenter the mode-switch write lock. The
// leader callback reads the lock-free RM discovery pointer, so it must be
// set independently of the mode-switch critical section.
func TestBakeModeSwitchObservesNewPDLeader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	t.Cleanup(server.Stop)
	members := &membersServer{}
	oldURL := "http://" + listener.Addr().String()
	_, port, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	members.leader.Store(&pdpb.Member{MemberId: 1, ClientUrls: []string{oldURL}})
	pdpb.RegisterPDServer(server, members)
	rmpb.RegisterResourceManagerServer(server, &testRMServer{id: "pd"})
	go func() { _ = server.Serve(listener) }()

	discovery := sd.NewDefaultServiceDiscovery(ctx, cancel, []string{oldURL}, nil)
	t.Cleanup(discovery.Close)
	require.NoError(t, discovery.CheckMemberChanged())

	inner := newInnerClientForRMRouteTest(t, ctx, oldURL)
	inner.serviceDiscovery = &manualDiscovery{ServiceDiscovery: discovery}
	require.NoError(t, inner.setup())
	t.Cleanup(func() {
		inner.tokenDispatcher.dispatcherCancel()
		inner.wg.Wait()
	})

	inner.Lock()
	inner.serviceMode = pdpb.ServiceMode_API_SVC_MODE
	inner.Unlock()
	members.leader.Store(&pdpb.Member{MemberId: 2, ClientUrls: []string{"http://localhost:" + port}})
	inner.setServiceMode(pdpb.ServiceMode_PD_SVC_MODE)
	t.Cleanup(func() {
		if inner.tsoClient != nil {
			inner.tsoClient.Close()
		}
	})
	inner.RLock()
	require.EqualValues(t, pdpb.ServiceMode_PD_SVC_MODE, inner.serviceMode)
	inner.RUnlock()
}
