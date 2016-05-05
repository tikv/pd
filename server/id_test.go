package server

import (
	"math/rand"
	"net"
	"sync"

	"github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/twinj/uuid"
)

var _ = Suite(&testAllocIDSuite{})

type testAllocIDSuite struct {
	client *clientv3.Client
	alloc  *idAllocator
	svr    *Server
}

func (s *testAllocIDSuite) getRootPath() string {
	return "test_alloc_id"
}

func (s *testAllocIDSuite) SetUpSuite(c *C) {
	s.svr = newTestServer(c, s.getRootPath())
	s.client = newEtcdClient(c)
	s.alloc = s.svr.idAlloc

	deleteRoot(c, s.client, s.getRootPath())

	go s.svr.Run()
}

func (s *testAllocIDSuite) TearDownSuite(c *C) {
	s.client.Close()
}

func (s *testAllocIDSuite) TestID(c *C) {
	mustGetLeader(c, s.client, s.svr.getLeaderPath())

	var last uint64
	for i := uint64(0); i < allocStep; i++ {
		id, err := s.alloc.Alloc()
		c.Assert(err, IsNil)
		c.Assert(id, Greater, last)
		last = id
	}

	var wg sync.WaitGroup

	var m sync.Mutex
	ids := make(map[uint64]struct{})

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < 200; i++ {
				id, err := s.alloc.Alloc()
				c.Assert(err, IsNil)
				m.Lock()
				_, ok := ids[id]
				ids[id] = struct{}{}
				m.Unlock()
				c.Assert(ok, IsFalse)
			}
		}()
	}

	wg.Wait()
}

func (s *testAllocIDSuite) TestCommand(c *C) {
	leader := mustGetLeader(c, s.client, s.svr.getLeaderPath())

	conn, err := net.Dial("tcp", leader.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()

	idReq := &pdpb.AllocIdRequest{}

	req := &pdpb.Request{
		CmdType: pdpb.CommandType_AllocId.Enum(),
		Header: &pdpb.RequestHeader{
			Uuid:        uuid.NewV4().Bytes(),
			ClusterName: proto.String("0"),
		},
		AllocId: idReq,
	}

	var last uint64
	for i := uint64(0); i < 2*allocStep; i++ {
		rawMsgID := uint64(rand.Int63())
		sendRequest(c, conn, rawMsgID, req)
		msgID, resp := recvResponse(c, conn)
		c.Assert(rawMsgID, Equals, msgID)
		c.Assert(resp.AllocId, NotNil)
		c.Assert(resp.AllocId.GetId(), Greater, last)
		last = resp.AllocId.GetId()
	}
}
