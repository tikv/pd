package server

import (
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/util"
	"github.com/twinj/uuid"
)

var _ = Suite(&testTsoSuite{})

type testTsoSuite struct {
	client *clientv3.Client
	svr    *Server
}

func (s *testTsoSuite) getRootPath() string {
	return "test_tso"
}

func (s *testTsoSuite) SetUpSuite(c *C) {
	s.svr = newTestServer(c, s.getRootPath())
	s.client = newEtcdClient(c)
	deleteRoot(c, s.client, s.getRootPath())

	go s.svr.Run()
}

func (s *testTsoSuite) TearDownSuite(c *C) {
	s.svr.Close()
	s.client.Close()
}

func sendRequest(c *C, conn net.Conn, msgID uint64, request *pdpb.Request) {
	err := util.WriteMessage(conn, msgID, request)
	c.Assert(err, IsNil)
}

func recvResponse(c *C, conn net.Conn) (uint64, *pdpb.Response) {
	resp := &pdpb.Response{}
	msgID, err := util.ReadMessage(conn, resp)
	c.Assert(err, IsNil)
	return msgID, resp
}

func (s *testTsoSuite) testGetTimestamp(c *C, conn net.Conn, n int) {
	tso := &pdpb.TsoRequest{
		Number: proto.Uint32(uint32(n)),
	}

	req := &pdpb.Request{
		CmdType: pdpb.CommandType_Tso.Enum(),
		Header: &pdpb.RequestHeader{
			Uuid:        uuid.NewV4().Bytes(),
			ClusterName: proto.String("0"),
		},
		Tso: tso,
	}

	rawMsgID := uint64(rand.Int63())
	sendRequest(c, conn, rawMsgID, req)
	msgID, resp := recvResponse(c, conn)
	c.Assert(rawMsgID, Equals, msgID)
	c.Assert(resp.Tso, NotNil)
	c.Assert(resp.Tso.Timestamps, HasLen, n)

	res := resp.Tso.Timestamps
	last := pdpb.Timestamp{}
	for i := 0; i < n; i++ {
		c.Assert(res[i].GetPhysical(), GreaterEqual, last.GetPhysical())
		if res[i].GetPhysical() == last.GetPhysical() {
			c.Assert(res[i].GetLogical(), Greater, last.GetLogical())
		}

		last = *res[i]
	}
}

func mustGetLeader(c *C, client *clientv3.Client, leaderPath string) *pdpb.Leader {
	for i := 0; i < 10; i++ {
		leader, err := GetLeader(client, leaderPath)
		c.Assert(err, IsNil)
		if leader != nil {
			return leader
		}
		time.Sleep(500 * time.Millisecond)
	}

	c.Fatal("get leader error")
	return nil
}

func (s *testTsoSuite) TestTso(c *C) {
	leader := mustGetLeader(c, s.client, s.svr.getLeaderPath())

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			conn, err := net.Dial("tcp", leader.GetAddr())
			c.Assert(err, IsNil)
			defer conn.Close()

			s.testGetTimestamp(c, conn, 10)
		}()
	}

	wg.Wait()
}
