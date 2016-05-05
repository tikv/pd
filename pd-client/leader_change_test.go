package pd

import (
	"strings"
	"time"

	. "github.com/pingcap/check"
)

var _ = Suite(&testLeaderChangeSuite{})

type testLeaderChangeSuite struct{}

func (s *testLeaderChangeSuite) TestLeaderChange(c *C) {
	srv1 := newServer(c, 1235, "/pd-leader-change", "1")

	// wait for srv1 to become leader
	time.Sleep(time.Second * 3)

	client, err := NewClient(strings.Split(*testEtcd, ","), "/pd-leader-change", "1")
	c.Assert(err, IsNil)
	defer client.Close()

	p1, l1, err := client.GetTS()
	c.Assert(err, IsNil)

	srv2 := newServer(c, 1236, "/pd-leader-change", "1")
	defer srv2.Close()

	// stop srv1, srv2 will become leader
	srv1.Close()

	for i := 0; i < 10; i++ {
		p2, l2, err := client.GetTS()
		if err == nil {
			c.Assert(p1<<18+l1, Less, p2<<18+l2)
			return
		}
		time.Sleep(time.Second)
	}
	c.Error("failed getTS from new leader after 10 seconds")
}
