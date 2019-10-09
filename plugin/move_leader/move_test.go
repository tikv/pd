package main

import (
	"encoding/hex"
	. "github.com/pingcap/check"
	_ "github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/mock/mockcluster"
	"github.com/pingcap/pd/pkg/mock/mockoption"
	"github.com/pingcap/pd/pkg/testutil"
	"github.com/pingcap/pd/server/schedule"
	_ "go.uber.org/zap"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPluginCode(t *testing.T) {
	TestingT(t)
}

type testPluginCodeSuite struct {
	tc *mockcluster.Cluster
	oc *schedule.OperatorController
}

var _ = Suite(&testPluginCodeSuite{})

func (s *testPluginCodeSuite) SetUpTest(c *C) {
	opt := mockoption.NewScheduleOptions()
	s.tc = mockcluster.NewCluster(opt)
	s.oc = schedule.NewOperatorController(nil, nil)
	//Stores:    1    2    3    4    5
	//"zone"    z1	 z2   z3   z4	z5
	//"rock"	r1   r2   r3   r4   r5
	//"host"	h1	 h2   h3   h4   h5
	//Region1:   L    F    F    -    -
	//Region2:   F    L    F    -    -
	//Region3:   -    F    -    L    F
	//Region4:   -    F    L    F    -
	//Region5:   F    -    -    F    L
	//Region6:   -    -    F    F    L
	//Region7:   L    F    -    -    F
	//Region8:   F    -    F    L    -
	//Region9:   -    L    F    -    F
	//Region10:  -    F    L    F    -
	s.tc.PutStoreWithLabels(1, "zone", "z1", "rack", "r1", "host", "h1")
	s.tc.SetStoreUp(1)
	s.tc.PutStoreWithLabels(2, "zone", "z2", "rack", "r2", "host", "h2")
	s.tc.SetStoreUp(2)
	s.tc.PutStoreWithLabels(3, "zone", "z3", "rack", "r3", "host", "h3")
	s.tc.SetStoreUp(3)
	s.tc.PutStoreWithLabels(4, "zone", "z4", "rack", "r4", "host", "h4")
	s.tc.SetStoreUp(4)
	s.tc.PutStoreWithLabels(5, "zone", "z5", "rack", "r5", "host", "h5")
	s.tc.SetStoreUp(5)
	s.tc.AddLeaderRegionWithRange(1,
		"",
		DecodeToString("757365727461626C653A7573657231773937383833313437333137333731323135"), 1, 2, 3)
	s.tc.AddLeaderRegionWithRange(2,
		DecodeToString("757365727461626C653A7573657231773937383833313437333137333731323135"),
		DecodeToString("757365727461626C653A7573657232643637353232383738383832303830353737"), 2, 1, 3)
	s.tc.AddLeaderRegionWithRange(3,
		DecodeToString("757365727461626C653A7573657232643637353232383738383832303830353737"),
		DecodeToString("757365727461626C653A7573657233943637353232383738383832303830353737"), 4, 2, 5)
	s.tc.AddLeaderRegionWithRange(4,
		DecodeToString("757365727461626C653A7573657233943637353232383738383832303830353737"),
		DecodeToString("757365727461626C653A7573657234443637353232383738383832303830353737"), 3, 2, 4)
	s.tc.AddLeaderRegionWithRange(5,
		DecodeToString("757365727461626C653A7573657234443637353232383738383832303830353737"),
		DecodeToString("757365727461626C653A7573657235743637353232383738383832303830353737"), 5, 1, 4)
	s.tc.AddLeaderRegionWithRange(6,
		DecodeToString("757365727461626C653A7573657235743637353232383738383832303830353737"),
		DecodeToString("757365727461626C653A7573657236273036373639353831393732343031333937"), 5, 3, 4)
	s.tc.AddLeaderRegionWithRange(7,
		DecodeToString("757365727461626C653A7573657236273036373639353831393732343031333937"),
		DecodeToString("757365727461626C653A7573657236973036373639353831393732343031333937"), 1, 2, 5)
	s.tc.AddLeaderRegionWithRange(8,
		DecodeToString("757365727461626C653A7573657236973036373639353831393732343031333937"),
		DecodeToString("757365727461626C653A7573657238373036373639353831393732343031333937"), 4, 1, 3)
	s.tc.AddLeaderRegionWithRange(9,
		DecodeToString("757365727461626C653A7573657238373036373639353831393732343031333937"),
		DecodeToString("757365727461626C653A7573657239973036373639353831393732343031333937"), 2, 3, 5)
	s.tc.AddLeaderRegionWithRange(10,
		DecodeToString("757365727461626C653A7573657239973036373639353831393732343031333937"),
		"", 3, 2, 4)

}

func (s *testPluginCodeSuite) TestMoveScheduler(c *C) {
	filePath, err := filepath.Abs("../conf/test_config.toml")
	c.Assert(err, IsNil)
	uc := NewUserConfig()
	c.Assert(uc.LoadConfig(filePath, 3), Equals, true)
	c.Assert(uc, NotNil)


	schedule.PluginsMapLock.RLock()
	names := []string{}
	for name, _ := range schedule.PluginsMap {
		names = append(names, name)
	}
	schedule.PluginsMapLock.RUnlock()

	for _, name := range names {
		ss := strings.Split(name, "-")
		if ss[0] == "Leader" {
			schedule.PluginsMapLock.Lock()
			schedule.PluginsMap[name].UpdateStoreIDs(s.tc)
			lb := newMoveLeaderUserScheduler(s.oc, "move-leader-user-scheduler-"+ss[1],
				schedule.PluginsMap[name].GetKeyStart(), schedule.PluginsMap[name].GetKeyEnd(),
				schedule.PluginsMap[name].GetStoreIDs(), schedule.PluginsMap[name].GetInterval())
			schedule.PluginsMapLock.Unlock()
			c.Assert(lb, NotNil)
			c.Assert(lb.GetType(), Equals, "move-leader-user")
			c.Assert(lb.IsScheduleAllowed(s.tc), Equals, true)
			op := lb.Schedule(s.tc)[0]
			c.Assert(op, NotNil)
			if ss[1] == "0" {
				//transferLeader
				//region2 leader form store 2 to 3
				c.Assert(lb.GetName(), Equals, "move-leader-user-scheduler-0")
				testutil.CheckTransferLeader(c, op, schedule.OpLeader, 2, 3)
				c.Assert(op.RegionID(), Equals, uint64(2))
			} else if ss[1] == "1" {
				//moveLeader
				//region4 leader from store 3 to 1
				//addPeer 2 steps; removePeer 2 steps; transferLeader 1 step
				c.Assert(lb.GetName(), Equals, "move-leader-user-scheduler-1")
				c.Assert(op.Len(), Equals, 5)
				c.Assert(op.Kind()&schedule.OpLeader, Equals, schedule.OpLeader)
				c.Assert(op.RegionID(), Equals, uint64(4))
			}
		}
		if ss[0] == "Region" {
			schedule.PluginsMapLock.Lock()
			schedule.PluginsMap[name].UpdateStoreIDs(s.tc)
			lb := newMoveRegionUserScheduler(s.oc, "move-region-user-scheduler-"+ss[1],
				schedule.PluginsMap[name].GetKeyStart(), schedule.PluginsMap[name].GetKeyEnd(),
				schedule.PluginsMap[name].GetStoreIDs(), schedule.PluginsMap[name].GetInterval())
			schedule.PluginsMapLock.Unlock()
			c.Assert(lb, NotNil)
			c.Assert(lb.GetType(), Equals, "move-region-user")
			c.Assert(lb.IsScheduleAllowed(s.tc), Equals, true)
			op := lb.Schedule(s.tc)[0]
			c.Assert(op, NotNil)
			if ss[1] == "0" {
				//move region6 to store 1 2 3
				//2 addPeer 4 steps; transferLeader 1 step;2 removePeer 2 steps
				c.Assert(lb.GetName(), Equals, "move-region-user-scheduler-0")
				c.Assert(op.Len(), Equals, 7)
				c.Assert(op.Kind()&schedule.OpRegion, Equals, schedule.OpRegion)
				c.Assert(op.RegionID(), Equals, uint64(6))
			} else if ss[1] == "1" {
				//move region8 to store 4 5
				//addPeer 2 steps; removePeer 1 step
				c.Assert(lb.GetName(), Equals, "move-region-user-scheduler-1")
				c.Assert(op.Len(), Equals, 3)
				c.Assert(op.Kind()&schedule.OpRegion, Equals, schedule.OpRegion)
				c.Assert(op.RegionID(), Equals, uint64(8))
			}
		}
	}
	// test move-leader and move-region schedulers adjustable conflict
	filePath, err = filepath.Abs("../conf/test_conflict2.toml")
	c.Assert(err, IsNil)
	uc = NewUserConfig()
	c.Assert(uc.LoadConfig(filePath, 3), Equals, true)
	c.Assert(uc, NotNil)
	schedule.PluginsMapLock.Lock()
	name := "Leader-1"
	schedule.PluginsMap[name].UpdateStoreIDs(s.tc)
	lb := newMoveLeaderUserScheduler(s.oc, "move-leader-user-scheduler-"+"2",
		schedule.PluginsMap[name].GetKeyStart(), schedule.PluginsMap[name].GetKeyEnd(),
		schedule.PluginsMap[name].GetStoreIDs(), schedule.PluginsMap[name].GetInterval())
	schedule.PluginsMapLock.Unlock()
	c.Assert(lb, NotNil)
	c.Assert(lb.GetType(), Equals, "move-leader-user")
	c.Assert(lb.IsScheduleAllowed(s.tc), Equals, true)
	ops := lb.Schedule(s.tc)
	c.Assert(ops, NotNil)
}

func DecodeToString(source string) string {
	key, _ := hex.DecodeString(source)
	return string(key)
}

func (s *testPluginCodeSuite) TestFilter(c *C) {
	s.tc.AddLeaderStore(11, 3)
	s.tc.AddLeaderStore(12, 3)
	s.tc.AddLeaderStore(13, 3)
	s.tc.AddLeaderRegion(21, 11, 12, 13)
	s.tc.AddLeaderRegion(22, 12, 11, 13)
	startTime, _ := time.ParseInLocation("2006-01-02 15:04:05", "2019-01-02 15:04:05", time.Local)
	endTime, _ := time.ParseInLocation("2006-01-02 15:04:05", "2019-10-02 15:04:05", time.Local)
	interval := schedule.TimeInterval{
		Begin: startTime,
		End:   endTime,
	}
	filter := NewViolentFilter()
	c.Assert(filter, NotNil)
	c.Assert(filter.Type(), Equals, "violent-filter")
	c.Assert(filter.FilterSource(s.tc, s.tc.GetRegion(21), &interval, []uint64{21, 22, 23}), Equals, true)
	c.Assert(filter.FilterSource(s.tc, s.tc.GetRegion(21), &interval, []uint64{20, 22, 23}), Equals, false)
	c.Assert(filter.FilterTarget(s.tc, s.tc.GetRegion(22), &interval, []uint64{21, 22, 23}), Equals, true)
	c.Assert(filter.FilterTarget(s.tc, s.tc.GetRegion(22), &interval, []uint64{1, 2, 3}), Equals, false)
}

func (s *testPluginCodeSuite) TestBaseScheduler(c *C) {
	lb := newUserBaseScheduler(s.oc)
	c.Assert(lb, NotNil)
	c.Assert(lb.GetMinInterval(), Equals, MinScheduleInterval)
	interval := time.Duration(1000)
	c.Assert(lb.GetNextInterval(interval), Equals, time.Duration(1300))
}

//test structure.go 
//func GetRegionIDs()
func (s *testPluginCodeSuite) TestGetRegionIDs(c *C) {
	regionIDs := schedule.GetRegionIDs(s.tc,"757365727461626C653A7573657231773937383833313437333137333731323135",
		"757365727461626C653A7573657234443637353232383738383832303830353737")
	c.Assert(len(regionIDs), Equals, 3)
	c.Assert(regionIDs[0], Equals, uint64(2))
	c.Assert(regionIDs[1], Equals, uint64(3))
	c.Assert(regionIDs[2], Equals, uint64(4))
}

//test structure.go 
//func GetStoreByLabel()
func (s *testPluginCodeSuite) TestGetStoreByLabel(c *C) {
	label1 := schedule.Label{Key: "zone", Value: "z1"}
	label2 := schedule.Label{Key: "rack", Value: "r1"}
	label3 := schedule.Label{Key: "host", Value: "h1"}
	c.Assert(schedule.GetStoreByLabel(s.tc, []schedule.Label{label1,label2,label3}), NotNil)
}

//func (s *testPluginCodeSuite) TestGetFunction(c *C) {
//	f, err := schedule.GetFunction("../plugin/userConfigPlugin.so", "NewUserConfig")
//	c.Assert(err, IsNil)
//	c.Assert(f, NotNil)
//}