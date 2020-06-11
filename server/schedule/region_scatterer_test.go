package schedule

import (
	"context"
	. "github.com/pingcap/check"
	"github.com/pingcap/pd/v4/pkg/mock/mockcluster"
	"github.com/pingcap/pd/v4/pkg/mock/mockhbstream"
	"github.com/pingcap/pd/v4/pkg/mock/mockoption"
	"github.com/pingcap/pd/v4/server/schedule/operator"
	"github.com/pingcap/pd/v4/server/schedule/placement"
)

type sequencer struct {
	minID uint64
	maxID uint64
	curID uint64
}

func newSequencer(maxID uint64) *sequencer {
	return newSequencerWithMinID(1, maxID)
}

func newSequencerWithMinID(minID, maxID uint64) *sequencer {
	return &sequencer{
		minID: minID,
		maxID: maxID,
		curID: maxID,
	}
}

func (s *sequencer) next() uint64 {
	s.curID++
	if s.curID > s.maxID {
		s.curID = s.minID
	}
	return s.curID
}

var _ = Suite(&testScatterRegionSuite{})

type testScatterRegionSuite struct{}

func (s *testScatterRegionSuite) TestSixStores(c *C) {
	s.scatter(c, 6, 4, false)
	s.scatter(c, 6, 4, true)
}

func (s *testScatterRegionSuite) TestFiveStores(c *C) {
	s.scatter(c, 5, 5, false)
	s.scatter(c, 5, 5, true)
}

func (s *testScatterRegionSuite) TestSixSpecialStores(c *C) {
	s.scatterSpecial(c, 3, 6, 4)
}

func (s *testScatterRegionSuite) TestFiveSpecialStores(c *C) {
	s.scatterSpecial(c, 5, 5, 5)
}

func (s *testScatterRegionSuite) checkOperator(op *operator.Operator, c *C) {
	for i := 0; i < op.Len(); i++ {
		if rp, ok := op.Step(i).(operator.RemovePeer); ok {
			for j := i + 1; j < op.Len(); j++ {
				if tr, ok := op.Step(j).(operator.TransferLeader); ok {
					c.Assert(rp.FromStore, Not(Equals), tr.FromStore)
					c.Assert(rp.FromStore, Not(Equals), tr.ToStore)
				}
			}
		}
	}
}

func (s *testScatterRegionSuite) scatter(c *C, numStores, numRegions uint64, useRules bool) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)

	// Add ordinary stores.
	for i := uint64(1); i <= numStores; i++ {
		tc.AddRegionStore(i, 0)
	}
	tc.EnablePlacementRules = useRules

	seq := newSequencer(numStores)
	// Region 1 has the same distribution with the Region 2, which is used to test selectPeerToReplace.
	tc.AddLeaderRegion(1, 1, 2, 3)
	for i := uint64(2); i <= numRegions; i++ {
		tc.AddLeaderRegion(i, seq.next(), seq.next(), seq.next())
	}

	scatterer := NewRegionScatterer(tc)

	for i := uint64(1); i <= numRegions; i++ {
		region := tc.GetRegion(i)
		if op, _ := scatterer.Scatter(region); op != nil {
			s.checkOperator(op, c)
			ApplyOperator(tc, op)
		}
	}

	countPeers := make(map[uint64]uint64)
	for i := uint64(1); i <= numRegions; i++ {
		region := tc.GetRegion(i)
		for _, peer := range region.GetPeers() {
			countPeers[peer.GetStoreId()]++
		}
	}

	// Each store should have the same number of peers.
	for _, count := range countPeers {
		c.Assert(count, Equals, numRegions*3/numStores)
	}
}

func (s *testScatterRegionSuite) scatterSpecial(c *C, numOrdinaryStores, numSpecialStores, numRegions uint64) {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)

	// Add ordinary stores.
	for i := uint64(1); i <= numOrdinaryStores; i++ {
		tc.AddRegionStore(i, 0)
	}
	// Add special stores.
	for i := uint64(1); i <= numSpecialStores; i++ {
		tc.AddLabelsStore(numOrdinaryStores+i, 0, map[string]string{"engine": "tiflash"})
	}
	tc.EnablePlacementRules = true
	c.Assert(tc.RuleManager.SetRule(&placement.Rule{
		GroupID: "pd", ID: "learner", Role: placement.Learner, Count: 3,
		LabelConstraints: []placement.LabelConstraint{{Key: "engine", Op: placement.In, Values: []string{"tiflash"}}}}), IsNil)

	ordinarySeq := newSequencer(numOrdinaryStores)
	specialSeq := newSequencerWithMinID(numOrdinaryStores+1, numOrdinaryStores+numSpecialStores)
	// Region 1 has the same distribution with the Region 2, which is used to test selectPeerToReplace.
	tc.AddRegionWithLearner(1, 1, []uint64{2, 3}, []uint64{numOrdinaryStores + 1, numOrdinaryStores + 2, numOrdinaryStores + 3})
	for i := uint64(2); i <= numRegions; i++ {
		tc.AddRegionWithLearner(
			i,
			ordinarySeq.next(),
			[]uint64{ordinarySeq.next(), ordinarySeq.next()},
			[]uint64{specialSeq.next(), specialSeq.next(), specialSeq.next()},
		)
	}

	scatterer := NewRegionScatterer(tc)

	for i := uint64(1); i <= numRegions; i++ {
		region := tc.GetRegion(i)
		if op, _ := scatterer.Scatter(region); op != nil {
			s.checkOperator(op, c)
			ApplyOperator(tc, op)
		}
	}

	countOrdinaryPeers := make(map[uint64]uint64)
	countSpecialPeers := make(map[uint64]uint64)
	for i := uint64(1); i <= numRegions; i++ {
		region := tc.GetRegion(i)
		for _, peer := range region.GetPeers() {
			storeID := peer.GetStoreId()
			store := tc.Stores.GetStore(storeID)
			if store.GetLabelValue("engine") == "tiflash" {
				countSpecialPeers[storeID]++
			} else {
				countOrdinaryPeers[storeID]++
			}
		}
	}

	// Each store should have the same number of peers.
	for _, count := range countOrdinaryPeers {
		c.Assert(count, Equals, numRegions*3/numOrdinaryStores)
	}
	for _, count := range countSpecialPeers {
		c.Assert(count, Equals, numRegions*3/numSpecialStores)
	}
}

func (s *testScatterRegionSuite) TestStoreLimit(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	oc := NewOperatorController(ctx, tc, mockhbstream.NewHeartbeatStream())

	// Add stores 1~6.
	for i := uint64(1); i <= 5; i++ {
		tc.AddRegionStore(i, 0)
	}

	// Add regions 1~4.
	seq := newSequencer(3)
	// Region 1 has the same distribution with the Region 2, which is used to test selectPeerToReplace.
	tc.AddLeaderRegion(1, 1, 2, 3)
	for i := uint64(2); i <= 5; i++ {
		tc.AddLeaderRegion(i, seq.next(), seq.next(), seq.next())
	}

	scatterer := NewRegionScatterer(tc)

	for i := uint64(1); i <= 5; i++ {
		region := tc.GetRegion(i)
		if op, _ := scatterer.Scatter(region); op != nil {
			c.Assert(oc.AddWaitingOperator(op), Equals, 1)
		}
	}
}
