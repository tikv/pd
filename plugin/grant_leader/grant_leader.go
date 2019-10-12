package main

import (
	"fmt"
	"strconv"

	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pkg/errors"
)

func init() {
	schedule.RegisterScheduler("user-grant-leader", func(opController *schedule.OperatorController, args []string) (schedule.Scheduler, error) {
		if len(args) != 1 {
			return nil, errors.New("user-grant-leader needs 1 argument")
		}
		id, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return NewUserScheduler(opController, id), nil
	})
}

func SchedulerType() string {
	return "user-grant-leader"
}

func SchedulerArgs() []string {
	args := []string{"7"}
	return args
}

// grantLeaderScheduler transfers all leaders to peers in the store.
type grantLeaderScheduler struct {
	*userBaseScheduler
	name    string
	storeID uint64
}

// newGrantLeaderScheduler creates an admin scheduler that transfers all leaders
// to a store.
func NewUserScheduler(opController *schedule.OperatorController, storeID uint64) schedule.Scheduler {
	base := newUserBaseScheduler(opController)
	return &grantLeaderScheduler{
		userBaseScheduler: base,
		name:          fmt.Sprintf("user-grant-leader-scheduler-%d", storeID),
		storeID:       storeID,
	}
}

func (s *grantLeaderScheduler) GetName() string {
	return s.name
}

func (s *grantLeaderScheduler) GetType() string {
	return "user-grant-leader"
}
func (s *grantLeaderScheduler) Prepare(cluster schedule.Cluster) error {
	return cluster.BlockStore(s.storeID)
}

func (s *grantLeaderScheduler) Cleanup(cluster schedule.Cluster) {
	cluster.UnblockStore(s.storeID)
}

func (s *grantLeaderScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return s.opController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (s *grantLeaderScheduler) Schedule(cluster schedule.Cluster) []*operator.Operator {
	region := cluster.RandFollowerRegion(s.storeID, core.HealthRegion())
	if region == nil {
		return nil
	}
	op := operator.CreateTransferLeaderOperator("user-grant-leader", region, region.GetLeader().GetStoreId(), s.storeID, operator.OpLeader)
	op.SetPriorityLevel(core.HighPriority)
	return []*operator.Operator{op}
}
