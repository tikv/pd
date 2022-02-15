package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server"
	"go.uber.org/zap"
)

// GetMembers returns the PD members.
func GetMembers() gin.HandlerFunc {
	return func(c *gin.Context) {
		svr := c.MustGet("server").(*server.Server)
		members, err := svr.GetMembers()
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
			return
		}
		dclocationDistribution, err := svr.GetTSOAllocatorManager().GetClusterDCLocationsFromEtcd()
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
			return
		}
		for _, m := range members {
			m.DcLocation = ""
			binaryVersion, e := svr.GetMember().GetMemberBinaryVersion(m.GetMemberId())
			if e != nil {
				log.Error("failed to load binary version", zap.Uint64("member", m.GetMemberId()), errs.ZapError(e))
			}
			m.BinaryVersion = binaryVersion
			deployPath, e := svr.GetMember().GetMemberDeployPath(m.GetMemberId())
			if e != nil {
				log.Error("failed to load deploy path", zap.Uint64("member", m.GetMemberId()), errs.ZapError(e))
			}
			m.DeployPath = deployPath
			if svr.GetMember().GetEtcdLeader() == 0 {
				log.Warn("no etcd leader, skip get leader priority", zap.Uint64("member", m.GetMemberId()))
				continue
			}
			leaderPriority, e := svr.GetMember().GetMemberLeaderPriority(m.GetMemberId())
			if e != nil {
				log.Error("failed to load leader priority", zap.Uint64("member", m.GetMemberId()), errs.ZapError(e))
				continue
			}
			m.LeaderPriority = int32(leaderPriority)
			gitHash, e := svr.GetMember().GetMemberGitHash(m.GetMemberId())
			if e != nil {
				log.Error("failed to load git hash", zap.Uint64("member", m.GetMemberId()), errs.ZapError(e))
				continue
			}
			m.GitHash = gitHash
			found := false
			for dcLocation, serverIDs := range dclocationDistribution {
				for _, serverID := range serverIDs {
					if serverID == m.MemberId {
						m.DcLocation = dcLocation
						found = true
						break
					}
				}
				if found {
					break
				}
			}
		}
		var etcdLeader, pdLeader *pdpb.Member
		leaderID := svr.GetMember().GetEtcdLeader()
		for _, m := range members {
			if m.MemberId == leaderID {
				etcdLeader = m
				break
			}
		}

		tsoAllocatorManager := svr.GetTSOAllocatorManager()
		tsoAllocatorLeaders, err := tsoAllocatorManager.GetLocalAllocatorLeaders()
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
			return
		}

		leader := svr.GetMember().GetLeader()
		for _, m := range members {
			if m.MemberId == leader.GetMemberId() {
				pdLeader = m
				break
			}
		}

		c.IndentedJSON(http.StatusOK, gin.H{
			"members":               members,
			"leader":                pdLeader,
			"etcd_leader":           etcdLeader,
			"tso_allocator_leaders": tsoAllocatorLeaders,
		})
	}
}
