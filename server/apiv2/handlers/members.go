package handlers

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/server"
	"go.uber.org/zap"
)

// GetMembers returns the PD members.
func GetMembers() gin.HandlerFunc {
	return func(c *gin.Context) {
		svr := c.MustGet("server").(*server.Server)
		req := &pdpb.GetMembersRequest{Header: &pdpb.RequestHeader{ClusterId: svr.ClusterID()}}
		grpcServer := &server.GrpcServer{Server: svr}
		members, err := grpcServer.GetMembers(context.Background(), req)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
			return
		}
		dclocationDistribution, err := svr.GetTSOAllocatorManager().GetClusterDCLocationsFromEtcd()
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
			return
		}
		for _, m := range members.GetMembers() {
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
		c.IndentedJSON(http.StatusOK, members)
	}
}

type updateParams struct {
	LeaderPriority float64 `json:"leader_priority"`
}

// UpdateMemberByName will update the PD member info according to the given parameters.
func UpdateMemberByName() gin.HandlerFunc {
	return func(c *gin.Context) {
		svr := c.MustGet("server").(*server.Server)

		// Get etcd ID by name.
		id, err := getMemberIDByName(svr, c.Param("name"))
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
			return
		}
		if id == 0 {
			c.AbortWithStatus(http.StatusNotFound)
			return
		}

		var p updateParams
		if err := c.BindJSON(&p); err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause())
			return
		}
		err = svr.GetMember().SetMemberLeaderPriority(id, int(p.LeaderPriority))
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
			return
		}
		c.JSON(http.StatusOK, nil)
	}
}

// DeleteMemberByName will delete the PD member according to the given member's name.
func DeleteMemberByName() gin.HandlerFunc {
	return func(c *gin.Context) {
		svr := c.MustGet("server").(*server.Server)

		// Get etcd ID by name.
		id, err := getMemberIDByName(svr, c.Param("name"))
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
			return
		}

		if id == 0 {
			c.AbortWithStatus(http.StatusNotFound)
			return
		}

		if err := deleteMemberByID(svr, id); err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
			return
		}
		c.JSON(
			http.StatusOK,
			nil,
		)
	}
}

func getMemberIDByName(svr *server.Server, name string) (id uint64, err error) {
	members, err := svr.GetMembers()
	if err != nil {
		return
	}
	for _, m := range members {
		if name == m.Name {
			id = m.GetMemberId()
			break
		}
	}
	return
}

func deleteMemberByID(svr *server.Server, id uint64) error {
	// Delete config.
	err := svr.GetMember().DeleteMemberLeaderPriority(id)
	if err != nil {
		return err
	}

	// Delete dc-location info.
	err = svr.GetMember().DeleteMemberDCLocationInfo(id)
	if err != nil {
		return err
	}

	client := svr.GetClient()
	_, err = etcdutil.RemoveEtcdMember(client, id)
	if err != nil {
		return err
	}
	return nil
}
