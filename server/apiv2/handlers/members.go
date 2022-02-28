// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package handlers

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server"
	"go.uber.org/zap"
)

// GetMembers returns the PD members.
func GetMembers() gin.HandlerFunc {
	return func(c *gin.Context) {
		svr := c.MustGet("server").(*server.Server)
		members, err := getAllMembers(svr)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
			return
		}
		c.IndentedJSON(http.StatusOK, members)
	}
}

// GetMemberByName will return the PD member according to the given member's name.
func GetMemberByName() gin.HandlerFunc {
	return func(c *gin.Context) {
		svr := c.MustGet("server").(*server.Server)
		// TODO: only get the member with the given name
		members, err := getAllMembers(svr)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
			return
		}

		for _, member := range members.Members {
			if member.GetName() == c.Param("name") {
				c.JSON(http.StatusOK, member)
				return
			}
		}
		c.AbortWithStatus(http.StatusNotFound)
	}
}

type updateMembersParams struct {
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

		var p updateMembersParams
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

func getAllMembers(svr *server.Server) (*pdpb.GetMembersResponse, error) {
	req := &pdpb.GetMembersRequest{Header: &pdpb.RequestHeader{ClusterId: svr.ClusterID()}}
	grpcServer := &server.GrpcServer{Server: svr}
	members, err := grpcServer.GetMembers(context.Background(), req)
	if err != nil {
		return nil, err
	}
	dclocationDistribution, err := svr.GetTSOAllocatorManager().GetClusterDCLocationsFromEtcd()
	if err != nil {
		return nil, err
	}
	for _, m := range members.GetMembers() {
		var e error
		m.BinaryVersion, e = svr.GetMember().GetMemberBinaryVersion(m.GetMemberId())
		if e != nil {
			log.Error("failed to load binary version", zap.Uint64("member", m.GetMemberId()), errs.ZapError(e))
		}
		m.DeployPath, e = svr.GetMember().GetMemberDeployPath(m.GetMemberId())
		if e != nil {
			log.Error("failed to load deploy path", zap.Uint64("member", m.GetMemberId()), errs.ZapError(e))
		}
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
		m.GitHash, e = svr.GetMember().GetMemberGitHash(m.GetMemberId())
		if e != nil {
			log.Error("failed to load git hash", zap.Uint64("member", m.GetMemberId()), errs.ZapError(e))
			continue
		}
		for dcLocation, serverIDs := range dclocationDistribution {
			found := slice.Contains(serverIDs, m.MemberId)
			if found {
				m.DcLocation = dcLocation
				break
			}
		}
	}
	return members, nil
}
