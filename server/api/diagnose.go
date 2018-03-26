// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"context"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
	"net/http"
	"time"
)

type diagnoseType int
type diagnoseKV map[diagnoseType][4]string

const (
	//analyze level
	levelNormal   = "Normal"
	levelWarning  = "Warning"
	levelMinor    = "Minor"
	levelMajor    = "Major"
	levelCritical = "Critical"
	levelFatal    = "Fatal"

	//analyze module
	modMember   = "member"
	modTiKV     = "TiKV"
	modReplica  = "Replic"
	modSchedule = "Schedule"
	modDefault  = "indefinite"

	memberOneInstance diagnoseType = iota
	memberEvenInstance
	memberLostPeers
	memberLostPeersMoreThanHalf
	memberLeaderChanged
	tikvCap70
	tikvCap80
	tikvCap90
	tikvLostPeers
	tikvLostPeersLongTime
)

var (
	diagnoseMap = diagnoseKV{
		memberOneInstance:           [4]string{modMember, levelWarning, "only one PD instance is running", "please add PD instance"},
		memberEvenInstance:          [4]string{modMember, levelMinor, "PD instances is even number", "the recommended number of PD's instances is odd"},
		memberLostPeers:             [4]string{modMember, levelMajor, "some PD instances is down", "please check host load and traffic"},
		memberLostPeersMoreThanHalf: [4]string{modMember, levelCritical, "more than half PD instances is down", "please check host load and traffic"},
		memberLeaderChanged:         [4]string{modMember, levelMinor, "PD cluster leader is changed", "please check host load and traffic"},
		tikvCap70:                   [4]string{modTiKV, levelWarning, "some TiKV stroage used more than 70%", "plase add TiKV node"},
		tikvCap80:                   [4]string{modTiKV, levelMinor, "some TiKV stroage used more than 80%", "plase add TiKV node"},
		tikvCap90:                   [4]string{modTiKV, levelMajor, "some TiKV stroage used more than 90%", "plase add TiKV node"},
		tikvLostPeers:               [4]string{modTiKV, levelWarning, "some TiKV lost connect", "plase check network"},
		tikvLostPeersLongTime:       [4]string{modTiKV, levelMajor, "some TiKV lost connect more than 1h", "plase check network"},
	}
)

type diagnoseHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newDiagnoseHandler(svr *server.Server, rd *render.Render) *diagnoseHandler {
	return &diagnoseHandler{
		svr: svr,
		rd:  rd,
	}
}

//Recommended return detail
type Recommended struct {
	Module      string `json:"module"`
	Description string `json:"description"`
	Instruction string `json:"instruction"`
	Level       string `json:"level"`
}

func diagnosePD(key diagnoseType) *Recommended {
	var d [4]string
	switch key {
	case memberOneInstance:
		d = diagnoseMap[memberOneInstance]
	case memberEvenInstance:
		d = diagnoseMap[memberEvenInstance]
	case memberLostPeers:
		d = diagnoseMap[memberLostPeers]
	case memberLostPeersMoreThanHalf:
		d = diagnoseMap[memberLostPeersMoreThanHalf]
	case memberLeaderChanged:
		d = diagnoseMap[memberLeaderChanged]
	case tikvCap70:
		d = diagnoseMap[tikvCap70]
	case tikvCap80:
		d = diagnoseMap[tikvCap80]
	case tikvCap90:
		d = diagnoseMap[tikvCap90]
	case tikvLostPeers:
		d = diagnoseMap[tikvLostPeers]
	case tikvLostPeersLongTime:
		d = diagnoseMap[tikvLostPeersLongTime]
	}
	return &Recommended{
		Module:      d[0],
		Level:       d[1],
		Description: d[2],
		Instruction: d[3],
	}
}

func (d *diagnoseHandler) membersDiagnose(rd []*Recommended) error {
	var lenMembers, lostMembers int
	var changedLeaderLess1Min bool
	req := &pdpb.GetMembersRequest{Header: &pdpb.RequestHeader{ClusterId: d.svr.ClusterID()}}
	members, err := d.svr.GetMembers(context.Background(), req)
	if err != nil {
		return errors.Trace(err)
	}
	lenMembers = len(members.Members)
	if lenMembers > 0 {
		for _, m := range members.Members {
			pm, err := getEtcdPeerStats(m.ClientUrls[0])
			if err != nil {
				//get peer etcd failed
				lostMembers++
				continue
			}
			if time.Now().Sub(pm.LeaderInfo.StartTime) < time.Duration(3*time.Minute) {
				changedLeaderLess1Min = true
			}
		}
	} else {
		return errors.Errorf("get PD member error")
	}
	if changedLeaderLess1Min {
		rd = append(rd, diagnosePD(memberLeaderChanged))
	}
	if lenMembers-lostMembers == 1 {
		// only one pd running
		rd = append(rd, diagnosePD(memberOneInstance))
	}
	if lostMembers > 0 {
		// some pd can not connect
		rd = append(rd, diagnosePD(memberLostPeers))
	}
	if (lenMembers-lostMembers)%2 == 0 {
		// alived pd numbers is even
		rd = append(rd, diagnosePD(memberEvenInstance))
	}
	if float64(lenMembers)/2 < float64(lostMembers) {
		rd = append(rd, diagnosePD(memberLostPeersMoreThanHalf))
	}
	return nil
}

func (d *diagnoseHandler) tikvDiagnose(rd []*Recommended) error {
	return nil
}

func (d *diagnoseHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rd := []*Recommended{}
	if err := d.membersDiagnose(rd); err != nil {
		d.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	d.rd.JSON(w, http.StatusOK, rd)
}
