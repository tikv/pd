// Copyright 2019 PingCAP, Inc.
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

package keyvisual

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/pkg/apiutil/serverapi"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/server/cluster"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/unrolled/render"
	"github.com/urfave/negroni"
	"go.uber.org/zap"
)

var (
	defaultRegisterAPIGroupInfo = server.APIGroup{
		IsCore:  false,
		Name:    "keyvisual",
		Version: "v1",
	}
)

// keyvisualService provides the service of heatmap statistics of the key.
type keyvisualService struct {
	*http.ServeMux
	svr *server.Server
	ctx context.Context
	rd  *render.Render
}

// NewKeyvisualService creates a HTTP handler for heatmap service.
func NewKeyvisualService(ctx context.Context, svr *server.Server) (http.Handler, server.APIGroup) {
	mux := http.NewServeMux()
	k := &keyvisualService{
		ServeMux: mux,
		svr:      svr,
		ctx:      ctx,
		rd:       render.New(render.Options{StreamingJSON: true}),
	}

	k.HandleFunc("/pd/apis/keyvisual/v1/heatmaps", k.Heatmap)
	handler := negroni.New(
		serverapi.NewRuntimeServiceValidator(svr, defaultRegisterAPIGroupInfo),
		serverapi.NewRedirector(svr),
		negroni.Wrap(k),
	)
	go k.run()
	return handler, defaultRegisterAPIGroupInfo
}

// Heatmap returns the heatmap data.
func (s *keyvisualService) Heatmap(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-type", "application/json")
	form := r.URL.Query()
	startKey := form.Get("startkey")
	endKey := form.Get("endkey")
	startTimeString := form.Get("starttime")
	endTimeString := form.Get("endtime")
	typ := form.Get("type")
	endTime := time.Now()
	startTime := endTime.Add(-1200 * time.Minute)

	if startTimeString != "" {
		tsSec, err := strconv.ParseInt(startTimeString, 10, 64)
		if err != nil {
			log.Error("parse ts failed", zap.Error(err))
			s.rd.JSON(w, http.StatusBadRequest, "bad request")
			return
		}
		startTime = time.Unix(tsSec, 0)
	}
	if endTimeString != "" {
		tsSec, err := strconv.ParseInt(endTimeString, 10, 64)
		if err != nil {
			log.Error("parse ts failed", zap.Error(err))
			s.rd.JSON(w, http.StatusBadRequest, "bad request")
			return
		}
		endTime = time.Unix(tsSec, 0)
	}

	log.Info("Request matrix",
		zap.Time("start-time", startTime),
		zap.Time("end-time", endTime),
		zap.String("start-key", startKey),
		zap.String("end-key", endKey),
		zap.String("type", typ),
	)
	// TODO: get the heatmap
	s.rd.JSON(w, http.StatusNotImplemented, "not implemented")
}

// FIXME: works well with leader changed.
func (s *keyvisualService) run() {
	// TODO: make the ticker consistent with heartbeat interval
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			cluster := s.svr.GetRaftCluster()
			if cluster == nil || !serverapi.IsServiceAllowed(s.svr, defaultRegisterAPIGroupInfo) {
				continue
			}
			s.scanRegions(cluster)
			// TODO: implements the stats
		}
	}
}

func (s *keyvisualService) scanRegions(cluster *cluster.RaftCluster) []*core.RegionInfo {
	var key []byte
	limit := 1024
	regions := make([]*core.RegionInfo, 0, limit)
	for {
		select {
		case <-s.ctx.Done():
			return nil
		default:
		}
		rs := cluster.ScanRegions(key, []byte(""), limit)
		length := len(rs)
		if length == 0 {
			break

		}
		regions = append(regions, rs...)
		key = rs[length-1].GetEndKey()
		if len(key) == 0 {
			break
		}
	}
	return regions
}
