// Copyright 2019 TiKV Project Authors.
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

package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/pingcap/failpoint"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/requestutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
	"github.com/unrolled/render"
	"github.com/urfave/negroni"
)

// serviceInfoMiddleware is used to gather info from requsetInfo
type serviceInfoMiddleware struct {
	s                     *server.Server
	registeredSericeLabel *requestutil.RequestSchemaList
}

func newServiceInfoMiddleware(s *server.Server) negroni.Handler {
	registeredSericeLabel := requestutil.NewRequestSchemaList(len(config.HTTPRegisteredSericeLabel))

	for key, value := range config.HTTPRegisteredSericeLabel {
		if key[0] != '/' {
			continue
		}
		paths := strings.Split(key, "/")
		length := len(paths)
		registeredSericeLabel.AddServiceLabel(paths[1:length-1], paths[length-1], value)
	}
	return &serviceInfoMiddleware{s: s, registeredSericeLabel: registeredSericeLabel}
}

// ServeHTTP is used to implememt negroni.Handler for sericeInfoMiddleware
func (s *serviceInfoMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	if !s.s.GetEnabledServiceMiddleware() {
		next(w, r)
		return
	}

	requestInfo := s.registeredSericeLabel.GetRequestInfo(r)
	r = r.WithContext(requestutil.WithRequestInfo(r.Context(), requestInfo))

	failpoint.Inject("addSericeInfoMiddleware", func() {
		w.Header().Add("service-label", requestInfo.ServiceLabel)
		w.Header().Add("body-param", requestInfo.BodyParm)
		w.Header().Add("url-param", requestInfo.URLParam)
		w.Header().Add("method", requestInfo.Method)
		w.Header().Add("component", requestInfo.Component)
		w.Header().Add("ip", requestInfo.IP)
	})

	next(w, r)
}

type clusterMiddleware struct {
	s  *server.Server
	rd *render.Render
}

func newClusterMiddleware(s *server.Server) clusterMiddleware {
	return clusterMiddleware{
		s:  s,
		rd: render.New(render.Options{IndentJSON: true}),
	}
}

func (m clusterMiddleware) Middleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rc := m.s.GetRaftCluster()
		if rc == nil {
			m.rd.JSON(w, http.StatusInternalServerError, errs.ErrNotBootstrapped.FastGenByArgs().Error())
			return
		}
		ctx := context.WithValue(r.Context(), clusterCtxKey{}, rc)
		h.ServeHTTP(w, r.WithContext(ctx))
	})
}

type clusterCtxKey struct{}

func getCluster(r *http.Request) *cluster.RaftCluster {
	return r.Context().Value(clusterCtxKey{}).(*cluster.RaftCluster)
}
