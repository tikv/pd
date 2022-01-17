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

	"github.com/pingcap/failpoint"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/requestutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/cluster"
	"github.com/unrolled/render"
	"github.com/urfave/negroni"
)

// serviceMiddleware is used to
type middlewareBuilder struct {
	svr     *server.Server
	handler http.Handler
}

func newMiddlewareBuilder(s *server.Server) middlewareBuilder {
	return middlewareBuilder{
		svr: s,
		handler: negroni.New(
			newRequestInfoMiddleware(s),
			// todo: add audit and rate limit middleware
		),
	}
}

func (s *middlewareBuilder) middleware(handler http.Handler) http.Handler {
	return negroni.New(negroni.Wrap(s.handler), negroni.Wrap(handler))
}

func (s *middlewareBuilder) middlewareFunc(next func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		s.handler.ServeHTTP(w, r)
		next(w, r)
	}
}

// requestInfoMiddleware is used to gather info from requsetInfo
type requestInfoMiddleware struct {
	svr *server.Server
}

func newRequestInfoMiddleware(s *server.Server) negroni.Handler {
	return &requestInfoMiddleware{svr: s}
}

func (rm *requestInfoMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	if !rm.svr.IsServiceMiddlewareEnabled() {
		next(w, r)
		return
	}

	requestInfo := requestutil.GetRequestInfo(r)
	r = r.WithContext(requestutil.WithRequestInfo(r.Context(), requestInfo))

	failpoint.Inject("addRequestInfoMiddleware", func() {
		w.Header().Add("service-label", requestInfo.ServiceLabel)
		w.Header().Add("body-param", requestInfo.BodyParam)
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
