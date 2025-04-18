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
	"time"

	"github.com/unrolled/render"
	"github.com/urfave/negroni"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/audit"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/requestutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/cluster"
)

// serviceMiddlewareBuilder is used to build service middleware for HTTP api
type serviceMiddlewareBuilder struct {
	svr      *server.Server
	handlers []negroni.Handler
}

func newServiceMiddlewareBuilder(s *server.Server) *serviceMiddlewareBuilder {
	return &serviceMiddlewareBuilder{
		svr:      s,
		handlers: []negroni.Handler{newRequestInfoMiddleware(s), newAuditMiddleware(s), newRateLimitMiddleware(s)},
	}
}

func (s *serviceMiddlewareBuilder) createHandler(next func(http.ResponseWriter, *http.Request)) http.Handler {
	return negroni.New(append(s.handlers, negroni.WrapFunc(next))...)
}

// requestInfoMiddleware is used to gather info from requestInfo
type requestInfoMiddleware struct {
	svr *server.Server
}

func newRequestInfoMiddleware(s *server.Server) negroni.Handler {
	return &requestInfoMiddleware{svr: s}
}

func (rm *requestInfoMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	if !rm.svr.GetServiceMiddlewarePersistOptions().IsAuditEnabled() && !rm.svr.GetServiceMiddlewarePersistOptions().IsRateLimitEnabled() {
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
		w.Header().Add("caller-id", requestInfo.CallerID)
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

func (m clusterMiddleware) middleware(h http.Handler) http.Handler {
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

type auditMiddleware struct {
	svr *server.Server
}

func newAuditMiddleware(s *server.Server) negroni.Handler {
	return &auditMiddleware{svr: s}
}

// ServeHTTP is used to implement negroni.Handler for auditMiddleware
func (s *auditMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	if !s.svr.GetServiceMiddlewarePersistOptions().IsAuditEnabled() {
		next(w, r)
		return
	}

	requestInfo, ok := requestutil.RequestInfoFrom(r.Context())
	if !ok {
		requestInfo = requestutil.GetRequestInfo(r)
	}

	labels := s.svr.GetServiceAuditBackendLabels(requestInfo.ServiceLabel)
	if labels == nil {
		next(w, r)
		return
	}

	beforeNextBackends := make([]audit.Backend, 0)
	afterNextBackends := make([]audit.Backend, 0)
	for _, backend := range s.svr.GetAuditBackend() {
		if backend.Match(labels) {
			if backend.ProcessBeforeHandler() {
				beforeNextBackends = append(beforeNextBackends, backend)
			} else {
				afterNextBackends = append(afterNextBackends, backend)
			}
		}
	}
	for _, backend := range beforeNextBackends {
		backend.ProcessHTTPRequest(r)
	}

	next(w, r)

	endTime := time.Now().Unix()
	r = r.WithContext(requestutil.WithEndTime(r.Context(), endTime))
	for _, backend := range afterNextBackends {
		backend.ProcessHTTPRequest(r)
	}
}

type rateLimitMiddleware struct {
	svr *server.Server
}

func newRateLimitMiddleware(s *server.Server) negroni.Handler {
	return &rateLimitMiddleware{svr: s}
}

// ServeHTTP is used to implement negroni.Handler for rateLimitMiddleware
func (s *rateLimitMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	if !s.svr.GetServiceMiddlewarePersistOptions().IsRateLimitEnabled() {
		next(w, r)
		return
	}
	requestInfo, ok := requestutil.RequestInfoFrom(r.Context())
	if !ok {
		requestInfo = requestutil.GetRequestInfo(r)
	}

	// There is no need to check whether rateLimiter is nil. CreateServer ensures that it is created
	rateLimiter := s.svr.GetServiceRateLimiter()
	if done, err := rateLimiter.Allow(requestInfo.ServiceLabel); err == nil {
		defer done()
		next(w, r)
	} else {
		http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
	}
}
