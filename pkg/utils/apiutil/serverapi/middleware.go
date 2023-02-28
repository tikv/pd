// Copyright 2016 TiKV Project Authors.
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

package serverapi

import (
	"net/http"
	"net/url"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/server"
	"github.com/urfave/negroni"
	"go.uber.org/zap"
)

// HTTP headers.
const (
	PDRedirectorHeader    = "PD-Redirector"
	PDAllowFollowerHandle = "PD-Allow-follower-handle"
)

type runtimeServiceValidator struct {
	s     *server.Server
	group apiutil.APIServiceGroup
}

// NewRuntimeServiceValidator checks if the path is invalid.
func NewRuntimeServiceValidator(s *server.Server, group apiutil.APIServiceGroup) negroni.Handler {
	return &runtimeServiceValidator{s: s, group: group}
}

func (h *runtimeServiceValidator) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	if IsServiceAllowed(h.s, h.group) {
		next(w, r)
		return
	}

	http.Error(w, "no service", http.StatusServiceUnavailable)
}

// IsServiceAllowed checks the service through the path.
func IsServiceAllowed(s *server.Server, group apiutil.APIServiceGroup) bool {
	// for core path
	if group.IsCore {
		return true
	}

	opt := s.GetServerOption()
	cfg := opt.GetPDServerConfig()
	if cfg != nil {
		for _, allow := range cfg.RuntimeServices {
			if group.Name == allow {
				return true
			}
		}
	}

	return false
}

type redirector struct {
	s *server.Server
}

// NewRedirector redirects request to the leader if needs to be handled in the leader.
func NewRedirector(s *server.Server) negroni.Handler {
	return &redirector{s: s}
}

func (h *redirector) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	allowFollowerHandle := len(r.Header.Get(PDAllowFollowerHandle)) > 0
	isLeader := h.s.GetMember().IsLeader()
	if !h.s.IsClosed() && (allowFollowerHandle || isLeader) {
		next(w, r)
		return
	}

	// Prevent more than one redirection.
	if name := r.Header.Get(PDRedirectorHeader); len(name) != 0 {
		log.Error("redirect but server is not leader", zap.String("from", name), zap.String("server", h.s.Name()), errs.ZapError(errs.ErrRedirect))
		http.Error(w, apiutil.ErrRedirectToNotLeader, http.StatusInternalServerError)
		return
	}

	r.Header.Set(PDRedirectorHeader, h.s.Name())

	leader := h.s.GetMember().GetLeader()
	if leader == nil {
		http.Error(w, "no leader", http.StatusServiceUnavailable)
		return
	}
	clientUrls := leader.GetClientUrls()
	urls := make([]url.URL, 0, len(clientUrls))
	for _, item := range clientUrls {
		u, err := url.Parse(item)
		if err != nil {
			http.Error(w, errs.ErrURLParse.Wrap(err).GenWithStackByCause().Error(), http.StatusInternalServerError)
			return
		}

		urls = append(urls, *u)
	}
	client := h.s.GetHTTPClient()
	apiutil.NewCustomReverseProxies(client, urls).ServeHTTP(w, r)
}
