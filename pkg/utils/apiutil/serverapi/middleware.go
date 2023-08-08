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
	PDRedirectorHeader     = "PD-Redirector"
	PDAllowFollowerHandle  = "PD-Allow-follower-handle"
	PDPreferFollowerHandle = "PD-Prefer-Follower-Handle"
	ForwardedForHeader     = "X-Forwarded-For"
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

// TODO: replace with http proxy.
type redirector struct {
	s *server.Server

	microserviceRedirectRules []*microserviceRedirectRule
}

type microserviceRedirectRule struct {
	matchPath         string
	targetPath        string
	targetServiceName string
}

// NewRedirector redirects request to the leader if needs to be handled in the leader.
func NewRedirector(s *server.Server, opts ...RedirectorOption) negroni.Handler {
	r := &redirector{s: s}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// RedirectorOption defines the option of redirector
type RedirectorOption func(*redirector)

// MicroserviceRedirectRule new a microservice redirect rule option
func MicroserviceRedirectRule(targetServiceName, matchPath, targetPath string) RedirectorOption {
	return func(s *redirector) {
		s.microserviceRedirectRules = append(s.microserviceRedirectRules, &microserviceRedirectRule{
			matchPath,
			targetPath,
			targetServiceName,
		})
	}
}

func (h *redirector) matchMicroServiceRedirectRules(r *http.Request) (bool, string) {
	if !h.s.IsAPIServiceMode() {
		return false, ""
	}
	if len(h.microserviceRedirectRules) == 0 {
		return false, ""
	}
	for _, rule := range h.microserviceRedirectRules {
		if rule.matchPath == r.URL.Path {
			addr, ok := h.s.GetServicePrimaryAddr(r.Context(), rule.targetServiceName)
			if !ok || addr == "" {
				log.Warn("failed to get the service primary addr when try match redirect rules",
					zap.String("path", r.URL.Path))
			}
			r.URL.Path = rule.targetPath
			return true, addr
		}
	}
	return false, ""
}

func (h *redirector) tryRedirectToFollowerHandler(r *http.Request) (bool, []string) {
	if len(r.Header.Get(PDPreferFollowerHandle)) == 0 {
		return false, nil
	}
	if !h.s.GetMember().IsLeader() {
		return false, nil
	}
	members, err := h.s.GetMembers()
	if err != nil {
		log.Error("failed to get members", errs.ZapError(err))
		return false, nil
	}
	leader := h.s.GetLeader()
	urls := make([]string, 0, len(members))
	// TODO, maintain member health status.
	for _, m := range members {
		if m.GetMemberId() == leader.GetMemberId() && leader.GetMemberId() != 0 {
			continue
		}
		urls = append(urls, m.GetClientUrls()[0])
	}
	if len(urls) == 0 {
		return false, nil
	}
	return true, urls
}

func (h *redirector) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	allowFollowerHandle := len(r.Header.Get(PDAllowFollowerHandle)) > 0 || len(r.Header.Get(PDPreferFollowerHandle)) > 0
	needRedirectToFollower, followerAddrs := h.tryRedirectToFollowerHandler(r)
	needRedirectToService, targetAddr := h.matchMicroServiceRedirectRules(r)
	isLeader := h.s.GetMember().IsLeader()
	allowHandle := !h.s.IsClosed() && (allowFollowerHandle || isLeader)
	if allowHandle && !needRedirectToService && !needRedirectToFollower {
		next(w, r)
		return
	}

	// Prevent more than one redirection.
	if names := r.Header.Values(PDRedirectorHeader); len(names) > 1 {
		log.Error("redirect is more than one times", zap.Strings("from", names), zap.String("server", h.s.Name()), errs.ZapError(errs.ErrRedirect))
		http.Error(w, apiutil.ErrRedirectFailed, http.StatusInternalServerError)
		return
	}

	r.Header.Add(PDRedirectorHeader, h.s.Name())
	r.Header.Add(ForwardedForHeader, r.RemoteAddr)

	var clientUrls []string
	if needRedirectToService {
		if len(targetAddr) == 0 {
			http.Error(w, apiutil.ErrRedirectFailed, http.StatusInternalServerError)
			return
		}
		clientUrls = append(clientUrls, targetAddr)
	} else if needRedirectToFollower {
		clientUrls = followerAddrs
	} else {
		leader := h.s.GetMember().GetLeader()
		if leader == nil {
			http.Error(w, "no leader", http.StatusServiceUnavailable)
			return
		}
		clientUrls = leader.GetClientUrls()
	}
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
