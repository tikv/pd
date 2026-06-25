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
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/urfave/negroni/v3"
	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/server"
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

	microserviceRedirectRules []RedirectRule
}

// RedirectRule describes how to match and rewrite microservice paths.
type RedirectRule struct {
	MatchPath         string
	TargetPath        string
	TargetServiceName string
	MatchMethods      []string
	Filter            func(*http.Request) bool
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
func MicroserviceRedirectRule(matchPath, targetPath, targetServiceName string,
	methods []string, filters ...func(*http.Request) bool) RedirectorOption {
	return func(s *redirector) {
		rule := RedirectRule{
			MatchPath:         matchPath,
			TargetPath:        targetPath,
			TargetServiceName: targetServiceName,
			MatchMethods:      methods,
		}
		if len(filters) > 0 {
			rule.Filter = filters[0]
		}
		s.microserviceRedirectRules = append(s.microserviceRedirectRules, rule)
	}
}

func (h *redirector) matchMicroserviceRedirectRules(r *http.Request) (bool, string) {
	return MatchMicroserviceRedirect(
		r,
		h.microserviceRedirectRules,
		h.s.IsKeyspaceGroupEnabled(),
		h.s.IsServiceIndependent,
		h.s.GetServicePrimaryAddr)
}

// MatchMicroserviceRedirect checks rules, rewrites path in-place, and returns (matched, targetAddr).
// If matched but no primary is available, it returns matched=true with empty addr so caller can handle the redirect error.
func MatchMicroserviceRedirect(
	r *http.Request,
	rules []RedirectRule,
	isKeyspaceGroupEnabled bool,
	isServiceIndependent func(string) bool,
	getPrimary func(context.Context, string) (string, bool),
) (bool, string) {
	if !isKeyspaceGroupEnabled || len(rules) == 0 {
		return false, ""
	}
	if r.Header.Get(apiutil.XForbiddenForwardToMicroserviceHeader) == "true" {
		return false, ""
	}
	// Remove trailing '/' from the URL path
	// It will be helpful when matching the redirect rules "schedulers" or "schedulers/{name}"
	r.URL.Path = strings.TrimRight(r.URL.Path, "/")
	for _, rule := range rules {
		// Now we only support checking the scheduling service whether it is independent
		if rule.TargetServiceName == constant.SchedulingServiceName && !isServiceIndependent(constant.SchedulingServiceName) {
			continue
		}
		if !strings.HasPrefix(r.URL.Path, rule.MatchPath) || !slice.Contains(rule.MatchMethods, r.Method) {
			continue
		}
		if rule.Filter != nil && !rule.Filter(r) {
			continue
		}
		// we check the service primary addr here,
		// if the service is not available, we will return ErrRedirect by returning an empty addr.
		addr, ok := getPrimary(r.Context(), rule.TargetServiceName)
		if !ok || addr == "" {
			log.Warn("failed to get the service primary addr when trying to match redirect rules",
				zap.String("path", r.URL.Path))
			return true, ""
		}
		// If the URL contains escaped characters, use RawPath instead of Path
		origin := r.URL.Path
		path := r.URL.Path
		if r.URL.RawPath != "" {
			path = r.URL.RawPath
		}
		// Extract parameters from the URL path
		// e.g. r.URL.Path = /pd/api/v1/operators/1 (before redirect)
		//      matchPath  = /pd/api/v1/operators
		//      targetPath = /scheduling/api/v1/operators
		//      r.URL.Path = /scheduling/api/v1/operator/1 (after redirect)
		pathParams := strings.TrimPrefix(path, rule.MatchPath)
		pathParams = strings.Trim(pathParams, "/") // Remove leading and trailing '/'
		if len(pathParams) > 0 {
			r.URL.Path = rule.TargetPath + "/" + pathParams
		} else {
			r.URL.Path = rule.TargetPath
		}
		log.Debug("redirect to microservice", zap.String("path", r.URL.Path), zap.String("origin-path", origin),
			zap.String("target", addr), zap.String("method", r.Method))
		return true, addr
	}
	return false, ""
}

var localOnlyPaths = []string{
	"/pd/api/v1/admin/log",
	"/pd/api/v1/status",
	"/pd/api/v1/health",
	"/pd/api/v1/ping",
	"/pd/api/v1/version",
	"/pd/api/v1/debug/pprof",
}

func (h *redirector) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	// Special case: GET /config should always be handled locally on followers
	// to return a merged view of local and cluster configurations.
	// POST /config should still be forwarded to the leader to update cluster-wide config.
	if r.URL.Path == "/pd/api/v1/config" && r.Method == http.MethodGet {
		next(w, r)
		return
	}
	for _, path := range localOnlyPaths {
		if strings.HasPrefix(r.URL.Path, path) {
			next(w, r)
			return
		}
	}

	redirectToMicroservice, targetAddr := h.matchMicroserviceRedirectRules(r)
	allowFollowerHandle := len(r.Header.Get(apiutil.PDAllowFollowerHandleHeader)) > 0

	if h.s.IsClosed() {
		http.Error(w, errs.ErrServerNotStarted.FastGenByArgs().Error(), http.StatusInternalServerError)
		return
	}

	if (allowFollowerHandle || h.s.GetMember().IsServing()) && !redirectToMicroservice {
		next(w, r)
		return
	}

	forwardedIP, forwardedPort := apiutil.GetIPPortFromHTTPRequest(r)
	if len(forwardedIP) > 0 {
		r.Header.Add(apiutil.XForwardedForHeader, forwardedIP)
	} else {
		// Fallback if GetIPPortFromHTTPRequest failed to get the IP.
		r.Header.Add(apiutil.XForwardedForHeader, r.RemoteAddr)
	}
	if len(forwardedPort) > 0 {
		r.Header.Add(apiutil.XForwardedPortHeader, forwardedPort)
	}

	var clientUrls []string
	if redirectToMicroservice {
		if len(targetAddr) == 0 {
			http.Error(w, errs.ErrRedirect.FastGenByArgs().Error(), http.StatusInternalServerError)
			return
		}
		clientUrls = append(clientUrls, targetAddr)
		// Add a header to the response, it is used to mark whether the request has been forwarded to the microservice.
		w.Header().Add(apiutil.XForwardedToMicroserviceHeader, "true")
	} else if name := r.Header.Get(apiutil.PDRedirectorHeader); len(name) == 0 {
		leader := h.waitForLeader(r)
		// The leader has not been elected yet.
		if leader == nil {
			http.Error(w, errs.ErrRedirectNoLeader.FastGenByArgs().Error(), http.StatusServiceUnavailable)
			return
		}
		// If the leader is the current server now, we can handle the request directly.
		if h.s.GetMember().IsServing() || leader.GetName() == h.s.Name() {
			next(w, r)
			return
		}
		clientUrls = leader.GetClientUrls()
		r.Header.Set(apiutil.PDRedirectorHeader, h.s.Name())
	} else {
		// Prevent more than one redirection among PD.
		log.Warn("redirect but server is not leader", zap.String("from", name), zap.String("server", h.s.Name()), errs.ZapError(errs.ErrRedirectToNotLeader))
		http.Error(w, errs.ErrRedirectToNotLeader.FastGenByArgs().Error(), http.StatusInternalServerError)
		return
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

const (
	backoffMaxDelay = 3 * time.Second
	backoffInterval = 100 * time.Millisecond
)

// If current server does not have a leader, backoff to increase the chance of success.
func (h *redirector) waitForLeader(r *http.Request) (leader *pdpb.Member) {
	var (
		interval = backoffInterval
		maxDelay = backoffMaxDelay
		curDelay = time.Duration(0)
	)
	for {
		leader = h.s.GetMember().GetLeader()
		if leader != nil {
			return
		}
		select {
		case <-time.After(interval):
			curDelay += interval
			if curDelay >= maxDelay {
				return
			}
			interval *= 2
			if curDelay+interval > maxDelay {
				interval = maxDelay - curDelay
			}
		case <-r.Context().Done():
			return
		case <-h.s.Context().Done():
			return
		}
	}
}
