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
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/urfave/negroni"
	"go.uber.org/zap"
)

// HTTP headers.
const (
	RedirectorHeader    = "PD-Redirector"
	AllowFollowerHandle = "PD-Allow-follower-handle"
	FollowerHandle      = "PD-Follower-handle"
)

const (
	errRedirectFailed      = "redirect failed"
	errRedirectToNotLeader = "redirect to not leader"
)

type runtimeServiceValidator struct {
	s     *server.Server
	group server.ServiceGroup
}

// NewRuntimeServiceValidator checks if the path is invalid.
func NewRuntimeServiceValidator(s *server.Server, group server.ServiceGroup) negroni.Handler {
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
func IsServiceAllowed(s *server.Server, group server.ServiceGroup) bool {
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
	allowFollowerHandle := len(r.Header.Get(AllowFollowerHandle)) > 0
	isLeader := h.s.GetMember().IsLeader()
	if !h.s.IsClosed() && (allowFollowerHandle || isLeader) {
		if !isLeader {
			w.Header().Add(FollowerHandle, "true")
		}
		next(w, r)
		return
	}

	// Prevent more than one redirection.
	if name := r.Header.Get(RedirectorHeader); len(name) != 0 {
		log.Error("redirect but server is not leader", zap.String("from", name), zap.String("server", h.s.Name()), errs.ZapError(errs.ErrRedirect))
		http.Error(w, errRedirectToNotLeader, http.StatusInternalServerError)
		return
	}

	r.Header.Set(RedirectorHeader, h.s.Name())

	leader := h.s.GetMember().GetLeader()
	if leader == nil {
		http.Error(w, "no leader", http.StatusServiceUnavailable)
		return
	}

	urls, err := config.ParseUrls(strings.Join(leader.GetClientUrls(), ","))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	client := h.s.GetHTTPClient()
	NewCustomReverseProxies(client, urls).ServeHTTP(w, r)
}

type customReverseProxies struct {
	urls   []url.URL
	client *http.Client
}

// NewCustomReverseProxies returns the custom reverse proxies.
func NewCustomReverseProxies(dialClient *http.Client, urls []url.URL) http.Handler {
	p := &customReverseProxies{
		client: dialClient,
	}

	p.urls = append(p.urls, urls...)

	return p
}

func (p *customReverseProxies) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	for _, url := range p.urls {
		r.RequestURI = ""
		r.URL.Host = url.Host
		r.URL.Scheme = url.Scheme

		resp, err := p.client.Do(r)
		if err != nil {
			log.Error("request failed", errs.ZapError(errs.ErrSendRequest, err))
			continue
		}

		b, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Error("read failed", errs.ZapError(errs.ErrIORead, err))
			continue
		}

		copyHeader(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		if _, err := w.Write(b); err != nil {
			log.Error("write failed", errs.ZapError(errs.ErrWriteHTTPBody, err))
			continue
		}

		return
	}

	http.Error(w, errRedirectFailed, http.StatusInternalServerError)
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		values := dst[k]
		for _, v := range vv {
			if !contains(values, v) {
				dst.Add(k, v)
			}
		}
	}
}

func contains(s []string, x string) bool {
	for _, n := range s {
		if x == n {
			return true
		}
	}
	return false
}
