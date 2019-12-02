// Copyright 2016 PingCAP, Inc.
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

package serverapi

import (
	"crypto/tls"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/config"
	"go.uber.org/zap"
)

// somes HTTP filed.
const (
	RedirectorHeader    = "PD-Redirector"
	AllowFollowerHandle = "PD-Allow-follower-handle"
	FollowerHandle      = "PD-Follwer-handle"
)

const (
	errRedirectFailed      = "redirect failed"
	errRedirectToNotLeader = "redirect to not leader"
)

var initHTTPClientOnce sync.Once

// dialClient used to dail http request.
var dialClient = &http.Client{
	Transport: &http.Transport{
		DisableKeepAlives: true,
	},
}

type runtimeServiceAuth struct {
	s *server.Server
}

// NewRuntimeServiceAuth checks if the path is invalid.
func NewRuntimeServiceAuth(s *server.Server) *runtimeServiceAuth {
	return &runtimeServiceAuth{s: s}
}

func (h *runtimeServiceAuth) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	if IsServiceAllowed(h.s, r.RequestURI) {
		next(w, r)
		return
	}

	http.Error(w, "no service", http.StatusServiceUnavailable)
}

// IsServiceAllowed checks the service through the path.
func IsServiceAllowed(s *server.Server, path string) bool {
	opt := s.GetServerOption()
	cfg := opt.LoadPDServerConfig()
	if cfg != nil {
		for _, allow := range cfg.RuntimeServices {
			if len(allow) != 0 && strings.Contains(path, allow) {
				return true
			}
		}
	}
	// for core path
	if strings.Contains(path, "api/v1") {
		return true
	}
	return false
}

type redirector struct {
	s *server.Server
}

// NewRedirector redirects request to the leader if needs to be handled in the leader.
func NewRedirector(s *server.Server) *redirector {
	return &redirector{s: s}
}

func (h *redirector) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	allowFollowerHandle := len(r.Header.Get(AllowFollowerHandle)) > 0
	if !h.s.IsClosed() && (h.s.GetMember().IsLeader() || allowFollowerHandle) {
		if allowFollowerHandle {
			w.Header().Add(FollowerHandle, "true")
		}
		next(w, r)
		return
	}

	// Prevent more than one redirection.
	if name := r.Header.Get(RedirectorHeader); len(name) != 0 {
		log.Error("redirect but server is not leader", zap.String("from", name), zap.String("server", h.s.Name()))
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
	initHTTPClientOnce.Do(func() {
		var tlsConfig *tls.Config
		tlsConfig, err = h.s.GetSecurityConfig().ToTLSConfig()
		dialClient = &http.Client{
			Transport: &http.Transport{
				DisableKeepAlives: true,
				TLSClientConfig:   tlsConfig,
			},
		}
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	newCustomReverseProxies(urls).ServeHTTP(w, r)
}

type customReverseProxies struct {
	urls   []url.URL
	client *http.Client
}

func newCustomReverseProxies(urls []url.URL) *customReverseProxies {
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
			log.Error("request failed", zap.Error(err))
			continue
		}

		b, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Error("request failed", zap.Error(err))
			continue
		}

		copyHeader(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		if _, err := w.Write(b); err != nil {
			log.Error("write failed", zap.Error(err))
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
