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

package api

import (
	"math/rand"
	"net/http"
	"net/http/httputil"

	"github.com/pingcap/pd/server"
)

type redirector struct {
	s *server.Server
}

func newRedirector(s *server.Server) *redirector {
	return &redirector{s: s}
}

func (h *redirector) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	if h.s.IsLeader() {
		next(w, r)
		return
	}

	leader, err := h.s.GetLeader()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if leader == nil {
		http.Error(w, "no leader", http.StatusInternalServerError)
		return
	}

	urls, err := server.ParseUrls(leader.GetAddr())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	u := &urls[rand.Intn(len(urls))]

	// Use unix socket for tests.
	if u.Scheme == "unix" {
		u.Scheme = "http"
		proxy := httputil.NewSingleHostReverseProxy(u)
		proxy.Transport = &http.Transport{Dial: unixDial}
		proxy.ServeHTTP(w, r)
	} else {
		proxy := httputil.NewSingleHostReverseProxy(u)
		proxy.ServeHTTP(w, r)
	}
}
