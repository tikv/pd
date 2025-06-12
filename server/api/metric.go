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
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/server"
)

type queryMetric struct {
	s *server.Server
}

func newqueryMetric(s *server.Server) *queryMetric {
	return &queryMetric{s: s}
}

func (h *queryMetric) queryMetric(w http.ResponseWriter, r *http.Request) {
	metricAddr := h.s.GetConfig().PDServerCfg.MetricStorage
	if metricAddr == "" {
		http.Error(w, "metric storage doesn't set", http.StatusInternalServerError)
		return
	}
	u, err := url.Parse(metricAddr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	switch u.Scheme {
	case "http", "https":
		// Replace the pd path with the prometheus http API path.
		r.URL.Path = strings.Replace(r.URL.Path, "pd/api/v1/metric", "api/v1", 1)
		apiutil.NewCustomReverseProxies(h.s.GetHTTPClient(), []url.URL{*u}).ServeHTTP(w, r)
	default:
		// TODO: Support read data by self after support store metric data in PD/TiKV.
		http.Error(w, fmt.Sprintf("schema of metric storage address is no supported, address: %v", metricAddr), http.StatusInternalServerError)
	}
}
