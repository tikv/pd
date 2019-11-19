// Copyright 2019 PingCAP, Inc.
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
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/config"
	"net/http"
)

const prometheusQueryAPI  = "/api/v1/query"

type queryMetric struct {
	s *server.Server
}

func newQueryMetric(s *server.Server) *queryMetric {
	return &queryMetric{s: s}
}

func (h *queryMetric) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	metricAddr := h.s.GetConfig().Metric.StorageAddress
	if metricAddr == "" {
		metricAddr = "http://127.0.0.1:9090"
	}
	urls, err := config.ParseUrls(metricAddr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	r.URL.Path=prometheusQueryAPI
	newCustomReverseProxies(urls).ServeHTTP(w, r)
}

