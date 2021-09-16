// Copyright 2021 TiKV Project Authors.
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

package prometheus

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/tikv/pd/pkg/autoscaling"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

// HTTPHandler is a handler to handle the prometheus HTTP request.
type HTTPHandler struct {
	svr *server.Server
	rd  *render.Render
}

// NewHTTPHandler creates a HTTPHandler.
func NewHTTPHandler(svr *server.Server, rd *render.Render) *HTTPHandler {
	return &HTTPHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	query := strings.Split(strings.Split(string(body), "&")[0], "=")[1]
	component := getComponentType(query)
	if component != autoscaling.TiKV && component != autoscaling.TiDB {
		h.rd.JSON(w, http.StatusInternalServerError, fmt.Sprintf("unkown component type: %d", component))
		return
	}
	metricType := getMetricType(query)
	if metricType != autoscaling.CPUUsage && metricType != autoscaling.CPUQuota {
		h.rd.JSON(w, http.StatusInternalServerError, fmt.Sprintf("unkown metric type: %d", metricType))
		return
	}

	resp := buildCPUMockData(component, metricType)

	h.rd.JSON(w, http.StatusOK, resp)
}
