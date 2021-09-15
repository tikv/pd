// Copyright 2020 TiKV Project Authors.
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
	"context"
	"net/http"

	"github.com/tikv/pd/pkg/apiutil/serverapi"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
	"github.com/urfave/negroni"
)

const prometheusPrefix = "/prometheus/api/v1/query"

var (
	prometheusServiceGroup = server.ServiceGroup{
		Name:       "prometheus",
		Version:    "v1alpha",
		IsCore:     false,
		PathPrefix: prometheusPrefix,
	}
)

// NewHandler creates an HTTP handler for prometheus.
func NewHandler(_ context.Context, svr *server.Server) (http.Handler, server.ServiceGroup, error) {
	prometheusHandler := http.NewServeMux()
	rd := render.New(render.Options{
		IndentJSON: true,
	})
	prometheusHandler.Handle(prometheusPrefix, negroni.New(
		serverapi.NewRedirector(svr),
		negroni.Wrap(NewHTTPHandler(svr, rd))),
	)
	return prometheusHandler, prometheusServiceGroup, nil
}
