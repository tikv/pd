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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package autoscaling

import (
	"context"
	"net/http"

	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/apiutil/serverapi"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
	"github.com/urfave/negroni"
)

const autoScalingPrefix = "/autoscaling"

var (
	autoscalingServiceGroup = apiutil.APIServiceGroup{
		Name:       "autoscaling",
		Version:    "v1alpha",
		IsCore:     false,
		PathPrefix: autoScalingPrefix,
	}
)

// NewHandler creates a HTTP handler for auto scaling.
func NewHandler(_ context.Context, svr *server.Server) (http.Handler, apiutil.APIServiceGroup, error) {
	autoScalingHandler := http.NewServeMux()
	rd := render.New(render.Options{
		IndentJSON: true,
	})
	autoScalingHandler.Handle(autoScalingPrefix, negroni.New(
		serverapi.NewRedirector(svr),
		negroni.Wrap(NewHTTPHandler(svr, rd))),
	)
	return autoScalingHandler, autoscalingServiceGroup, nil
}
