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

// +build without_dashboard

package dashboard

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/pingcap-incubator/tidb-dashboard/pkg/config"

	"github.com/tikv/pd/server"
)

var (
	serviceGroup = server.ServiceGroup{
		Name:       "dashboard",
		Version:    "v1",
		IsCore:     false,
		PathPrefix: config.UIPathPrefix,
	}
)

// SetCheckInterval does nothing
func SetCheckInterval(time.Duration) {}

// GetServiceBuilders returns a empty Dashboard Builder
func GetServiceBuilders() []server.HandlerBuilder {
	return []server.HandlerBuilder{
		func(context.Context, *server.Server) (http.Handler, server.ServiceGroup, error) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, "Dashboard is not built.\n")
			})
			return handler, serviceGroup, nil
		},
	}
}
