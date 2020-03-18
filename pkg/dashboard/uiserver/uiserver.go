// Copyright 2020 PingCAP, Inc.
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

package uiserver

import (
	"context"
	"io"
	"net/http"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/server"
	"go.uber.org/zap"
)

var serviceGroup = server.ServiceGroup{
	Name:       "dashboard-ui",
	Version:    "v1",
	IsCore:     false,
	PathPrefix: "/dashboard/",
}

// NewService returns an http.Handler that serves the dashboard UI
func NewService(ctx context.Context, srv *server.Server) (http.Handler, server.ServiceGroup) {
	fs := assetFS()
	if fs != nil {
		fileServer := http.StripPrefix(serviceGroup.PathPrefix, http.FileServer(fs))
		log.Info("Enabled Dashboard UI", zap.String("path", serviceGroup.PathPrefix))
		return fileServer, serviceGroup
	}

	emptyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "Dashboard UI is not built.\n")
	})
	return emptyHandler, serviceGroup
}
