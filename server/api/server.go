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
	"net/http"

	"github.com/juju/errors"
	"github.com/pingcap/pd/server"
	"github.com/urfave/negroni"
)

// ServeHTTP creates a HTTP service and serves.
func ServeHTTP(addr string, srv *server.Server) error {
	engine := negroni.New()

	recovery := negroni.NewRecovery()
	engine.Use(recovery)

	router := createRouter(srv)
	engine.UseHandler(router)

	err := http.ListenAndServe(addr, engine)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}
