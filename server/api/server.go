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

	"github.com/unrolled/render"
	"github.com/urfave/negroni"
)

var rd = render.New()

// ServeHTTP creates a HTTP service.
func ServeHTTP(addr string) {
	engine := negroni.New()

	recovery := negroni.NewRecovery()
	engine.Use(recovery)

	router := buildNewRouter()
	engine.UseHandler(router)

	http.ListenAndServe(addr, engine)
}
