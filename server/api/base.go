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
	"encoding/json"

	"github.com/astaxie/beego"
	"github.com/ngaut/log"
)

type baseError struct {
	Code  int32  `json:"code"`
	Error string `json:"error"`
}

type baseController struct {
	beego.Controller
}

func (c *baseController) serveError(code int32, err error) {
	baseError := &baseError{
		Code:  code,
		Error: err.Error(),
	}

	json, err := json.Marshal(baseError)
	if err != nil {
		log.Errorf("failed to marshal object, %v", baseError)
		c.Abort("500")
	} else {
		c.CustomAbort(500, string(json))
	}
}
