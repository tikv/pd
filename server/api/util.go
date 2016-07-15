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
	"io/ioutil"
	"net/http"

	"github.com/juju/errors"
)

func fromBody(r *http.Request, data interface{}) error {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return errors.Trace(err)
	}

	err = json.Unmarshal(body, data)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}
