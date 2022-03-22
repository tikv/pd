// Copyright 2022 TiKV Project Authors.
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

package apiutil

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/pingcap/errcode"
	"github.com/pingcap/errors"
	"github.com/tikv/pd/pkg/errs"
	"github.com/unrolled/render"
)

// FieldError connects an error to a particular field
type FieldError struct {
	error
	field string
}

// ParseUint64VarsField connects strconv.ParseUint with request variables
// It hardcodes the base to 10 and bit size to 64
// Any error returned will connect the requested field to the error via FieldError
func ParseUint64VarsField(vars map[string]string, varName string) (uint64, *FieldError) {
	str, ok := vars[varName]
	if !ok {
		return 0, &FieldError{field: varName, error: fmt.Errorf("field %s not present", varName)}
	}
	parsed, err := strconv.ParseUint(str, 10, 64)
	if err == nil {
		return parsed, nil
	}
	return parsed, &FieldError{field: varName, error: err}
}

// CollectEscapeStringOption
func CollectEscapeStringOption(option string, input map[string]interface{}, collectors ...func(v string)) error {
	if v, ok := input[option].(string); ok {
		value, err := url.QueryUnescape(v)
		if err != nil {
			return err
		}
		for _, c := range collectors {
			c(value)
		}
		return nil
	}
	return errs.ErrOptionNotExist.FastGenByArgs(option)
}

// CollectStringOption
func CollectStringOption(option string, input map[string]interface{}, collectors ...func(v string)) error {
	if v, ok := input[option].(string); ok {
		for _, c := range collectors {
			c(v)
		}
		return nil
	}
	return errs.ErrOptionNotExist.FastGenByArgs(option)
}

// ParseKey
func ParseKey(name string, input map[string]interface{}) ([]byte, string, error) {
	k, ok := input[name]
	if !ok {
		return nil, "", fmt.Errorf("missing %s", name)
	}
	rawKey, ok := k.(string)
	if !ok {
		return nil, "", fmt.Errorf("bad format %s", name)
	}
	returned, err := hex.DecodeString(rawKey)
	if err != nil {
		return nil, "", fmt.Errorf("split key %s is not in hex format", name)
	}
	return returned, rawKey, nil
}

// ReadJSON reads a JSON data from r and then closes it.
// An error due to invalid json will be returned as a JSONError
func ReadJSON(r io.ReadCloser, data interface{}) error {
	var err error
	defer DeferClose(r, &err)
	b, err := io.ReadAll(r)
	if err != nil {
		return errors.WithStack(err)
	}

	err = json.Unmarshal(b, data)
	if err != nil {
		return tagJSONError(err)
	}

	return err
}

// ReadJSONRespondError writes json into data.
// On error respond with a 400 Bad Request
func ReadJSONRespondError(rd *render.Render, w http.ResponseWriter, body io.ReadCloser, data interface{}) error {
	err := ReadJSON(body, data)
	if err == nil {
		return nil
	}
	var errCode errcode.ErrorCode
	if jsonErr, ok := errors.Cause(err).(JSONError); ok {
		errCode = errcode.NewInvalidInputErr(jsonErr.Err)
	} else {
		errCode = errcode.NewInternalErr(err)
	}
	ErrorResp(rd, w, errCode)
	return err
}
