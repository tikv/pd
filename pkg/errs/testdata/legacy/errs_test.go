// Copyright 2026 TiKV Project Authors.
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

package legacy_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/errcode"
	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/errs"
)

type clientError struct {
	Detail string `json:"detail"`
}

func (e *clientError) Error() string { return e.Detail }

type operationError struct{ errcode.CodedError }

func (operationError) GetOperation() string { return "update store" }

func TestLegacyHTTPError(t *testing.T) {
	data := &clientError{Detail: "store limit reached"}
	quota := errcode.NewCode("quota").SetHTTP(http.StatusTooManyRequests).Child("quota.exceeded")
	for _, code := range []errcode.Code{errcode.InvalidInputCode, quota, errcode.UnimplementedCode} {
		t.Run(string(code.CodeStr()), func(t *testing.T) {
			re := require.New(t)
			original := operationError{errcode.NewCodedError(data, code)}
			_, native := any(original).(errs.HTTPError)
			re.False(native, "the real legacy Code() signature must use reflection")
			adapted := errs.AsHTTPError(original)
			re.NotNil(adapted)
			re.Equal(string(code.CodeStr()), adapted.Code())
			re.Equal(code.HTTPCode(), adapted.HTTPCode())
			re.Same(data, adapted.GetClientData())
			re.Equal(original.Error(), adapted.Error())
			operation, ok := adapted.(interface{ GetOperation() string })
			re.True(ok)
			re.Equal("update store", operation.GetOperation())
			for _, wrapped := range []error{errors.Annotate(original, "store"), fmt.Errorf("store: %w", original)} {
				found := errs.FindHTTPError(wrapped)
				re.NotNil(found)
				re.Equal(adapted.Code(), found.Code())
				re.Equal(adapted.HTTPCode(), found.HTTPCode())
				re.Same(data, found.GetClientData())
				re.Equal(wrapped.Error(), found.Error())
			}
		})
	}
}

func TestLegacyHTTPErrorGroup(t *testing.T) {
	re := require.New(t)
	data := &clientError{Detail: "invalid store"}
	input := errcode.NewInvalidInputErr(data)
	missing := errcode.NewNotFoundErr(data)
	group := errcode.Combine(input, missing)
	for _, original := range []error{group, errors.Annotate(group, "store"), fmt.Errorf("store: %w", group)} {
		found := errs.FindHTTPError(original)
		re.NotNil(found)
		re.Equal("input", found.Code())
		re.Equal(http.StatusBadRequest, found.HTTPCode())
		re.Equal(original.Error(), found.Error())
		re.Same(data, found.GetClientData())
		members := errors.Errors(found)
		re.Len(members, 2)
		re.Equal("input", errs.FindHTTPError(members[0]).Code())
		re.Equal("missing", errs.FindHTTPError(members[1]).Code())
	}
}
