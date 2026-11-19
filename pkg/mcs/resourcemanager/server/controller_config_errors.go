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

package server

import (
	stderrors "errors"
)

var errControllerConfigValidation = stderrors.New("controller config validation failed")

func wrapControllerConfigValidationError(err error) error {
	if err == nil {
		return nil
	}
	return &controllerConfigValidationError{err: err}
}

// IsControllerConfigValidationError reports whether err is a controller-config validation error.
func IsControllerConfigValidationError(err error) bool {
	return stderrors.Is(err, errControllerConfigValidation)
}

type controllerConfigValidationError struct {
	err error
}

func (e *controllerConfigValidationError) Error() string {
	return e.err.Error()
}

func (e *controllerConfigValidationError) Unwrap() error {
	return e.err
}

// Is makes controllerConfigValidationError match errControllerConfigValidation via errors.Is.
func (*controllerConfigValidationError) Is(target error) bool {
	return target == errControllerConfigValidation
}
