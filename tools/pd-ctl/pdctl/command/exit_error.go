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

package command

import (
	"errors"
	"fmt"
)

type commandExitError struct {
	code   int
	err    error
	silent bool
}

func (e *commandExitError) Error() string {
	if e.err == nil {
		return fmt.Sprintf("command exited with status %d", e.code)
	}
	return e.err.Error()
}

func (e *commandExitError) Unwrap() error {
	return e.err
}

func newCommandExitError(code int, err error, silent bool) error {
	return &commandExitError{code: code, err: err, silent: silent}
}

// ExitCode returns a command-specific process exit code and whether the error
// has already been represented in the command output.
func ExitCode(err error) (code int, silent bool) {
	var exitErr *commandExitError
	if errors.As(err, &exitErr) {
		return exitErr.code, exitErr.silent
	}
	return 1, false
}
