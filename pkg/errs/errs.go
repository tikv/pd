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

package errs

import (
	"strings"

	"github.com/pingcap/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ZapError is used to make the log output easier.
func ZapError(err error, causeError ...error) zap.Field {
	if err == nil {
		return zap.Skip()
	}
	if e, ok := err.(*errors.Error); ok {
		if len(causeError) >= 1 {
			err = e.Wrap(causeError[0]).FastGenWithCause()
		} else {
			err = e.FastGenByArgs()
		}
	}
	return zap.Field{Key: "error", Type: zapcore.ErrorType, Interface: err}
}

// AggregateErrors aggregate errors into one error
func AggregateErrors(errs []error) error {
	if len(errs) < 1 {
		return nil
	}
	if len(errs) == 1 {
		return errs[0]
	}
	s := make([]string, 0, len(errs))
	for id, err := range errs {
		s[id] = err.Error()
	}
	return errors.New("[" + strings.Join(s, ",") + "]")
}
