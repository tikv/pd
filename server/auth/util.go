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
// See the License for the specific language governing permissionKeys and
// limitations under the License.
//

package auth

import (
	"regexp"

	"github.com/tikv/pd/pkg/errs"
)

var (
	patName = regexp.MustCompile("^([A-Za-z])[A-Za-z0-9_]*$")
)

func validateName(name string) error {
	if patName.MatchString(name) {
		return nil
	}
	return errs.ErrInvalidName.FastGenByArgs()
}
