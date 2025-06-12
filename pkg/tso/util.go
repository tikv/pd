// Copyright 2023 TiKV Project Authors.
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

package tso

import (
	"fmt"
	"regexp"
	"strconv"
)

// ExtractKeyspaceGroupIDFromPath extracts keyspace group id from the given path, which contains
// the pattern of `tso/keyspace_groups/membership/(\d{5})$`.
func ExtractKeyspaceGroupIDFromPath(compiledRegexp *regexp.Regexp, path string) (uint32, error) {
	match := compiledRegexp.FindStringSubmatch(path)
	if match == nil {
		return 0, fmt.Errorf("invalid keyspace group id path: %s", path)
	}
	id, err := strconv.ParseUint(match[1], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse keyspace group ID: %v", err)
	}
	return uint32(id), nil
}
