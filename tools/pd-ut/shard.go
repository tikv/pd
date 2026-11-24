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

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// selectPackages uses exact package names so a CI shard cannot also select
// similarly named packages assigned to another runner.
func selectPackages(available []string, selection string) ([]string, error) {
	known := make(map[string]bool, len(available))
	for _, pkg := range available {
		known[pkg] = true
	}
	selected := strings.Split(selection, ",")
	for _, pkg := range selected {
		if !known[pkg] {
			return nil, fmt.Errorf("unknown or repeated package %q", pkg)
		}
		delete(known, pkg)
	}
	return selected, nil
}

func orderTasks(tasks []task, filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	var timings struct {
		Tests map[string]float64 `json:"tests"`
	}
	if err := json.Unmarshal(data, &timings); err != nil {
		return err
	}
	key := func(t task) string { return filepath.Join(modulePath, t.pkg) + "." + t.test }
	slices.SortFunc(tasks, func(a, b task) int {
		ak, bk := key(a), key(b)
		switch {
		case timings.Tests[ak] > timings.Tests[bk]:
			return -1
		case timings.Tests[ak] < timings.Tests[bk]:
			return 1
		default:
			return strings.Compare(ak, bk)
		}
	})
	return nil
}
