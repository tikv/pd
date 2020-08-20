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

package typeutil

import "strings"

// TiDBInfo record the detail tidb info
type TiDBInfo struct {
	Version        string            `json:"version"`
	StartTimestamp int64             `json:"start_timestamp"`
	Labels         map[string]string `json:"labels"`
	GitHash        string            `json:"git_hash"`
	Address        string
}

// GetLabelValue returns a label's value (if exists).
func (t *TiDBInfo) GetLabelValue(key string) string {
	for k, v := range t.GetLabels() {
		if strings.EqualFold(k, key) {
			return v
		}
	}
	return ""
}

// GetLabels returns the labels of the tidb.
func (t *TiDBInfo) GetLabels() map[string]string {
	return t.Labels
}
