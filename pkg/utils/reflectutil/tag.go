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

package reflectutil

import (
	"reflect"
	"strings"
)

// FindJSONFullTagByChildTag is used to find field by child json tag recursively and return the full tag of field.
// If we have both "a.c" and "b.c" config items, for a given c, it's hard for us to decide which config item it represents.
// We'd better to naming a config item without duplication.
func FindJSONFullTagByChildTag(t reflect.Type, tag string) string {
	for i := range t.NumField() {
		field := t.Field(i)

		column := field.Tag.Get("json")
		c := strings.Split(column, ",")
		if c[0] == tag {
			return c[0]
		}

		if field.Type.Kind() == reflect.Struct {
			path := FindJSONFullTagByChildTag(field.Type, tag)
			if path == "" {
				continue
			}
			return field.Tag.Get("json") + "." + path
		}
	}
	return ""
}

// FindSameFieldByJSON is used to check whether there is same field between `m` and `v`
func FindSameFieldByJSON(v any, m map[string]any) bool {
	t := reflect.TypeOf(v).Elem()
	for i := range t.NumField() {
		jsonTag := t.Field(i).Tag.Get("json")
		if i := strings.Index(jsonTag, ","); i != -1 { // trim 'foobar,string' to 'foobar'
			jsonTag = jsonTag[:i]
		}
		if _, ok := m[jsonTag]; ok {
			return true
		}
	}
	return false
}

// FindFieldByJSONTag is used to find field by full json tag recursively and return the field
func FindFieldByJSONTag(t reflect.Type, tags []string) reflect.Type {
	if len(tags) == 0 {
		return t
	}
	if t.Kind() != reflect.Struct {
		return nil
	}
	tag := tags[0]
	tagRemain := tags[1:]
	for i := range t.NumField() {
		jsonTag := t.Field(i).Tag.Get("json")
		if j := strings.Index(jsonTag, ","); j != -1 { // trim 'foobar,string' to 'foobar'
			jsonTag = jsonTag[:j]
		}
		if jsonTag == tag {
			return FindFieldByJSONTag(t.Field(i).Type, tagRemain)
		}
	}
	return nil
}
