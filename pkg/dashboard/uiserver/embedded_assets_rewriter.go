// Copyright 2020 PingCAP, Inc.
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

package uiserver

import (
	"html"
	"os"
	"strings"
	"sync"
)

var once sync.Once

type modifiedFileInfo struct {
	os.FileInfo
	size int64
}

func (f modifiedFileInfo) Size() int64 {
	return f.size
}

func (f modifiedFileInfo) Sys() interface{} {
	return nil
}

// InitAssetFS init the static resources with given public path prefix.
func InitAssetFS(prefix string) {
	once.Do(func() {
		rewrite := func(assetPath string) {
			a, err := _bindata[assetPath]()
			if err != nil {
				panic("Asset " + assetPath + " not found.")
			}
			tmplText := string(a.bytes)
			updated := strings.ReplaceAll(tmplText, "__PUBLIC_PATH_PREFIX__", html.EscapeString(prefix))
			a.bytes = []byte(updated)
			a.info = modifiedFileInfo{a.info, int64(len(a.bytes))}
			_bindata[assetPath] = func() (*asset, error) {
				return a, nil
			}
		}
		rewrite("build/index.html")
		rewrite("build/diagnoseReport.html")
	})
}
