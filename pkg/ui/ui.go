// Copyright 2019 PingCAP, Inc.
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

package ui

import (
	"bytes"
	"os"
	"time"

	assetfs "github.com/elazarl/go-bindata-assetfs"
	"net/http"
)

// Asset loads and returns the asset for the given name.
// It returns an error if the asset could not be found or
// could not be loaded.
var Asset func(name string) ([]byte, error)

// AssetDir returns the file names below a certain
// directory embedded in the file by go-bindata.
// For example if you run go-bindata on data/... and data contains the
// following hierarchy:
//     data/
//       foo.txt
//       img/
//         a.png
//         b.png
// then AssetDir("data") would return []string{"foo.txt", "img"},
// AssetDir("data/img") would return []string{"a.png", "b.png"},
// AssetDir("foo.txt") and AssetDir("notexist") would return an error, and
// AssetDir("") will return []string{"data"}.
var AssetDir func(name string) ([]string, error)

// AssetInfo loads and returns the asset info for the given name.
// It returns an error if the asset could not be found or
// could not be loaded.
var AssetInfo func(name string) (os.FileInfo, error)

var indexHTML = []byte(`
<!DOCTYPE html>
<title>PD-Dashboard</title>
Binary built without web UI.
<hr>
<em>Have fun</em>`)

// Handler returns an http.Handler that serves the UI.
func Handler() http.Handler {
	fileServer := http.FileServer(&assetfs.AssetFS{
		Asset:     Asset,
		AssetDir:  AssetDir,
		AssetInfo: AssetInfo,
	})

	enableWebUI := Asset != nil && AssetDir != nil && AssetInfo != nil

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !enableWebUI {
			http.ServeContent(w, r, "index.html", time.Now(), bytes.NewReader(indexHTML))
			return
		}

		fileServer.ServeHTTP(w, r)
	})
}
