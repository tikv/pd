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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package uiserver

import (
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/pingcap/tidb-dashboard/pkg/config"
	"github.com/pingcap/tidb-dashboard/pkg/uiserver"

	"github.com/tikv/pd/pkg/dashboard/distroutil"
)

var once sync.Once

// Assets returns the Assets FileSystem of the dashboard UI
// NOTE: if you see "undefined: assets" error, please run `make dashboard-ui` in the root directory of the repository.
func Assets(cfg *config.Config) http.FileSystem {
	once.Do(func() {
		resPath := distroutil.MustGetResPath()
		uiserver.RewriteAssets(assets, cfg, resPath, func(fs http.FileSystem, f http.File, path, newContent string, bs []byte) {
			m := fs.(vfsgen۰FS)
			fi := f.(os.FileInfo)
			m[path] = &vfsgen۰CompressedFileInfo{
				name:              fi.Name(),
				modTime:           time.Now(),
				uncompressedSize:  int64(len(newContent)),
				compressedContent: bs,
			}
		})
	})
	return assets
}
