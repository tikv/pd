// Copyright 2021 TiKV Project Authors.
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

package distroutil

import (
	"os"
	"path/filepath"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb-dashboard/util/distro"
	"go.uber.org/zap"
)

const (
	resFolderName   string = "distro-res"
	stringsFileName string = "strings.json"
)

// MustGetResPath returns the default path that a distribution resource should be placed,
// which is the ${BinaryPath}/distro-res.
func MustGetResPath() string {
	exePath, err := os.Executable()
	if err != nil {
		log.Fatal("failed to read the execution path", zap.Error(err))
		return ""
	}
	return filepath.Join(filepath.Dir(exePath), resFolderName)
}

// MustLoadAndReplaceStrings loads the distro strings from ${BinaryPath}/distro-res/strings.json
// and replace the strings in the TiDB Dashboard. If the strings file does not exist, the default
// distro string will be used.
func MustLoadAndReplaceStrings() {
	resPath := MustGetResPath()
	strings, err := distro.ReadResourceStringsFromFile(filepath.Join(resPath, stringsFileName))
	if err != nil {
		log.Fatal("failed to load distro strings", zap.Error(err))
	}
	log.Info("using distribution strings", zap.Any("strings", strings))
	distro.ReplaceGlobal(strings)
}
