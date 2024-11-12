// Copyright 2024 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package caller

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// GetCallerID returns the name of the currently running process
func GetCallerID() ID {
	return ID(filepath.Base(os.Args[0]))
}

// GetComponent returns the package path of the calling function
// The argument i is the number of stack frames to ascend.
func GetComponent(i int) Component {
	// Get the program counter for the calling function
	pc, _, _, ok := runtime.Caller(i + 1)
	if !ok {
		return "unknown"
	}

	// Retrieve the full function name, including the package path
	fullFuncName := runtime.FuncForPC(pc).Name()

	// Separates the package and function
	lastSlash := strings.LastIndex(fullFuncName, ".")

	// Extract the package name
	return Component(fullFuncName[:lastSlash])
}
