// Copyright 2024 TiKV Project Authors.
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

package caller

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

type (
	// Caller ID can be understood as a binary file; it is a process.
	ID string
	// Caller component refers to the components within the process.
	Component string
)

const (
	// TestID is used for test.
	TestID ID = "test"
	// TestComponent is used for test.
	TestComponent Component = "test"
)

var processName ID

func init() {
	processName = ID(filepath.Base(os.Args[0]))
}

// GetCallerID returns the name of the currently running process
func GetCallerID() ID {
	return processName
}

// GetComponent returns the package path of the calling function
// The argument upperLayer specifies the number of stack frames to ascend.
// NOTE: This function is time-consuming and please do not use it in high qps scenarios.
func GetComponent(upperLayer int) Component {
	// Get the program counter for the calling function
	pc, _, _, ok := runtime.Caller(upperLayer + 1)
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
