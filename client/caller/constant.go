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
