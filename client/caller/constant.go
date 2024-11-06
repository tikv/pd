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
	// ID represents the caller ID.
	ID string
	// Component represents the caller component.
	Component string
)

// nolint:exported
const (
	Unknown   ID = "unknown"
	Tidb      ID = "tidb"
	Pd        ID = "pd"
	Tikv      ID = "tikv"
	TiFlash   ID = "tiflash"
	Lightning ID = "lightning"
	Dumpling  ID = "dumpling"
	Br        ID = "br"
	Dm        ID = "dm"
	Cdc       ID = "cdc"
	Operator  ID = "operator"
	Dashboard ID = "dashboard"

	// If the component you used is not in the list, please add it here.
	Ddl Component = "ddl"

	// TestID is used for test.
	TestID ID = "test"
	// TestComponent is used for test.
	TestComponent Component = "test"
)
