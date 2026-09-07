// Copyright 2019 TiKV Project Authors.
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

package testutil

import "go.uber.org/goleak"

// LeakOptions is used to filter goroutines that cannot be synchronously
// stopped by their owning dependencies.
var LeakOptions = []goleak.Option{
	// leveldb.DB.Close does not wait for mpoolDrain, which exits at most one
	// second later after draining the memory pool.
	goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
	// lumberjack v2 never closes millCh, including from Logger.Close, so the
	// rotation worker cannot be stopped by callers.
	goleak.IgnoreTopFunction("gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun"),
}
