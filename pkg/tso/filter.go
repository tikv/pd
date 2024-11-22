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

package tso

// FilterDCLocation will filter out the allocatorGroup with a given dcLocation.
func FilterDCLocation(dcLocation string) func(ag *allocatorGroup) bool {
	return func(ag *allocatorGroup) bool { return ag.dcLocation == dcLocation }
}

// FilterUninitialized will filter out the allocatorGroup uninitialized.
func FilterUninitialized() func(ag *allocatorGroup) bool {
	return func(ag *allocatorGroup) bool { return !ag.allocator.IsInitialize() }
}

// FilterUnavailableLeadership will filter out the allocatorGroup whose leadership is unavailable.
func FilterUnavailableLeadership() func(ag *allocatorGroup) bool {
	return func(ag *allocatorGroup) bool { return !ag.leadership.Check() }
}
