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

package operator

// OpStatus represents the status of an Operator.
type OpStatus = uint32

// Status list
const (
	// Status list
	CREATED OpStatus = iota // Just created.
	STARTED                 // Started and not finished
	// Followings are end status
	SUCCESS  // Finished successfully
	CANCELED // Canceled due to some reason
	EXPIRED  // Didn't start to run for too long
	TIMEOUT  // Running for too long
	// Status list end
	statusCount    // Total count of status
	firstEndStatus = SUCCESS
)

var statusString [statusCount]string = [statusCount]string{
	CREATED:  "Created",
	STARTED:  "Started",
	SUCCESS:  "Success",
	CANCELED: "Canceled",
	EXPIRED:  "Expired",
	TIMEOUT:  "Timeout",
}

// StatusToString converts Status to string.
func StatusToString(s OpStatus) string {
	if s < statusCount {
		return statusString[s]
	}
	return "Unknown"
}
