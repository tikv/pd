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

package keypath

import "fmt"

// MemberBinaryDeployPath returns the member binary deploy path.
func MemberBinaryDeployPath(id uint64) string {
	return fmt.Sprintf(memberBinaryDeployPathFormat, ClusterID(), id)
}

// MemberGitHashPath returns the member git hash path.
func MemberGitHashPath(id uint64) string {
	return fmt.Sprintf(memberGitHashPath, ClusterID(), id)
}

// MemberBinaryVersionPath returns the member binary version path.
func MemberBinaryVersionPath(id uint64) string {
	return fmt.Sprintf(memberBinaryVersionPathFormat, ClusterID(), id)
}
