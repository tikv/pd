// Copyright 2026 TiKV Project Authors.
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

package command

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestMicroServicesCommandUsageListsAllServices(t *testing.T) {
	require.Equal(t, "microservice <tso|scheduling|router|resource-manager>", NewMicroServicesCommand().Use)
}

// TestResourceManagerCommandUsesDiscoveryServiceName guards the one non-trivial
// invariant newMSResourceManagerCommand relies on: the CLI name ("resource-manager")
// differs from the etcd discovery key (constant.ResourceManagerServiceName,
// "resource_manager"), so its Run funcs must pass the service name explicitly
// rather than deriving it from cmd.Parent().Name() like tso/scheduling/router do.
func TestResourceManagerCommandUsesDiscoveryServiceName(t *testing.T) {
	re := require.New(t)
	run := func(args ...string) string {
		var gotPath string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[]`))
		}))
		defer server.Close()

		root := &cobra.Command{Use: "pd-ctl"}
		root.SetOut(io.Discard)
		root.PersistentFlags().StringP("pd", "u", "", "address of PD")
		root.AddCommand(NewMicroServicesCommand())
		root.SetArgs(append([]string{"-u", server.URL}, args...))
		re.NoError(root.Execute())
		return gotPath
	}

	re.Equal("/pd/api/v2/ms/members/resource_manager", run("microservice", "resource-manager", "members"))
	re.Equal("/pd/api/v2/ms/primary/resource_manager", run("microservice", "resource-manager", "primary"))
}
