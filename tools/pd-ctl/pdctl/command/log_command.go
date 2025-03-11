// Copyright 2018 TiKV Project Authors.
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
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/spf13/cobra"

	"github.com/tikv/pd/pkg/utils/apiutil"
)

var (
	logPrefix = "pd/api/v1/admin/log"
)

// NewLogCommand New a log subcommand of the rootCmd
func NewLogCommand() *cobra.Command {
	conf := &cobra.Command{
		Use:   "log [fatal|error|warn|info|debug] [addr]",
		Short: "set log level",
		Run:   logCommandFunc,
	}
	return conf
}

func logCommandFunc(cmd *cobra.Command, args []string) {
	var err error
	if len(args) == 0 || len(args) > 2 {
		cmd.Println(cmd.UsageString())
		return
	}

	data, err := json.Marshal(args[0])
	if err != nil {
		cmd.Printf("Failed to set log level: %s\n", err)
		return
	}

	if len(args) == 2 {
		url, err := checkURL(args[1])
		if err != nil {
			cmd.Printf("Failed to parse address %v: %s\n", args[1], err)
			return
		}
		_, err = doRequestSingleEndpoint(cmd, url, logPrefix, http.MethodPost, http.Header{"Content-Type": {"application/json"}, apiutil.PDAllowFollowerHandleHeader: {"true"}},
			WithBody(bytes.NewBuffer(data)))
		if err != nil {
			cmd.Printf("Failed to set %v log level: %s\n", args[1], err)
			return
		}
	} else {
		_, err = doRequest(cmd, logPrefix, http.MethodPost, http.Header{"Content-Type": {"application/json"}},
			WithBody(bytes.NewBuffer(data)))
		if err != nil {
			cmd.Printf("Failed to set log level: %s\n", err)
			return
		}
	}
	cmd.Println("Success!")
}
