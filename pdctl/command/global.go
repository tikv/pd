// Copyright 2016 PingCAP, Inc.
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

package command

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"

	"github.com/spf13/cobra"
)

func getAddressFromCmd(cmd *cobra.Command, prefix string) string {
	p, err := cmd.Flags().GetString("pd")
	if err != nil {
		fmt.Println("Get pd address error,should set flag with '-u'")
		os.Exit(1)
	}

	u, err := url.Parse(p)
	if err != nil {
		fmt.Println("address is wrong format,should like 'http://127.0.0.1:2379'")
	}
	if u.Scheme == "" {
		u.Scheme = "http"
	}
	s := fmt.Sprintf("%s/%s", u, prefix)
	return s
}

func printResponseError(r *http.Response) {
	fmt.Printf("[%d]:", r.StatusCode)
	io.Copy(os.Stdout, r.Body)
}

// UsageTemplate will used to generate a help information
const UsageTemplate = `Usage:{{if .Runnable}}
  {{if .HasAvailableFlags}}{{appendIfNotPresent .UseLine ""}}{{else}}{{.UseLine}}{{end}}{{end}}{{if .HasAvailableSubCommands}}
  {{if .HasParent}}{{ .Name}} [command]{{else}}[command]{{end}}{{end}}{{if gt .Aliases 0}}

Aliases:
  {{.NameAndAliases}}
{{end}}{{if .HasExample}}

Examples:
{{ .Example }}{{end}}{{ if .HasAvailableSubCommands}}

Available Commands:{{range .Commands}}{{if .IsAvailableCommand}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{ if .HasAvailableLocalFlags}}

Additional help topics:{{range .Commands}}{{if .IsHelpCommand}}
  {{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}{{ if .HasAvailableSubCommands }}

Use "{{if .HasParent}}{{.Name}} [command] help{{else}}[command] help{{end}}" for more information about a command.{{end}}
`
