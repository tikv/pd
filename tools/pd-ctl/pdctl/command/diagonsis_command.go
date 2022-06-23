// Copyright 2022 TiKV Project Authors.
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
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
	"github.com/tikv/pd/server/schedule/diagnosis"
)

// var (
// 	diagnosisPrefix = "pd/api/v1/diagnosis"
// )

// NewDiagnosisCommand returns a scheduler command.
func NewDiagnosisCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "diagnose",
		Short: "diagnose commands",
	}
	c.AddCommand(NewBalanceRegionDiagnoseCommand())
	return c
}

// NewBalanceRegionDiagnoseCommand returns commands to diagnose balance-region scheduler.
func NewBalanceRegionDiagnoseCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "balance-region-scheduler <store_id>",
		Short: "diagnose balance-region scheduler",
		Run:   diagnoseBalanceRegionCommandFunc,
	}
	c.AddCommand(
		NewBalanceRegionDiagnosisResultCommand(),
	)
	return c
}

// NewBalanceRegionDiagnosisResultCommand returns commands to get balance-region scheduler diagnosis result.
func NewBalanceRegionDiagnosisResultCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "result <store_id>",
		Short: "get balance-region scheduler diagnosis result",
		Run:   getBalanceRegionDiagnosisResultCommandFunc,
	}
	c.Flags().String("detailed", "", "detailed diagnosis")
	return c
}

func diagnoseBalanceRegionCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	p := cmd.Name()
	path := path.Join(schedulersPrefix, p, "diagnose", args[0])
	r, err := doRequest(cmd, path, http.MethodPost, http.Header{})
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			err = errors.New("[404] scheduler not found")
		}
		cmd.Println(err)
		return
	}
	cmd.Println(r)
}

func getBalanceRegionDiagnosisResultCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	p := cmd.Parent().Name()
	path := path.Join(schedulersPrefix, p, "diagnose", args[0])
	r, err := doRequest(cmd, path, http.MethodGet, http.Header{})
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			err = errors.New("[404] scheduler not found")
		}
		cmd.Println(err)
		return
	}
	cmd.Println(convertDiagnosisOutput(r, cmd.Flag("detailed") != nil))
}

func convertDiagnosisOutput(content string, detailed bool) string {
	result := &diagnosis.DiagnosisResult{}
	err := json.Unmarshal([]byte(content), result)
	if err != nil {
		return content
	}
	lines := make([]string, 0)
	lines = append(lines, fmt.Sprintf("%s :", result.SchedulerName))
	lines = append(lines, fmt.Sprintf("\tStore %-6d: %s", result.StoreID, result.Description))
	lines = append(lines, fmt.Sprintf("\t              %s", result.Reason))
	if detailed {
		lines = append(lines, fmt.Sprintf("\t              |%s|%s|%s|%s|", template("Step", 40), template("Failure Reason", 40), template("Ratio", 20), template("Sample Object", 30)))
		for _, detail := range result.Detailed {
			lines = append(lines, fmt.Sprintf("\t              |%s|%s|%s|%s|", template(detail.Step, 40), template(detail.Reason, 40), template(detail.Ratio, 20), template(detail.SampleObject, 30)))
		}
	}

	return strings.Join(lines, "\n")
}

func template(title string, length int) string {
	len := len(title)
	leftBlank := (length - len) / 2
	rightBlank := (length - len + 1) / 2
	return fmt.Sprintf("%"+strconv.Itoa(leftBlank)+"s%s%"+strconv.Itoa(rightBlank)+"s", "", title, "")
}
