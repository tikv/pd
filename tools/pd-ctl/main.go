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

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/pingcap/pd/v4/tools/pd-ctl/pdctl"
)

func main() {
	//pdAddr := os.Getenv("PD_ADDR")
	//if pdAddr != "" {
	//	os.Args = append(os.Args, "-u", pdAddr)
	//}
	//flag.CommandLine.ParseErrorsWhitelist.UnknownFlags = true
	//flag.Parse()

	var input []string
	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		pdctl.Detach = true
		b, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			fmt.Println(err)
			return
		}
		input = strings.Split(strings.TrimSpace(string(b[:])), " ")
	}

	pdctl.Start(append(os.Args[1:], input...))
}
