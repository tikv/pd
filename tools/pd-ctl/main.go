// Copyright 2016 TiKV Project Authors.
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

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"go.uber.org/zap/zapcore"

	"github.com/pingcap/log"

	"github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/pdctl/command"
)

func main() {
	pdAddr := os.Getenv("PD_ADDR")
	if pdAddr != "" {
		os.Args = append(os.Args, "-u", pdAddr)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)

	log.SetLevel(zapcore.FatalLevel)
	var inputs []string
	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		in, err := pdctl.ReadStdin(os.Stdin)
		if err != nil {
			fmt.Println(err)
			return
		}
		inputs = in
	}
	exitCode := pdctl.MainStartContext(ctx, append(os.Args[1:], inputs...))
	if ctx.Err() != nil {
		exitCode = 130
	}
	stop()
	if command.PDCli != nil {
		command.PDCli.Close()
	}
	os.Exit(exitCode)
}
