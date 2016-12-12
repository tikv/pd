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
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/bobappleyard/readline"
	"github.com/pingcap/pd/pdctl"
)

var url string

func init() {
	flag.StringVar(&url, "u", "http://127.0.0.1:2379", "the pd address")
}

func main() {
	flag.Parse()
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		fmt.Printf("\nGot signal [%v] to exit.\n", sig)
		readline.Cleanup()
		switch sig {
		case syscall.SIGTERM:
			os.Exit(0)
		default:
			os.Exit(1)
		}
	}()

	loop()
}

func loop() {
	for {
		line, err := readline.String("> ")
		if err != nil {
			continue
		}

		if line == "exit" {
			os.Exit(0)
		}

		readline.AddHistory(line)
		args := strings.Split(line, " ")
		args = append(args, "-u", url)
		usage, err := pdctl.Start(args)
		if err != nil {
			fmt.Println(usage)
		}
	}
}
