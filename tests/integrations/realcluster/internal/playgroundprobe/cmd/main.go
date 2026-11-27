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

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tikv/pd/tests/integrations/realcluster/internal/playgroundprobe"
)

const defaultProbeTimeout = 30 * time.Second

// Usage:
//
//	playgroundprobe <playground-binary> [timeout]
//	playgroundprobe --lookup <tiup-binary> [timeout]
//
// The first form prints "usable" or "unusable". The second form prints the path
// reported by `tiup --binary playground`, or nothing if playground is not
// installed, and fails if TiUP does not answer before the timeout.
func main() {
	args := os.Args[1:]
	lookup := len(args) > 0 && args[0] == "--lookup"
	if lookup {
		args = args[1:]
	}
	if len(args) < 1 || len(args) > 2 {
		fmt.Fprintf(os.Stderr, "usage: %s [--lookup <tiup-binary> | <playground-binary>] [timeout]\n", os.Args[0])
		os.Exit(2)
	}
	rawTimeout := ""
	if len(args) == 2 {
		rawTimeout = args[1]
	}
	timeout, err := playgroundprobe.ParseTimeout(rawTimeout, defaultProbeTimeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid TIUP_PROBE_TIMEOUT: %v\n", err)
		os.Exit(2)
	}
	sigCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	ctx, cancel := context.WithTimeout(sigCtx, timeout)
	var (
		binPath string
		usable  bool
	)
	if lookup {
		binPath, err = playgroundprobe.LookupBinary(ctx, args[0], nil)
	} else {
		usable = playgroundprobe.Check(ctx, args[0], nil)
	}
	interrupted := sigCtx.Err() != nil
	cancel()
	stopSignals()
	if interrupted {
		os.Exit(130)
	}
	switch {
	case lookup && err != nil:
		fmt.Fprintf(os.Stderr, "tiup playground lookup timed out after %s: %v\n", timeout, err)
		os.Exit(1)
	case lookup:
		if binPath != "" {
			fmt.Println(binPath)
		}
	case usable:
		fmt.Println("usable")
	default:
		fmt.Println("unusable")
	}
}
