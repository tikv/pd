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
	"time"

	"github.com/tikv/pd/tests/integrations/realcluster/internal/playgroundprobe"
)

const defaultProbeTimeout = 30 * time.Second

func main() {
	if len(os.Args) < 2 || len(os.Args) > 3 {
		fmt.Fprintf(os.Stderr, "usage: %s <playground-binary> [timeout]\n", os.Args[0])
		os.Exit(2)
	}
	rawTimeout := ""
	if len(os.Args) == 3 {
		rawTimeout = os.Args[2]
	}
	timeout, err := playgroundprobe.ParseTimeout(rawTimeout, defaultProbeTimeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid TIUP_PROBE_TIMEOUT: %v\n", err)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	usable := playgroundprobe.Check(ctx, os.Args[1], nil)
	cancel()
	if usable {
		fmt.Println("usable")
		return
	}
	fmt.Println("unusable")
}
