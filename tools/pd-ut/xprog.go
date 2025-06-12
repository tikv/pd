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
//go:build xprog
// +build xprog

package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	// See https://github.com/golang/go/issues/15513#issuecomment-773994959
	// go test --exec=xprog ./...
	// Command line args looks like:
	// '$CWD/xprog /tmp/go-build2662369829/b1382/aggfuncs.test -test.paniconexit0 -test.timeout=10m0s'
	// This program moves the test binary /tmp/go-build2662369829/b1382/aggfuncs.test to someplace else for later use.

	// Extract the current work directory
	cwd := os.Args[0]
	cwd = cwd[:len(cwd)-len(filepath.Join("bin", "xprog"))]

	testBinaryPath := os.Args[1]
	dir, _ := filepath.Split(testBinaryPath)

	// Extract the package info from /tmp/go-build2662369829/b1382/importcfg.link
	pkg := getPackageInfo(dir)

	var prefix = filepath.Join("github.com", "tikv", "pd")
	if !strings.HasPrefix(pkg, prefix) {
		os.Exit(-3)
	}

	// github.com/tikv/pd/server/api/api.test => server/api/api
	pkg = pkg[len(prefix) : len(pkg)-len(".test")]

	_, file := filepath.Split(pkg)

	// The path of the destination file looks like $CWD/server/api/api.test.bin
	newName := filepath.Join(cwd, pkg, file+".test.bin")

	if err1 := os.Rename(testBinaryPath, newName); err1 != nil {
		// Rename fail, handle error like "invalid cross-device linkcd tools/check"
		err1 = MoveFile(testBinaryPath, newName)
		if err1 != nil {
			os.Exit(-4)
		}
	}
}

func getPackageInfo(dir string) string {
	// Read the /tmp/go-build2662369829/b1382/importcfg.link file to get the package information
	f, err := os.Open(filepath.Join(dir, "importcfg.link"))
	if err != nil {
		os.Exit(-1)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	line, _, err := r.ReadLine()
	if err != nil {
		os.Exit(-2)
	}
	start := strings.IndexByte(string(line), ' ')
	end := strings.IndexByte(string(line), '=')
	pkg := string(line[start+1 : end])
	return pkg
}

func MoveFile(srcPath, dstPath string) error {
	inputFile, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("couldn't open source file: %s", err)
	}
	outputFile, err := os.Create(dstPath)
	if err != nil {
		inputFile.Close()
		return fmt.Errorf("couldn't open dst file: %s", err)
	}
	defer outputFile.Close()
	_, err = io.Copy(outputFile, inputFile)
	inputFile.Close()
	if err != nil {
		return fmt.Errorf("writing to output file failed: %s", err)
	}

	// Handle the permissions
	si, err := os.Stat(srcPath)
	if err != nil {
		return fmt.Errorf("stat error: %s", err)
	}
	err = os.Chmod(dstPath, si.Mode())
	if err != nil {
		return fmt.Errorf("chmod error: %s", err)
	}

	// The copy was successful, so now delete the original file
	err = os.Remove(srcPath)
	if err != nil {
		return fmt.Errorf("failed removing original file: %s", err)
	}
	return nil
}
