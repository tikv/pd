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
// See the License for the specific language governing permissions and
// limitations under the License.

package pdctl

import (
	"testing"

	"github.com/spf13/cobra"
)

func newACommand() *cobra.Command {
	a := &cobra.Command{
		Use:   "testa",
		Short: "test A command",
	}
	return a
}

func newBCommand() *cobra.Command {
	a := &cobra.Command{
		Use:   "testb",
		Short: "test b command",
	}
	return a
}
func newDEFCommand() *cobra.Command {
	a := &cobra.Command{
		Use:   "testdef",
		Short: "test def command",
	}
	return a
}

func newCCommand() *cobra.Command {
	a := &cobra.Command{
		Use:   "testc",
		Short: "test c command",
	}
	return a
}

func Test_genCompleter(t *testing.T) {
	var subcommand = []string{"testa", "testb", "testc", "testdef"}

	rootCmd := &cobra.Command{
		Use:   "roottest",
		Short: "test root cmd",
	}
	rootCmd.AddCommand(newACommand(), newBCommand(), newCCommand(), newDEFCommand())

	pc := genCompleter(rootCmd)

	for _, cmd := range subcommand {
		runarray := []rune(cmd)
		inprefixarray := true
		for _, v := range pc {
			inprefixarray = true
			if len(runarray) != len(v.GetName())-1 {
				continue
			}
			for i := 0; i < len(runarray); i++ {
				if runarray[i] != v.GetName()[i] {
					inprefixarray = false
				}
			}
			if inprefixarray == true {
				break
			}
		}

		if inprefixarray == false {
			t.Errorf("%s not in prefix array", cmd)
		}
	}

}
