// Copyright 2017 PingCAP, Inc.
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
	"os"

	"github.com/spf13/cobra"
)

const (
	completionLongDesc = `
Output shell completion code for the specified shell (bash).
The shell code must be evaluated to provide interactive
completion of pd-ctl commands.  This can be done by sourcing it from
the .bash_profile.
`

	completionExample = `
	# Installing bash completion on macOS using homebrew
	## If running Bash 3.2 included with macOS
	    brew install bash-completion
	## or, if running Bash 4.1+
	    brew install bash-completion@2
	## If tkctl is installed via homebrew, this should start working immediately.
	## If you've installed via other means, you may need add the completion to your completion directory
	    tkctl completion bash > $(brew --prefix)/etc/bash_completion.d/tkctl


	# Installing bash completion on Linux
	## If bash-completion is not installed on Linux, please install the 'bash-completion' package
	## via your distribution's package manager.
	## Load the pd-ctl completion code for bash into the current shell
	    source <(pd-ctl completion bash)
	## Write bash completion code to a file and source if from .bash_profile
		pd-ctl completion bash > ~/completion.bash.inc
		printf "
		# pd-ctl shell completion
		source '$HOME/completion.bash.inc'
		" >> $HOME/.bash_profile
		source $HOME/.bash_profile
`
)

// NewCompletionCommand return a completion subcommand of root command
func NewCompletionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "completion SHELL",
		DisableFlagsInUseLine: true,
		Short:                 "Output shell completion code for the specified shell (bash)",
		Long:                  completionLongDesc,
		Example:               completionExample,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 1 && args[0] != "bash" {
				cmd.Printf("Unsupported shell type %s.\n", args[0])
				cmd.Usage()
				return
			}
			if len(args) > 1 {
				cmd.Println("Too many arguments. Expected only the shell type.")
				cmd.Usage()
				return
			}
			cmd.Root().GenBashCompletion(os.Stdout)
		},
		ValidArgs: []string{"bash"},
	}

	return cmd
}
