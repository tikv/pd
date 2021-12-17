// Copyright 2020 TiKV Project Authors.
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
	"bytes"
	"io"
	"os"

	"github.com/spf13/cobra"
)

const (
	completionLongDesc = `
Output shell completion code for the specified shell (bash or zsh).
The shell code must be evaluated to provide interactive
completion of pd-ctl commands.  This can be done by sourcing it from
the .bash_profile.

Note for zsh users: [1] zsh completions are only supported in versions of zsh >= 5.2
`

	completionExample = `
	# Installing bash completion on macOS using homebrew
	## If running Bash 3.2 included with macOS
	    brew install bash-completion
	## or, if running Bash 4.1+
	    brew install bash-completion@2
	## If you've installed via other means, you may need add the completion to your completion directory
	    pd-ctl completion bash > $(brew --prefix)/etc/bash_completion.d/pd-ctl


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

	# Load the pd-ctl completion code for zsh[1] into the current shell
	    source <(pd-ctl completion zsh)
	# Set the pd-ctl completion code for zsh[1] to autoload on startup
	    pd-ctl completion zsh > "${fpath[1]}/_pd-ctl"
`
)

var (
	completionShells = map[string]func(out io.Writer, cmd *cobra.Command) error{
		"bash": runCompletionBash,
		"zsh":  runCompletionZsh,
	}
)

// NewCompletionCommand return a completion subcommand of root command
func NewCompletionCommand() *cobra.Command {
	shells := []string{}
	for s := range completionShells {
		shells = append(shells, s)
	}

	cmd := &cobra.Command{
		Use:                   "completion SHELL",
		DisableFlagsInUseLine: true,
		Short:                 "Output shell completion code for the specified shell (bash)",
		Long:                  completionLongDesc,
		Example:               completionExample,
		Run:                   RunCompletion,
		ValidArgs:             shells,
	}

	return cmd
}

// RunCompletion wrapped the bash and zsh completion scripts
func RunCompletion(cmd *cobra.Command, args []string) {
	if len(args) == 0 {
		cmd.Println("Shell not specified.")
		cmd.Usage()
		return
	}
	if len(args) > 1 {
		cmd.Println("Too many arguments. Expected only the shell type.")
		cmd.Usage()
		return
	}
	run, found := completionShells[args[0]]
	if !found {
		cmd.Printf("Unsupported shell type %q.\n", args[0])
		cmd.Usage()
		return
	}

	run(os.Stdout, cmd.Root())
}

func runCompletionBash(out io.Writer, cmd *cobra.Command) error {
	return cmd.GenBashCompletion(out)
}

func runCompletionZsh(out io.Writer, cmd *cobra.Command) error {
	zshHead := "#compdef pd-ctl\n"

	if _, err := out.Write([]byte(zshHead)); err != nil {
		return err
	}

	zshInitialization := `
__pd-ctl_bash_source() {
	alias shopt=':'
	alias _expand=_bash_expand
	alias _complete=_bash_comp
	emulate -L sh
	setopt kshglob noshglob braceexpand

	source "$@"
}

__pd-ctl_type() {
	# -t is not supported by zsh
	if [ "$1" == "-t" ]; then
		shift

		# fake Bash 4 to disable "complete -o nospace". Instead
		# "compopt +-o nospace" is used in the code to toggle trailing
		# spaces. We don't support that, but leave trailing spaces on
		# all the time
		if [ "$1" = "__pd-ctl_compopt" ]; then
			echo builtin
			return 0
		fi
	fi
	type "$@"
}

__pd-ctl_compgen() {
	local completions w
	completions=( $(compgen "$@") ) || return $?

	# filter by given word as prefix
	while [[ "$1" = -* && "$1" != -- ]]; do
		shift
		shift
	done
	if [[ "$1" == -- ]]; then
		shift
	fi
	for w in "${completions[@]}"; do
		if [[ "${w}" = "$1"* ]]; then
			echo "${w}"
		fi
	done
}

__pd-ctl_compopt() {
	true # don't do anything. Not supported by bashcompinit in zsh
}

__pd-ctl_ltrim_colon_completions()
{
	if [[ "$1" == *:* && "$COMP_WORDBREAKS" == *:* ]]; then
		# Remove colon-word prefix from COMPREPLY items
		local colon_word=${1%${1##*:}}
		local i=${#COMPREPLY[*]}
		while [[ $((--i)) -ge 0 ]]; do
			COMPREPLY[$i]=${COMPREPLY[$i]#"$colon_word"}
		done
	fi
}

__pd-ctl_get_comp_words_by_ref() {
	cur="${COMP_WORDS[COMP_CWORD]}"
	prev="${COMP_WORDS[${COMP_CWORD}-1]}"
	words=("${COMP_WORDS[@]}")
	cword=("${COMP_CWORD[@]}")
}

__pd-ctl_filedir() {
	local RET OLD_IFS w qw

	__pd-ctl_debug "_filedir $@ cur=$cur"
	if [[ "$1" = \~* ]]; then
		# somehow does not work. Maybe, zsh does not call this at all
		eval echo "$1"
		return 0
	fi

	OLD_IFS="$IFS"
	IFS=$'\n'
	if [ "$1" = "-d" ]; then
		shift
		RET=( $(compgen -d) )
	else
		RET=( $(compgen -f) )
	fi
	IFS="$OLD_IFS"

	IFS="," __pd-ctl_debug "RET=${RET[@]} len=${#RET[@]}"

	for w in ${RET[@]}; do
		if [[ ! "${w}" = "${cur}"* ]]; then
			continue
		fi
		if eval "[[ \"\${w}\" = *.$1 || -d \"\${w}\" ]]"; then
			qw="$(__pd-ctl_quote "${w}")"
			if [ -d "${w}" ]; then
				COMPREPLY+=("${qw}/")
			else
				COMPREPLY+=("${qw}")
			fi
		fi
	done
}

__pd-ctl_quote() {
    if [[ $1 == \'* || $1 == \"* ]]; then
        # Leave out first character
        printf %q "${1:1}"
    else
    	printf %q "$1"
    fi
}

autoload -U +X bashcompinit && bashcompinit

# use word boundary patterns for BSD or GNU sed
LWORD='[[:<:]]'
RWORD='[[:>:]]'
if sed --help 2>&1 | grep -q GNU; then
	LWORD='\<'
	RWORD='\>'
fi

__pd-ctl_convert_bash_to_zsh() {
	sed \
	-e 's/declare -F/whence -w/' \
	-e 's/_get_comp_words_by_ref "\$@"/_get_comp_words_by_ref "\$*"/' \
	-e 's/local \([a-zA-Z0-9_]*\)=/local \1; \1=/' \
	-e 's/flags+=("\(--.*\)=")/flags+=("\1"); two_word_flags+=("\1")/' \
	-e 's/must_have_one_flag+=("\(--.*\)=")/must_have_one_flag+=("\1")/' \
	-e "s/${LWORD}_filedir${RWORD}/__pd-ctl_filedir/g" \
	-e "s/${LWORD}_get_comp_words_by_ref${RWORD}/__pd-ctl_get_comp_words_by_ref/g" \
	-e "s/${LWORD}__ltrim_colon_completions${RWORD}/__pd-ctl_ltrim_colon_completions/g" \
	-e "s/${LWORD}compgen${RWORD}/__pd-ctl_compgen/g" \
	-e "s/${LWORD}compopt${RWORD}/__pd-ctl_compopt/g" \
	-e "s/${LWORD}declare${RWORD}/builtin declare/g" \
	-e "s/\\\$(type${RWORD}/\$(__pd-ctl_type/g" \
	<<'BASH_COMPLETION_EOF'
`
	if _, err := out.Write([]byte(zshInitialization)); err != nil {
		return err
	}

	buf := new(bytes.Buffer)
	if err := cmd.GenBashCompletion(buf); err != nil {
		return err
	}
	if _, err := out.Write(buf.Bytes()); err != nil {
		return err
	}

	zshTail := `
BASH_COMPLETION_EOF
}

__pd-ctl_bash_source <(__pd-ctl_convert_bash_to_zsh)
_complete pd-ctl 2>/dev/null
`
	if _, err := out.Write([]byte(zshTail)); err != nil {
		return err
	}
	return nil
}
