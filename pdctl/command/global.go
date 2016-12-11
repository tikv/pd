package command

import (
	"fmt"

	"github.com/spf13/cobra"
)

type CommandFlags struct {
	URL string
}

func getAddressFromCmd(cmd *cobra.Command, prefix string) string {
	p, _ := cmd.Flags().GetString("pd_url")
	s := fmt.Sprintf("%s/%s", p, prefix)
	return s
}

const UsageTemplate = `Usage:{{if .Runnable}}
  {{if .HasAvailableFlags}}{{appendIfNotPresent .UseLine ""}}{{else}}{{.UseLine}}{{end}}{{end}}{{if .HasAvailableSubCommands}}
  {{if .HasParent}}{{ .Name}} [command]{{else}}[command]{{end}}{{end}}{{if gt .Aliases 0}}

Aliases:
  {{.NameAndAliases}}
{{end}}{{if .HasExample}}

Examples:
{{ .Example }}{{end}}{{ if .HasAvailableSubCommands}}

Available Commands:{{range .Commands}}{{if .IsAvailableCommand}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{ if .HasAvailableLocalFlags}}

Additional help topics:{{range .Commands}}{{if .IsHelpCommand}}
  {{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}{{ if .HasAvailableSubCommands }}

Use "{{if .HasParent}}{{.Name}} [command] help{{else}}[command] help{{end}}" for more information about a command.{{end}}
`
