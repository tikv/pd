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

package command

import "fmt"

const (
	// B = 1 byte
	B ByteSize = 1
	// KB = 1024 * 1 byte = 1 kilobyte
	KB = B << 10
	// MB = 1024 * 1 kilobyte = 1 megabyte
	MB = KB << 10
	// GB = 1024 * 1 megabyte = 1 gigabyte
	GB = MB << 10
	// TB = 1024 * 1 gigabyte = 1 terabyte
	TB = GB << 10
	// PB = 1024 * 1 terabyte = 1 petabyte
	PB = TB << 10
	// EB = 1024 * 1 petabyte = 1 exabyte
	EB = PB << 10
)

// ByteSize used for data size type
type ByteSize uint64

func (b ByteSize) toBytes() uint64 {
	return uint64(b)
}

func (b ByteSize) toKBytes() float64 {
	v := b / KB
	r := b % KB
	return float64(v) + float64(r)/float64(KB)
}

func (b ByteSize) toMBytes() float64 {
	v := b / MB
	r := b % MB
	return float64(v) + float64(r)/float64(MB)
}

func (b ByteSize) toGBytes() float64 {
	v := b / GB
	r := b % GB
	return float64(v) + float64(r)/float64(GB)
}

func (b ByteSize) toTBytes() float64 {
	v := b / TB
	r := b % TB
	return float64(v) + float64(r)/float64(TB)
}

func (b ByteSize) toPBytes() float64 {
	v := b / PB
	r := b % PB
	return float64(v) + float64(r)/float64(PB)
}

func (b ByteSize) toEBytes() float64 {
	v := b / EB
	r := b % EB
	return float64(v) + float64(r)/float64(EB)
}

// MarshalJSON implement josn MarshalJSON, format like "GB","MB","KB"
func (b ByteSize) MarshalJSON() ([]byte, error) {
	var r string
	switch {
	case b > EB:
		r = fmt.Sprintf("%.2f EB", b.toEBytes())
	case b > PB:
		r = fmt.Sprintf("%.2f PB", b.toPBytes())
	case b > TB:
		r = fmt.Sprintf("%.2f TB", b.toTBytes())
	case b > GB:
		r = fmt.Sprintf("%.2f GB", b.toGBytes())
	case b > MB:
		r = fmt.Sprintf("%.2f MB", b.toMBytes())
	case b > KB:
		r = fmt.Sprintf("%.2f KB", b.toKBytes())
	default:
		r = fmt.Sprintf("%d B", b)
	}
	return []byte(`"` + r + `"`), nil
}

// UsageTemplate will used to generate a help information
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

Use "{{if .HasParent}}help {{.Name}} [command] {{else}}help [command]{{end}}" for more information about a command.{{end}}
`
