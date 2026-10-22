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
	"go/ast"
	"go/token"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/multichecker"
	"golang.org/x/tools/go/analysis/passes/modernize"
)

func main() {
	// Preserve the checks enabled by gopls v0.18.1 while using the
	// repository's x/tools version for current Go language support.
	analyzers := []*analysis.Analyzer{
		modernize.AppendClippedAnalyzer,
		modernize.BLoopAnalyzer,
		modernize.AnyAnalyzer,
		modernize.FmtAppendfAnalyzer,
		modernize.MapsLoopAnalyzer,
		modernize.MinMaxAnalyzer,
		modernize.OmitZeroAnalyzer,
		modernize.RangeIntAnalyzer,
		modernize.SlicesContainsAnalyzer,
		modernize.SlicesDeleteAnalyzer,
		modernize.StringsSeqAnalyzer,
		modernize.SlicesSortAnalyzer,
		modernize.TestingContextAnalyzer,
	}
	for _, analyzer := range analyzers {
		run := analyzer.Run
		analyzer.Run = func(pass *analysis.Pass) (any, error) {
			// The previous modernize command ignored generated files.
			generated := make(map[*token.File]bool)
			for _, file := range pass.Files {
				if ast.IsGenerated(file) {
					generated[pass.Fset.File(file.FileStart)] = true
				}
			}
			report := pass.Report
			pass.Report = func(diag analysis.Diagnostic) {
				if !generated[pass.Fset.File(diag.Pos)] {
					report(diag)
				}
			}
			return run(pass)
		}
	}
	multichecker.Main(analyzers...)
}
