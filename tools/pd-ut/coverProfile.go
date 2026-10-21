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

package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"golang.org/x/tools/cover"
)

func collectCoverProfileFile() error {
	// Combine all the cover file of single test function into a whole.
	files, err := os.ReadDir(coverFileTempDir)
	if err != nil {
		return fmt.Errorf("read temporary cover files: %w", err)
	}

	w, err := os.Create(coverProfile)
	if err != nil {
		return fmt.Errorf("create cover file: %w", err)
	}
	//nolint: errcheck
	defer w.Close()
	if _, err := w.WriteString("mode: atomic\n"); err != nil {
		return fmt.Errorf("write cover profile header: %w", err)
	}

	result := make(map[string]*cover.Profile)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if err := collectOneCoverProfileFile(result, file); err != nil {
			return err
		}
	}

	w1 := bufio.NewWriter(w)
	for _, prof := range result {
		for _, block := range prof.Blocks {
			if _, err := fmt.Fprintf(w1, "%s:%d.%d,%d.%d %d %d\n",
				prof.FileName,
				block.StartLine,
				block.StartCol,
				block.EndLine,
				block.EndCol,
				block.NumStmt,
				block.Count,
			); err != nil {
				return fmt.Errorf("write cover profile: %w", err)
			}
		}
	}
	if err := w1.Flush(); err != nil {
		return fmt.Errorf("flush data to cover profile file: %w", err)
	}
	return nil
}

func collectOneCoverProfileFile(result map[string]*cover.Profile, file os.DirEntry) error {
	f, err := os.Open(filepath.Join(coverFileTempDir, file.Name()))
	if err != nil {
		return fmt.Errorf("open temporary cover file %s: %w", file.Name(), err)
	}
	//nolint: errcheck
	defer f.Close()

	profs, err := cover.ParseProfilesFromReader(f)
	if err != nil {
		return fmt.Errorf("parse temporary cover file %s: %w", file.Name(), err)
	}
	mergeProfile(result, profs)
	return nil
}

func mergeProfile(m map[string]*cover.Profile, profs []*cover.Profile) {
	for _, prof := range profs {
		sort.Sort(blocksByStart(prof.Blocks))
		old, ok := m[prof.FileName]
		if !ok {
			m[prof.FileName] = prof
			continue
		}

		// Merge samples from the same location.
		// The data has already been sorted.
		tmp := old.Blocks[:0]
		var i, j int
		for i < len(old.Blocks) && j < len(prof.Blocks) {
			v1 := old.Blocks[i]
			v2 := prof.Blocks[j]

			switch compareProfileBlock(v1, v2) {
			case -1:
				tmp = appendWithReduce(tmp, v1)
				i++
			case 1:
				tmp = appendWithReduce(tmp, v2)
				j++
			default:
				tmp = appendWithReduce(tmp, v1)
				tmp = appendWithReduce(tmp, v2)
				i++
				j++
			}
		}
		for ; i < len(old.Blocks); i++ {
			tmp = appendWithReduce(tmp, old.Blocks[i])
		}
		for ; j < len(prof.Blocks); j++ {
			tmp = appendWithReduce(tmp, prof.Blocks[j])
		}

		m[prof.FileName] = old
	}
}

// appendWithReduce works like append(), but it merge the duplicated values.
func appendWithReduce(input []cover.ProfileBlock, b cover.ProfileBlock) []cover.ProfileBlock {
	if len(input) >= 1 {
		last := &input[len(input)-1]
		if b.StartLine == last.StartLine &&
			b.StartCol == last.StartCol &&
			b.EndLine == last.EndLine &&
			b.EndCol == last.EndCol {
			if b.NumStmt != last.NumStmt {
				panic(fmt.Errorf("inconsistent NumStmt: changed from %d to %d", last.NumStmt, b.NumStmt))
			}
			// Merge the data with the last one of the slice.
			last.Count |= b.Count
			return input
		}
	}
	return append(input, b)
}

type blocksByStart []cover.ProfileBlock

func compareProfileBlock(x, y cover.ProfileBlock) int {
	if x.StartLine < y.StartLine {
		return -1
	}
	if x.StartLine > y.StartLine {
		return 1
	}

	// Now x.StartLine == y.StartLine
	if x.StartCol < y.StartCol {
		return -1
	}
	if x.StartCol > y.StartCol {
		return 1
	}

	return 0
}

func (b blocksByStart) Len() int      { return len(b) }
func (b blocksByStart) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b blocksByStart) Less(i, j int) bool {
	bi, bj := b[i], b[j]
	return bi.StartLine < bj.StartLine || bi.StartLine == bj.StartLine && bi.StartCol < bj.StartCol
}
