// Copyright 2019 TiKV Project Authors.
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

package analysis

import (
	"bufio"
	"errors"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"time"
)

var regionOperators = []string{"balance-region", "move-hot-read-region", "move-hot-write-region"}
var leaderOperators = []string{"balance-leader", "transfer-hot-read-leader", "transfer-hot-write-leader"}

// DefaultLayout is the default layout to parse log.
const DefaultLayout = "2006/01/02 15:04:05"

var supportLayouts = map[string]string{
	DefaultLayout: ".*?([0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}).*",
}

// Interpreter is the interface for all analysis to parse log
type Interpreter interface {
	CompileRegex(operator string) (*regexp.Regexp, error)
	ParseLog(filename string, r *regexp.Regexp) error
}

// CompileRegex is to provide regexp for transfer counter.
func (*TransferCounter) CompileRegex(operator string) (*regexp.Regexp, error) {
	var r *regexp.Regexp
	var err error

	for _, regionOperator := range regionOperators {
		if operator == regionOperator {
			r, err = regexp.Compile(".*?operator finish.*?region-id=([0-9]*).*?" + operator + ".*?store \\[([0-9]*)\\] to \\[([0-9]*)\\].*?")
		}
	}

	for _, leaderOperator := range leaderOperators {
		if operator == leaderOperator {
			r, err = regexp.Compile(".*?operator finish.*?region-id=([0-9]*).*?" + operator + ".*?store ([0-9]*) to ([0-9]*).*?")
		}
	}

	if r == nil {
		err = errors.New("unsupported operator. ")
	}
	return r, err
}

func parseLine(content string, r *regexp.Regexp) ([]uint64, error) {
	results := make([]uint64, 0, 4)
	subStrings := r.FindStringSubmatch(content)
	if len(subStrings) == 0 {
		return results, nil
	} else if len(subStrings) == 4 {
		for i := 1; i < 4; i++ {
			num, err := strconv.ParseInt(subStrings[i], 10, 64)
			if err != nil {
				return results, err
			}
			results = append(results, uint64(num))
		}
		return results, nil
	}
	return results, errors.New("Can't parse Log, with " + content)
}

func forEachLine(filename string, solve func(string) error) error {
	// Open file
	fi, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer func() {
		if err := fi.Close(); err != nil {
			log.Printf("error closing file: %s\n", err)
		}
	}()
	br := bufio.NewReader(fi)
	// For each
	for {
		content, _, err := br.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		err = solve(string(content))
		if err != nil {
			return err
		}
	}
	return nil
}

func isExpectTime(expect, layout string, isBeforeThanExpect bool) func(time.Time) bool {
	expectTime, err := time.Parse(layout, expect)
	if err != nil {
		return func(_ time.Time) bool {
			return true
		}
	}
	return func(current time.Time) bool {
		return current.Before(expectTime) == isBeforeThanExpect
	}
}

func currentTime(layout string) func(content string) (time.Time, error) {
	var r *regexp.Regexp
	var err error
	if pattern, ok := supportLayouts[layout]; ok {
		r, err = regexp.Compile(pattern)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("unsupported time layout")
	}
	return func(content string) (time.Time, error) {
		result := r.FindStringSubmatch(content)
		if len(result) == 2 {
			return time.Parse(layout, result[1])
		} else if len(result) == 0 {
			return time.Time{}, nil
		}
		return time.Time{}, errors.New("There is no valid time in log with " + content)
	}
}

// ParseLog is to parse log for transfer counter.
func (*TransferCounter) ParseLog(filename, start, end, layout string, r *regexp.Regexp) error {
	afterStart := isExpectTime(start, layout, false)
	beforeEnd := isExpectTime(end, layout, true)
	getCurrent := currentTime(layout)
	err := forEachLine(filename, func(content string) error {
		// Get current line time
		current, err := getCurrent(content)
		if err != nil || current.IsZero() {
			return err
		}
		// if current line time between start and end
		if afterStart(current) && beforeEnd(current) {
			results, err := parseLine(content, r)
			if err != nil {
				return err
			}
			if len(results) == 3 {
				regionID, sourceID, targetID := results[0], results[1], results[2]
				GetTransferCounter().AddTarget(regionID, targetID)
				GetTransferCounter().AddSource(regionID, sourceID)
			}
		}
		return nil
	})
	return err
}
