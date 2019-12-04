// Copyright 2019 PingCAP, Inc.
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

var supportOperators = []string{"balance-region", "balance-leader", "transfer-hot-read-leader", "move-hot-read-region", "transfer-hot-write-leader", "move-hot-write-region"}

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
func (c *TransferCounter) CompileRegex(operator string) (*regexp.Regexp, error) {
	var r *regexp.Regexp
	var err error

	for _, supportOperator := range supportOperators {
		if operator == supportOperator {
			r, err = regexp.Compile(".*?operator finish.*?region-id=([0-9]*).*?" + operator + ".*?store ([0-9]*) to ([0-9]*).*?")
		}
	}

	if r == nil {
		err = errors.New("unsupported operator. ")
	}
	return r, err
}

func (c *TransferCounter) parseLine(content string, r *regexp.Regexp) ([]uint64, error) {
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
	} else {
		return results, errors.New("Can't parse Log, with " + content)
	}
}

func forEachLine(filename string, solve func(string) error) error {
	// Open file
	fi, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer fi.Close()
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
		return func(current time.Time) bool {
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
		log.Fatal("Unsupported time layout.")
	}
	return func(content string) (time.Time, error) {
		result := r.FindStringSubmatch(content)
		if len(result) == 2 {
			return time.Parse(layout, result[1])
		} else if len(result) == 0 {
			return time.Time{}, nil
		} else {
			return time.Time{}, errors.New("There is no valid time in log with " + content)
		}
	}
}

// ParseLog is to parse log for transfer counter.
func (c *TransferCounter) ParseLog(filename, start, end, layout string, r *regexp.Regexp) error {
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
			results, err := c.parseLine(content, r)
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
