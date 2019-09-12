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

// Interpreter is the interface for all analysis to parse log
type Interpreter interface {
	CompileRegex(operator string) *regexp.Regexp
	parseLine(content string, r *regexp.Regexp) []uint64
	ParseLog(filename string, r *regexp.Regexp)
}

// CompileRegex is to provide regexp for transfer counter.
func (TransferCounter) CompileRegex(operator string) *regexp.Regexp {
	var r *regexp.Regexp
	var err error

	for _, supportOperator := range supportOperators {
		if operator == supportOperator {
			r, err = regexp.Compile(".*?operator finish.*?region-id=([0-9]*).*?" + operator + ".*?store ([0-9]*) to ([0-9]*).*?")
		}
	}

	if err != nil {
		log.Fatal("Error: ", err)
	}
	if r == nil {
		log.Fatal("Error operator: ", operator, ". Support operators: ", supportOperators)
	}
	return r
}

func (TransferCounter) parseLine(content string, r *regexp.Regexp) []uint64 {
	results := make([]uint64, 0, 4)
	subStrings := r.FindStringSubmatch(content)
	if len(subStrings) == 0 {
		return results
	} else if len(subStrings) == 4 {
		for i := 1; i < 4; i++ {
			num, err := strconv.ParseInt(subStrings[i], 10, 64)
			if err != nil {
				log.Fatal("Error: ", err)
			}
			results = append(results, uint64(num))
		}
	} else {
		log.Fatal("Parse Log Error, with ", content)
	}
	return results
}

func forEachLine(filename string, solve func(string)) {
	// Open file
	fi, err := os.Open(filename)
	if err != nil {
		log.Fatal("Error: ", err)
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
			log.Fatal("Error: ", err)
			return
		}
		solve(string(content))
	}
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
	if layout != "2006/01/02 15:04:05" {
		log.Fatal("Unsupported time layout")
	}
	r, err := regexp.Compile(".*?([0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}).*")
	if err != nil {
		log.Fatal("Error: ", err)
	}
	return func(content string) (time.Time, error) {
		result := r.FindStringSubmatch(content)
		if len(result) == 2 {
			current, err := time.Parse(layout, result[1])
			if err != nil {
				log.Fatal("Error: ", err)
			}
			return current, nil
		} else if len(result) == 0 {
			return time.Now(), errors.New("empty")
		} else {
			return time.Now(), errors.New("There is no valid time in log with " + content)
		}
	}
}

// ParseLog is to parse log for transfer counter.
func (c *TransferCounter) ParseLog(filename, start, end, layout string, r *regexp.Regexp) {
	afterStart := isExpectTime(start, layout, false)
	beforeEnd := isExpectTime(end, layout, true)
	getCurrent := currentTime(layout)
	forEachLine(filename, func(content string) {
		// Get current line time
		current, err := getCurrent(content)
		if err != nil {
			if err.Error() == "empty" {
				return
			}
			log.Fatal("Error: ", err)
		}
		// if current line time between start and end
		if afterStart(current) && beforeEnd(current) {
			results := c.parseLine(content, r)
			if len(results) == 3 {
				regionID, sourceID, targetID := results[0], results[1], results[2]
				GetTransferCounter().AddTarget(regionID, targetID)
				GetTransferCounter().AddSource(regionID, sourceID)
			}
		}
	})
}
