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

package regionmeta

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

type diskRow struct {
	RegionID uint64     `json:"r"`
	Node     int        `json:"n"`
	Meta     regionMeta `json:"m"`
}

type bufferedDiskRow struct {
	row     diskRow
	payload []byte
}

type diskBudget struct {
	limit   int64
	current int64
	peak    int64
	sizes   map[string]int64
}

func newDiskBudget(limit int64) *diskBudget {
	return &diskBudget{limit: limit, sizes: make(map[string]int64)}
}

func (b *diskBudget) write(writer io.Writer, path string, payload []byte) error {
	if b.current+int64(len(payload)) > b.limit {
		return fmt.Errorf("temporary JSON data exceeds %d MiB", b.limit/(1024*1024))
	}
	n, err := writer.Write(payload)
	if err != nil {
		return err
	}
	if n != len(payload) {
		return io.ErrShortWrite
	}
	b.current += int64(n)
	b.sizes[path] += int64(n)
	if b.current > b.peak {
		b.peak = b.current
	}
	return nil
}

func (b *diskBudget) remove(path string) {
	b.current -= b.sizes[path]
	delete(b.sizes, path)
	_ = os.Remove(path)
}

type sortedRows struct {
	directory   string
	budget      *diskBudget
	buffer      []bufferedDiskRow
	bufferBytes int
	chunks      []string
}

func newSortedRows(directory string, budget *diskBudget) *sortedRows {
	return &sortedRows{directory: directory, budget: budget}
}

func (s *sortedRows) add(regionID uint64, nodeIndex int, meta regionMeta) error {
	row := diskRow{RegionID: regionID, Node: nodeIndex, Meta: meta}
	payload, err := json.Marshal(row)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	s.buffer = append(s.buffer, bufferedDiskRow{row: row, payload: payload})
	s.bufferBytes += len(payload)
	if s.bufferBytes >= sortBufferBytes {
		return s.flush()
	}
	return nil
}

func (s *sortedRows) flush() (returnErr error) {
	if len(s.buffer) == 0 {
		return nil
	}
	slices.SortFunc(s.buffer, func(a, b bufferedDiskRow) int {
		return compareDiskRow(a.row, b.row)
	})
	file, err := os.CreateTemp(s.directory, "region-meta-*.jsonl")
	if err != nil {
		return err
	}
	path := file.Name()
	defer func() {
		if returnErr != nil {
			_ = file.Close()
			s.budget.remove(path)
		}
	}()
	writer := bufio.NewWriter(file)
	for _, row := range s.buffer {
		if err := s.budget.write(writer, path, row.payload); err != nil {
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	s.chunks = append(s.chunks, path)
	s.buffer = nil
	s.bufferBytes = 0
	return nil
}

func (s *sortedRows) finish() error {
	if err := s.flush(); err != nil {
		return err
	}
	for len(s.chunks) > 1 {
		original := s.chunks
		merged := make([]string, 0, (len(original)+mergeFanIn-1)/mergeFanIn)
		for start := 0; start < len(original); start += mergeFanIn {
			end := min(start+mergeFanIn, len(original))
			group := original[start:end]
			if len(group) == 1 {
				merged = append(merged, group[0])
				continue
			}
			path, err := s.merge(group)
			if err != nil {
				s.chunks = append(merged, original[start:]...)
				return err
			}
			merged = append(merged, path)
		}
		s.chunks = merged
	}
	return nil
}

func (s *sortedRows) merge(paths []string) (mergedPath string, returnErr error) {
	file, err := os.CreateTemp(s.directory, "region-meta-*.jsonl")
	if err != nil {
		return "", err
	}
	path := file.Name()
	defer func() {
		if returnErr != nil {
			_ = file.Close()
			s.budget.remove(path)
		}
	}()
	writer := bufio.NewWriter(file)
	err = iterateMergedRows(paths, func(row diskRow) error {
		payload, err := json.Marshal(row)
		if err != nil {
			return err
		}
		payload = append(payload, '\n')
		return s.budget.write(writer, path, payload)
	})
	if err != nil {
		return "", err
	}
	if err := writer.Flush(); err != nil {
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	for _, source := range paths {
		s.budget.remove(source)
	}
	return path, nil
}

func (s *sortedRows) iterate(visit func(diskRow) error) error {
	if len(s.chunks) == 0 {
		return nil
	}
	if len(s.chunks) != 1 {
		return errors.New("internal difference rows were not fully merged")
	}
	return iterateMergedRows(s.chunks, visit)
}

func (s *sortedRows) cleanup() {
	for _, path := range s.chunks {
		s.budget.remove(path)
	}
	s.chunks = nil
	s.buffer = nil
	s.bufferBytes = 0
}

func compareDiskRow(a, b diskRow) int {
	if a.RegionID != b.RegionID {
		return compareUint64(a.RegionID, b.RegionID)
	}
	return a.Node - b.Node
}

type rowSource struct {
	file   *os.File
	reader *bufio.Reader
}

type heapRow struct {
	row    diskRow
	source int
}

type rowHeap []heapRow

func (h *rowHeap) Len() int           { return len(*h) }
func (h *rowHeap) Less(i, j int) bool { return compareDiskRow((*h)[i].row, (*h)[j].row) < 0 }
func (h *rowHeap) Swap(i, j int)      { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }

// Push adds a row to the heap.
func (h *rowHeap) Push(value any) { *h = append(*h, value.(heapRow)) }

// Pop removes the final row from the heap.
func (h *rowHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

func iterateMergedRows(paths []string, visit func(diskRow) error) error {
	sources := make([]rowSource, 0, len(paths))
	for _, path := range paths {
		file, err := os.Open(filepath.Clean(path))
		if err != nil {
			for _, source := range sources {
				_ = source.file.Close()
			}
			return err
		}
		sources = append(sources, rowSource{file: file, reader: bufio.NewReader(file)})
	}
	defer func() {
		for _, source := range sources {
			_ = source.file.Close()
		}
	}()
	rows := &rowHeap{}
	heap.Init(rows)
	for i := range sources {
		row, err := readDiskRow(sources[i].reader)
		if err == nil {
			heap.Push(rows, heapRow{row: row, source: i})
		} else if !errors.Is(err, io.EOF) {
			return err
		}
	}
	for rows.Len() > 0 {
		value := heap.Pop(rows).(heapRow)
		if err := visit(value.row); err != nil {
			return err
		}
		next, err := readDiskRow(sources[value.source].reader)
		if err == nil {
			heap.Push(rows, heapRow{row: next, source: value.source})
		} else if !errors.Is(err, io.EOF) {
			return err
		}
	}
	return nil
}

func readDiskRow(reader *bufio.Reader) (diskRow, error) {
	payload, err := reader.ReadBytes('\n')
	if errors.Is(err, io.EOF) && len(payload) > 0 {
		err = nil
	}
	if err != nil {
		return diskRow{}, err
	}
	var row diskRow
	if err := json.Unmarshal(bytes.TrimSpace(payload), &row); err != nil {
		return diskRow{}, err
	}
	return row, nil
}

type scanState struct {
	Node        node
	CountBefore int
	CountAfter  int
	Scanned     int
	Pages       int
	Attempts    int
	Cursor      string
	StartedAt   string
	FinishedAt  string
}

func (s scanState) stable() bool {
	return s.CountBefore == s.Scanned && s.Scanned == s.CountAfter
}

type regionStream struct {
	state        *scanState
	client       *httpClient
	batchSize    int
	buffer       []regionRecord
	lastEndKey   *string
	terminalPage bool
}

func (s *regionStream) needsPage() bool {
	return len(s.buffer) == 0 && !s.terminalPage
}

func (s *regionStream) acceptPage(payload wireRegions, pageCounter *int, progress io.Writer) error {
	if payload.Count == nil || *payload.Count != len(payload.Regions) || len(payload.Regions) > s.batchSize {
		return errors.New("/regions/key response count or batch limit is invalid")
	}
	s.state.Pages++
	*pageCounter++
	if *pageCounter%100 == 0 {
		_, _ = fmt.Fprintf(progress, "scanned %d batches\n", *pageCounter)
	}
	if len(payload.Regions) == 0 {
		s.terminalPage = true
		return nil
	}
	for _, raw := range payload.Regions {
		record, err := normalizeRegion(raw)
		if err != nil {
			return err
		}
		endKey := record.Meta.EndKey
		if s.lastEndKey != nil && *s.lastEndKey == "" {
			return fmt.Errorf("%s: region found after the unbounded key", s.state.Node.Name)
		}
		if s.lastEndKey != nil && endKey != "" && compareHexKeys(endKey, *s.lastEndKey) <= 0 {
			return fmt.Errorf("%s: region scan key did not advance", s.state.Node.Name)
		}
		s.lastEndKey = &endKey
		s.buffer = append(s.buffer, record)
	}
	if *s.lastEndKey == "" {
		s.terminalPage = true
	} else {
		if compareHexKeys(*s.lastEndKey, s.state.Cursor) <= 0 {
			return fmt.Errorf("%s: region scan cursor did not advance", s.state.Node.Name)
		}
		s.state.Cursor = *s.lastEndKey
	}
	return nil
}

func (s *regionStream) next() (*regionRecord, error) {
	if s.needsPage() {
		return nil, errors.New("internal region page was not prefetched")
	}
	if len(s.buffer) == 0 {
		if s.state.FinishedAt == "" {
			s.state.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		}
		return nil, nil
	}
	record := s.buffer[0]
	s.buffer = s.buffer[1:]
	s.state.Scanned++
	if len(s.buffer) == 0 && s.terminalPage {
		s.state.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	return &record, nil
}

func prefetchPages(
	ctx context.Context,
	streams []*regionStream,
	requester *batchRequester,
	pageCounter *int,
	progress io.Writer,
) error {
	pending := make([]*regionStream, 0, len(streams))
	pages := make([]*wireRegions, 0, len(streams))
	calls := make([]requestCall, 0, len(streams))
	for _, stream := range streams {
		if !stream.needsPage() {
			continue
		}
		page := &wireRegions{}
		pending = append(pending, stream)
		pages = append(pages, page)
		query := url.Values{
			"format":  {"hex"},
			"key":     {stream.state.Cursor},
			"end_key": {""},
			"limit":   {strconv.Itoa(stream.batchSize)},
		}
		calls = append(calls, requestCall{
			client: stream.client, path: "/pd/api/v1/regions/key", query: query,
			local: true, destination: page,
		})
	}
	if err := requester.getJSON(ctx, calls); err != nil {
		return err
	}
	for i, stream := range pending {
		if err := stream.acceptPage(*pages[i], pageCounter, progress); err != nil {
			return err
		}
	}
	return nil
}

func scanStreams(
	ctx context.Context,
	states []*scanState,
	clients []*httpClient,
	batchSize int,
	differences *sortedRows,
	requester *batchRequester,
	progress io.Writer,
) error {
	pageCounter := 0
	streams := make([]*regionStream, 0, len(states))
	for i, state := range states {
		state.StartedAt = time.Now().UTC().Format(time.RFC3339Nano)
		streams = append(streams, &regionStream{
			state: state, client: clients[i], batchSize: batchSize,
		})
	}
	for {
		if err := prefetchPages(ctx, streams, requester, &pageCounter, progress); err != nil {
			return err
		}
		records := make([]*regionRecord, len(streams))
		allNil := true
		for i, stream := range streams {
			record, err := stream.next()
			if err != nil {
				return err
			}
			records[i] = record
			allNil = allNil && record == nil
		}
		if allNil {
			return nil
		}
		if !recordsEqual(records) {
			for i, record := range records {
				if record != nil {
					if err := differences.add(record.ID, i, record.Meta); err != nil {
						return err
					}
				}
			}
		}
		boundaries := recordBoundaries(records)
		for !allBoundariesEqual(boundaries) {
			boundary := minimumBoundary(boundaries)
			indexes := make([]int, 0, len(boundaries))
			selected := make([]*regionStream, 0, len(boundaries))
			for i, current := range boundaries {
				if current == boundary {
					indexes = append(indexes, i)
					selected = append(selected, streams[i])
				}
			}
			if err := prefetchPages(ctx, selected, requester, &pageCounter, progress); err != nil {
				return err
			}
			for _, i := range indexes {
				record, err := streams[i].next()
				if err != nil {
					return err
				}
				if record == nil {
					boundaries[i] = ""
					continue
				}
				boundaries[i] = record.Meta.EndKey
				if err := differences.add(record.ID, i, record.Meta); err != nil {
					return err
				}
			}
		}
	}
}

func recordsEqual(records []*regionRecord) bool {
	for i := 1; i < len(records); i++ {
		if (records[0] == nil) != (records[i] == nil) ||
			(records[0] != nil && !records[0].equal(*records[i])) {
			return false
		}
	}
	return true
}

func recordBoundaries(records []*regionRecord) []string {
	result := make([]string, len(records))
	for i, record := range records {
		if record != nil {
			result[i] = record.Meta.EndKey
		}
	}
	return result
}

func allBoundariesEqual(boundaries []string) bool {
	for i := 1; i < len(boundaries); i++ {
		if boundaries[i] != boundaries[0] {
			return false
		}
	}
	return true
}

func minimumBoundary(boundaries []string) string {
	minimum := boundaries[0]
	for _, value := range boundaries[1:] {
		if compareBoundary(value, minimum) < 0 {
			minimum = value
		}
	}
	return minimum
}

func compareBoundary(a, b string) int {
	if a == "" {
		if b == "" {
			return 0
		}
		return 1
	}
	if b == "" {
		return -1
	}
	return compareHexKeys(a, b)
}

func compareHexKeys(a, b string) int {
	aBytes, _ := hex.DecodeString(a)
	bBytes, _ := hex.DecodeString(b)
	return bytes.Compare(aBytes, bBytes)
}

type countResponse struct {
	Count *int `json:"count"`
}

func collectRegions(
	ctx context.Context,
	nodes []node,
	clients []*httpClient,
	batchSize int,
	scanRetries int,
	directory string,
	budget *diskBudget,
	requester *batchRequester,
	progress io.Writer,
) ([]*scanState, *sortedRows, error) {
	var lastStates []*scanState
	for attempt := 1; attempt <= scanRetries+1; attempt++ {
		states := make([]*scanState, 0, len(nodes))
		for _, current := range nodes {
			states = append(states, &scanState{Node: current, Attempts: attempt})
		}
		before := make([]*countResponse, len(nodes))
		calls := make([]requestCall, 0, len(nodes))
		for i := range nodes {
			before[i] = &countResponse{}
			calls = append(calls, requestCall{
				client: clients[i], path: "/pd/api/v1/regions/count", local: true, destination: before[i],
			})
		}
		if err := requester.getJSON(ctx, calls); err != nil {
			return nil, nil, err
		}
		for i := range states {
			if before[i].Count == nil || *before[i].Count < 0 {
				return nil, nil, errors.New("/regions/count response does not contain a valid count")
			}
			states[i].CountBefore = *before[i].Count
		}
		differences := newSortedRows(directory, budget)
		if err := scanStreams(ctx, states, clients, batchSize, differences, requester, progress); err != nil {
			differences.cleanup()
			return nil, nil, err
		}
		after := make([]*countResponse, len(nodes))
		calls = calls[:0]
		for i := range nodes {
			after[i] = &countResponse{}
			calls = append(calls, requestCall{
				client: clients[i], path: "/pd/api/v1/regions/count", local: true, destination: after[i],
			})
		}
		if err := requester.getJSON(ctx, calls); err != nil {
			differences.cleanup()
			return nil, nil, err
		}
		stable := true
		for i := range states {
			if after[i].Count == nil || *after[i].Count < 0 {
				differences.cleanup()
				return nil, nil, errors.New("/regions/count response does not contain a valid count")
			}
			states[i].CountAfter = *after[i].Count
			stable = stable && states[i].stable()
		}
		if stable {
			if err := differences.finish(); err != nil {
				differences.cleanup()
				return nil, nil, err
			}
			return states, differences, nil
		}
		differences.cleanup()
		lastStates = states
		if attempt <= scanRetries {
			_, _ = fmt.Fprintln(progress, "region count changed during scan; retrying every PD member")
		}
	}
	details := make([]string, 0, len(lastStates))
	for _, state := range lastStates {
		if !state.stable() {
			details = append(details, fmt.Sprintf("%s: before=%d, scanned=%d, after=%d",
				state.Node.Name, state.CountBefore, state.Scanned, state.CountAfter))
		}
	}
	return nil, nil, fmt.Errorf("unstable region set after %d cluster-wide scan attempt(s) (%s)",
		scanRetries+1, strings.Join(details, "; "))
}
