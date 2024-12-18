// Copyright 2021 TiKV Project Authors.
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

package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/statistics/utils"
)

type MockPackHotRegionInfo struct {
	isLeader         bool
	historyHotReads  []HistoryHotRegion
	historyHotWrites []HistoryHotRegion
	reservedDays     uint64
	pullInterval     time.Duration
}

// GetHistoryHotRegions get hot region info in HistoryHotRegion form.
func (m *MockPackHotRegionInfo) GetHistoryHotRegions(typ utils.RWType) ([]HistoryHotRegion, error) {
	switch typ {
	case utils.Write:
		result := make([]HistoryHotRegion, len(m.historyHotWrites))
		copy(result, m.historyHotWrites)
		return result, nil
	default: // case utils.Read:
		result := make([]HistoryHotRegion, len(m.historyHotReads))
		copy(result, m.historyHotReads)
		return result, nil
	}
}

// IsLeader return isLeader.
func (m *MockPackHotRegionInfo) IsLeader() bool {
	return m.isLeader
}

// GenHistoryHotRegions generate history hot region for test.
func (m *MockPackHotRegionInfo) GenHistoryHotRegions(num int, updateTime time.Time) {
	for i := range num {
		historyHotRegion := HistoryHotRegion{
			UpdateTime:    updateTime.UnixNano() / int64(time.Millisecond),
			RegionID:      uint64(i),
			StoreID:       uint64(i),
			PeerID:        rand.Uint64(),
			IsLeader:      i%2 == 0,
			IsLearner:     i%2 == 0,
			HotRegionType: HotRegionTypes[i%2],
			HotDegree:     int64(rand.Int() % 100),
			FlowBytes:     rand.Float64() * 100,
			KeyRate:       rand.Float64() * 100,
			QueryRate:     rand.Float64() * 100,
			StartKey:      fmt.Sprintf("%20d", i),
			EndKey:        fmt.Sprintf("%20d", i),
		}
		if i%2 == 1 {
			m.historyHotWrites = append(m.historyHotWrites, historyHotRegion)
		} else {
			m.historyHotReads = append(m.historyHotReads, historyHotRegion)
		}
	}
}

func (m *MockPackHotRegionInfo) GetHotRegionsReservedDays() uint64 {
	return m.reservedDays
}

func (m *MockPackHotRegionInfo) SetHotRegionsReservedDays(reservedDays uint64) {
	m.reservedDays = reservedDays
}

func (m *MockPackHotRegionInfo) GetHotRegionsWriteInterval() time.Duration {
	return m.pullInterval
}

func (m *MockPackHotRegionInfo) SetHotRegionsWriteInterval(interval time.Duration) {
	m.pullInterval = interval
}

// ClearHotRegion delete all region cached.
func (m *MockPackHotRegionInfo) ClearHotRegion() {
	m.historyHotReads = make([]HistoryHotRegion, 0)
	m.historyHotWrites = make([]HistoryHotRegion, 0)
}

func TestHotRegionWrite(t *testing.T) {
	re := require.New(t)
	packHotRegionInfo := &MockPackHotRegionInfo{}
	store, clean, err := newTestHotRegionStorage(10*time.Minute, 1, packHotRegionInfo)
	re.NoError(err)
	defer clean()
	now := time.Now()
	hotRegionStorages := []HistoryHotRegion{
		{
			UpdateTime:    now.UnixNano() / int64(time.Millisecond),
			RegionID:      1,
			StoreID:       1,
			HotRegionType: utils.Read.String(),
			StartKey:      string([]byte{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xff, 0x15, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0xff, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0xfa}),
			EndKey:        string([]byte{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xff, 0x15, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0xff, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0xfa}),
		},
		{
			UpdateTime:    now.Add(10*time.Second).UnixNano() / int64(time.Millisecond),
			RegionID:      2,
			StoreID:       1,
			HotRegionType: utils.Read.String(),
			StartKey:      string([]byte{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xff, 0x15, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0xff, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0xfa}),
			EndKey:        string([]byte{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xff, 0x15, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0xff, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0xfa}),
		},
		{
			UpdateTime:    now.Add(20*time.Second).UnixNano() / int64(time.Millisecond),
			RegionID:      3,
			StoreID:       1,
			HotRegionType: utils.Read.String(),
			StartKey:      string([]byte{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xff, 0x83, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0xff, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0xfa}),
			EndKey:        string([]byte{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xff, 0x83, 0x5f, 0x69, 0x80, 0x0, 0x0, 0x0, 0x0, 0xff, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0xfa}),
		},
	}
	var copyHotRegionStorages []HistoryHotRegion
	data, _ := json.Marshal(hotRegionStorages)
	json.Unmarshal(data, &copyHotRegionStorages)
	for i, region := range hotRegionStorages {
		copyHotRegionStorages[i].StartKey = region.StartKey
		copyHotRegionStorages[i].EndKey = region.EndKey
	}
	packHotRegionInfo.historyHotReads = hotRegionStorages
	packHotRegionInfo.historyHotWrites = []HistoryHotRegion{
		{
			UpdateTime:    now.Add(30*time.Second).UnixNano() / int64(time.Millisecond),
			RegionID:      4,
			StoreID:       1,
			HotRegionType: utils.Write.String(),
		},
	}
	store.pullHotRegionInfo()
	store.flush()
	iter := store.NewIterator([]string{utils.Read.String()},
		now.UnixNano()/int64(time.Millisecond),
		now.Add(40*time.Second).UnixNano()/int64(time.Millisecond))
	index := 0
	for next, err := iter.Next(); next != nil && err == nil; next, err = iter.Next() {
		copyHotRegionStorages[index].StartKey = core.HexRegionKeyStr([]byte(copyHotRegionStorages[index].StartKey))
		copyHotRegionStorages[index].EndKey = core.HexRegionKeyStr([]byte(copyHotRegionStorages[index].EndKey))
		re.Equal(&copyHotRegionStorages[index], next)
		index++
	}
	re.NoError(err)
	re.Equal(3, index)
}

func TestHotRegionDelete(t *testing.T) {
	re := require.New(t)
	defaultRemainDay := 7
	defaultDeleteData := 30
	deleteDate := time.Now().AddDate(0, 0, 0)
	packHotRegionInfo := &MockPackHotRegionInfo{}
	store, clean, err := newTestHotRegionStorage(10*time.Minute, uint64(defaultRemainDay), packHotRegionInfo)
	re.NoError(err)
	defer clean()
	historyHotRegions := make([]HistoryHotRegion, 0)
	for range defaultDeleteData {
		historyHotRegion := HistoryHotRegion{
			UpdateTime:    deleteDate.UnixNano() / int64(time.Millisecond),
			RegionID:      1,
			HotRegionType: utils.Read.String(),
		}
		historyHotRegions = append(historyHotRegions, historyHotRegion)
		deleteDate = deleteDate.AddDate(0, 0, -1)
	}
	packHotRegionInfo.historyHotReads = historyHotRegions
	store.pullHotRegionInfo()
	store.flush()
	store.delete(defaultRemainDay)
	iter := store.NewIterator(HotRegionTypes,
		deleteDate.UnixNano()/int64(time.Millisecond),
		time.Now().UnixNano()/int64(time.Millisecond))
	num := 0
	for next, err := iter.Next(); next != nil && err == nil; next, err = iter.Next() {
		num++
		re.Equal(&historyHotRegions[defaultRemainDay-num], next)
	}
}

func BenchmarkInsert(b *testing.B) {
	packHotRegionInfo := &MockPackHotRegionInfo{}
	regionStorage, clear, err := newTestHotRegionStorage(10*time.Hour, 7, packHotRegionInfo)
	defer clear()
	if err != nil {
		b.Fatal(err)
	}
	packHotRegionInfo.GenHistoryHotRegions(1000, time.Now())
	b.ResetTimer()
	regionStorage.pullHotRegionInfo()
	regionStorage.flush()
	b.StopTimer()
}

func BenchmarkInsertAfterManyDays(b *testing.B) {
	defaultInsertDay := 30
	packHotRegionInfo := &MockPackHotRegionInfo{}
	regionStorage, clear, err := newTestHotRegionStorage(10*time.Hour, uint64(defaultInsertDay), packHotRegionInfo)
	defer clear()
	if err != nil {
		b.Fatal(err)
	}
	nextTime := newTestHotRegions(regionStorage, packHotRegionInfo, 144*defaultInsertDay, 1000, time.Now())
	packHotRegionInfo.GenHistoryHotRegions(1000, nextTime)
	b.ResetTimer()
	regionStorage.pullHotRegionInfo()
	regionStorage.flush()
	b.StopTimer()
}

func BenchmarkDelete(b *testing.B) {
	defaultInsertDay := 7
	defaultRemainDay := 7
	packHotRegionInfo := &MockPackHotRegionInfo{}
	regionStorage, clear, err := newTestHotRegionStorage(10*time.Hour, uint64(defaultRemainDay), packHotRegionInfo)
	defer clear()
	if err != nil {
		b.Fatal(err)
	}
	deleteTime := time.Now().AddDate(0, 0, -14)
	newTestHotRegions(regionStorage, packHotRegionInfo, 144*defaultInsertDay, 1000, deleteTime)
	b.ResetTimer()
	regionStorage.delete(defaultRemainDay)
	b.StopTimer()
}

func BenchmarkRead(b *testing.B) {
	packHotRegionInfo := &MockPackHotRegionInfo{}
	regionStorage, clear, err := newTestHotRegionStorage(10*time.Hour, 7, packHotRegionInfo)
	if err != nil {
		b.Fatal(err)
	}
	defer clear()
	endTime := time.Now()
	startTime := endTime
	endTime = newTestHotRegions(regionStorage, packHotRegionInfo, 144*7, 1000, endTime)
	b.ResetTimer()
	iter := regionStorage.NewIterator(HotRegionTypes, startTime.UnixNano()/int64(time.Millisecond),
		endTime.AddDate(0, 1, 0).UnixNano()/int64(time.Millisecond))
	next, err := iter.Next()
	for next != nil && err == nil {
		next, err = iter.Next()
	}
	if err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
}

func newTestHotRegions(storage *HotRegionStorage, mock *MockPackHotRegionInfo, cycleTimes, num int, updateTime time.Time) time.Time {
	for range cycleTimes {
		mock.GenHistoryHotRegions(num, updateTime)
		storage.pullHotRegionInfo()
		storage.flush()
		updateTime = updateTime.Add(10 * time.Minute)
		mock.ClearHotRegion()
	}
	return updateTime
}

func newTestHotRegionStorage(pullInterval time.Duration,
	reservedDays uint64,
	packHotRegionInfo *MockPackHotRegionInfo) (
	hotRegionStorage *HotRegionStorage,
	clear func(), err error) {
	writePath := strings.Join([]string{".", "tmp"}, string(filepath.Separator))
	ctx := context.Background()
	packHotRegionInfo.pullInterval = pullInterval
	packHotRegionInfo.reservedDays = reservedDays
	// delete data in between today and tomorrow
	hotRegionStorage, err = NewHotRegionsStorage(ctx,
		writePath, nil, packHotRegionInfo)
	if err != nil {
		return nil, nil, err
	}
	clear = func() {
		hotRegionStorage.Close()
		PrintDirSize(writePath)
		os.RemoveAll(writePath)
	}
	return
}

// Print dir size
func PrintDirSize(path string) {
	size, err := DirSizeB(path)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("file size %d\n", size)
}

// DirSizeB get file size by path(B)
func DirSizeB(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}
