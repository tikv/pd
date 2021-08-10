package cluster

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/hotregionhistory"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/member"
	"github.com/tikv/pd/server/statistics"
)

var _ = Suite(&testHotRegionStorage{})

type testHotRegionStorage struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (t *testHotRegionStorage) SetUpSuite(c *C) {
	t.ctx, t.cancel = context.WithCancel(context.Background())
}

func (t *testHotRegionStorage) TestHotRegionWrite(c *C) {
	tc, _, cleanup := prepare(nil, func(tc *testCluster) {
		tc.regionStats = statistics.NewRegionStatistics(tc.GetOpts(), nil)
	}, func(co *coordinator) { co.run() }, c)
	defer cleanup()
	defer t.ctx.Done()
	defer os.RemoveAll("./tmp")
	raft := tc.RaftCluster
	member := &member.Member{}
	stats := statistics.StoreHotPeersStat{}
	statShows := []statistics.HotPeerStatShow{}
	regions, statShows, start_time, end_time :=
		newTestHotRegionHistory(raft, time.Now(), 3, 3)
	end_time = statShows[len(statShows)-2].LastUpdateTime.Unix()
	stats[1] = &statistics.HotPeersStat{
		Stats: statShows,
	}
	regionStorage, err := NewHotRegionsHistoryStorage(t.ctx,
		"./tmp", nil, raft, member, 1, 10*time.Second)
	defer regionStorage.LeveldbKV.Close()
	defer os.RemoveAll("./tmp")
	c.Assert(err, IsNil)
	regionStorage.packHotRegionInfo(stats, "read")
	regionStorage.flush()
	iter := regionStorage.NewIterator(start_time, end_time)
	index := 0
	for r, err := iter.Next(); r != nil && err == nil; r, err = iter.Next() {
		c.Assert(r.RegionID, Equals, statShows[index].RegionID)
		c.Assert(r.UpdateTime, Equals, statShows[index].LastUpdateTime.Unix())
		c.Assert(r.HotRegionType, Equals, hotregionhistory.HotRegionType_Read)
		c.Assert(r.StartKey, Equals, regions[index].GetMeta().StartKey)
		c.Assert(r.EndKey, Equals, regions[index].GetMeta().EndKey)
		index++
	}
	c.Assert(err, IsNil)
	c.Assert(index, Equals, len(statShows)-1)
}
func (t *testHotRegionStorage) TestHotRegionDelete(c *C) {
	tc, _, cleanup := prepare(nil, func(tc *testCluster) {
		tc.regionStats = statistics.NewRegionStatistics(tc.GetOpts(), nil)
	}, func(co *coordinator) { co.run() }, c)
	raft := tc.RaftCluster
	member := &member.Member{}
	regionStorage, err := NewHotRegionsHistoryStorage(t.ctx,
		"./tmp", nil, raft, member, 1, 10*time.Second)
	defer cleanup()
	defer t.ctx.Done()
	defer regionStorage.LeveldbKV.Close()
	defer os.RemoveAll("./tmp")
	stats := statistics.StoreHotPeersStat{}
	now := time.Now()
	next := time.Date(now.Year(), now.Month()-1, now.Day(), 0, 0, 0, 0, now.Location())
	_, statShows, start_time, end_time :=
		newTestHotRegionHistory(raft, next, 3, 3)
	stats[1] = &statistics.HotPeersStat{
		Stats: statShows,
	}
	statShows[2].LastUpdateTime = time.Now()
	end_time = statShows[2].LastUpdateTime.Unix()
	c.Assert(err, IsNil)
	regionStorage.packHotRegionInfo(stats, "read")
	regionStorage.flush()
	regionStorage.delete()
	iter := regionStorage.NewIterator(start_time, end_time)
	r, err := iter.Next()
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r.RegionID, Equals, statShows[2].RegionID)
	c.Assert(r.UpdateTime, Equals, statShows[2].LastUpdateTime.Unix())
	c.Assert(r.HotRegionType, Equals, hotregionhistory.HotRegionType_Read)
	r, err = iter.Next()
	c.Assert(err, IsNil)
	c.Assert(r, IsNil)
}

func BenchmarkInsert(b *testing.B) {
	ctx := context.Background()
	defer ctx.Done()
	_, opt, err := newTestScheduleConfig()
	if err != nil {
		log.Fatal(err)
	}
	tc := newTestCluster(ctx, opt)
	raft := tc.RaftCluster
	regionStorage, err := NewHotRegionsHistoryStorage(ctx,
		"./tmp", nil, raft, nil, 1, 1*time.Hour)
	defer regionStorage.LeveldbKV.Close()
	defer os.RemoveAll("./tmp")
	_, stat, _, _ := newBenchmarkHotRegoinHistory(raft, time.Now(), 1000, 3)
	b.ResetTimer()
	b.StartTimer()
	regionStorage.packHotRegionInfo(stat, "read")
	regionStorage.flush()
	b.StopTimer()
}
func BenchmarkInsertAfterMonth(b *testing.B) {
	ctx := context.Background()
	defer ctx.Done()
	_, opt, err := newTestScheduleConfig()
	if err != nil {
		log.Fatal(err)
	}
	tc := newTestCluster(ctx, opt)
	raft := tc.RaftCluster
	regionStorage, err := NewHotRegionsHistoryStorage(ctx,
		"./tmp", nil, raft, nil, 1, 1*time.Hour)
	defer regionStorage.LeveldbKV.Close()
	defer os.RemoveAll("./tmp")
	end_time := time.Now()
	//4320=(60*24*30)/10
	for i := 1; i < 4320; i++ {
		_, stat, _, _ := newBenchmarkHotRegoinHistory(raft, end_time, 1000, 3)
		regionStorage.packHotRegionInfo(stat, "read")
		regionStorage.flush()
		end_time.Add(10 * time.Minute)
	}
	_, stat, _, _ := newBenchmarkHotRegoinHistory(raft, end_time, 1000, 3)
	b.ResetTimer()
	b.StartTimer()
	regionStorage.packHotRegionInfo(stat, "read")
	regionStorage.flush()
	b.StopTimer()
	size, err := DirSizeB("./tmp")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("file size %d\n", size)
}
func BenchmarkDelete(b *testing.B) {
	ctx := context.Background()
	defer ctx.Done()
	_, opt, err := newTestScheduleConfig()
	if err != nil {
		log.Fatal(err)
	}
	tc := newTestCluster(ctx, opt)
	raft := tc.RaftCluster
	//delete data in between today and tomrrow
	regionStorage, err := NewHotRegionsHistoryStorage(ctx,
		"./tmp", nil, raft, nil, -1, 1*time.Hour)
	defer regionStorage.LeveldbKV.Close()
	defer os.RemoveAll("./tmp")
	end_time := time.Now()
	//4320=(60*24*31)/10
	for i := 1; i < 4464; i++ {
		_, stat, _, _ := newBenchmarkHotRegoinHistory(raft, end_time, 1000, 3)
		regionStorage.packHotRegionInfo(stat, "read")
		regionStorage.flush()
		end_time.Add(10 * time.Minute)
	}
	b.ResetTimer()
	b.StartTimer()
	regionStorage.delete()
	b.StopTimer()
	size, err := DirSizeB("./tmp")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("file size %d\n", size)
}
func BenchmarkDeleteAfterThreeMonth(b *testing.B) {
	ctx := context.Background()
	defer ctx.Done()
	_, opt, err := newTestScheduleConfig()
	if err != nil {
		log.Fatal(err)
	}
	tc := newTestCluster(ctx, opt)
	raft := tc.RaftCluster
	//delete data in between today and tomrrow
	regionStorage, err := NewHotRegionsHistoryStorage(ctx,
		"./tmp", nil, raft, nil, -1, 1*time.Hour)
	defer os.RemoveAll("./tmp")
	end_time := time.Now()
	//4464=(60*24*31)/10
	for i := 0; i < 4464; i++ {
		_, stat, _, _ := newBenchmarkHotRegoinHistory(raft, end_time, 1000, 3)
		regionStorage.packHotRegionInfo(stat, "read")
		regionStorage.flush()
		end_time.Add(10 * time.Minute)
	}
	//89=90-1,
	//leveldb will compaction after 90 times delete
	for i := 0; i < 89; i++ {
		regionStorage.delete()
		//144=24*60/10
		for i := 0; i < 144; i++ {
			_, stat, _, _ := newBenchmarkHotRegoinHistory(raft, end_time, 1000, 3)
			regionStorage.packHotRegionInfo(stat, "read")
			regionStorage.flush()
			end_time.Add(10 * time.Minute)
		}
	}
	b.ResetTimer()
	b.StartTimer()
	regionStorage.delete()
	b.StopTimer()
	size, err := DirSizeB("./tmp")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("file size %d\n", size)
}

func BenchmarkDeleteAfterYear(b *testing.B) {
	ctx := context.Background()
	defer ctx.Done()
	_, opt, err := newTestScheduleConfig()
	if err != nil {
		log.Fatal(err)
	}
	tc := newTestCluster(ctx, opt)
	raft := tc.RaftCluster
	//delete data in between today and tomrrow
	regionStorage, err := NewHotRegionsHistoryStorage(ctx,
		"./tmp", nil, raft, nil, -1, 1*time.Hour)
	defer os.RemoveAll("./tmp")
	end_time := time.Now()
	//4464=(60*24*31)/10
	for i := 0; i < 4464; i++ {
		_, stat, _, _ := newBenchmarkHotRegoinHistory(raft, end_time, 1000, 3)
		regionStorage.packHotRegionInfo(stat, "read")
		regionStorage.flush()
		end_time.Add(10 * time.Minute)
	}
	//334=365-31
	for i := 0; i < 334; i++ {
		regionStorage.delete()
		//144=24*60/10
		for i := 0; i < 144; i++ {
			_, stat, _, _ := newBenchmarkHotRegoinHistory(raft, end_time, 1000, 3)
			regionStorage.packHotRegionInfo(stat, "read")
			regionStorage.flush()
			end_time.Add(10 * time.Minute)
		}
	}
	b.ResetTimer()
	b.StartTimer()
	regionStorage.delete()
	b.StopTimer()
	size, err := DirSizeB("./tmp")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("file size %d\n", size)
}

func newTestHotRegionHistory(
	raft *RaftCluster,
	start time.Time,
	n, np uint64) (regions []*core.RegionInfo,
	statShows []statistics.HotPeerStatShow,
	start_time, end_time int64) {
	regions = newTestRegions(n, np)
	start_time = start.Unix()
	for _, region := range regions {
		raft.putRegion(region)
		statShow := statistics.HotPeerStatShow{
			RegionID:       region.GetMeta().Id,
			LastUpdateTime: start,
		}
		statShows = append(statShows, statShow)
		start = start.Add(10 * time.Second)
	}
	end_time = start.Unix()
	return
}

func newBenchmarkHotRegoinHistory(
	raft *RaftCluster,
	start time.Time,
	n, np uint64) (regions []*core.RegionInfo,
	stats statistics.StoreHotPeersStat,
	start_time, end_time int64) {
	stats = statistics.StoreHotPeersStat{}
	regions = newTestRegions(n, np)
	start_time = start.Unix()
	for _, region := range regions {
		raft.putRegion(region)
		peers := region.GetPeers()
		peer := peers[rand.Intn(len(peers))]
		if stats[peer.StoreId] == nil {
			stats[peer.StoreId] = &statistics.HotPeersStat{}
		}
		statShow := statistics.HotPeerStatShow{
			RegionID:       region.GetMeta().Id,
			StoreID:        peer.StoreId,
			LastUpdateTime: start,
			HotDegree:      rand.Int(),
			ByteRate:       rand.Float64(),
			KeyRate:        rand.Float64(),
			QueryRate:      rand.Float64(),
			AntiCount:      rand.Int(),
		}
		stats[peer.StoreId].Stats =
			append(stats[peer.StoreId].Stats, statShow)
	}
	end_time = start.Unix()
	return
}

//getFileSize get file size by path(B)
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

//getFileSize get file size by path(B)
func getFileSize(path string) int64 {
	if !exists(path) {
		return 0
	}
	fileInfo, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fileInfo.Size()
}

//exists Whether the path exists
func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil || os.IsExist(err)
}
