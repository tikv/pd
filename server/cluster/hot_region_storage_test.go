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
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server/core"
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
	regionStorage, clear, err := newTestHotRegionStorage(10*time.Hour, -1)
	defer clear()
	c.Assert(err, IsNil)
	raft := regionStorage.cluster
	stats := statistics.StoreHotPeersStat{}
	statShows := []statistics.HotPeerStatShow{}
	regions, statShows, start_time, end_time :=
		newTestHotRegionHistory(raft, time.Now(), 3, 3)
	end_time = statShows[len(statShows)-2].LastUpdateTime.Unix()
	stats[1] = &statistics.HotPeersStat{
		Stats: statShows,
	}
	c.Assert(err, IsNil)
	regionStorage.packHotRegionInfo(stats, "read")
	regionStorage.flush()
	iter := regionStorage.NewIterator(hotRegionTypes, start_time, end_time)
	index := 0
	for r, err := iter.Next(); r != nil && err == nil; r, err = iter.Next() {
		c.Assert(r.RegionID, Equals, statShows[index].RegionID)
		c.Assert(r.UpdateTime, Equals, statShows[index].LastUpdateTime.Unix())
		c.Assert(r.HotRegionType, Equals, "read")
		c.Assert(r.StartKey, Equals, regions[index].GetMeta().StartKey)
		c.Assert(r.EndKey, Equals, regions[index].GetMeta().EndKey)
		index++
	}
	c.Assert(err, IsNil)
	c.Assert(index, Equals, len(statShows)-1)
}

func (t *testHotRegionStorage) TestHotRegionDelete(c *C) {
	regionStorage, clear, err := newTestHotRegionStorage(10*time.Hour, -1)
	defer clear()
	c.Assert(err, IsNil)
	raft := regionStorage.cluster
	stats := statistics.StoreHotPeersStat{}
	now := time.Now()
	next := now.AddDate(0, 0, -1)
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
	iter := regionStorage.NewIterator(hotRegionTypes, start_time, end_time)
	r, err := iter.Next()
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r.RegionID, Equals, statShows[2].RegionID)
	c.Assert(r.UpdateTime, Equals, statShows[2].LastUpdateTime.Unix())
	c.Assert(r.HotRegionType, Equals, "read")
	r, err = iter.Next()
	c.Assert(err, IsNil)
	c.Assert(r, IsNil)
}

func BenchmarkInsert(b *testing.B) {
	regionStorage, clear, err := newTestHotRegionStorage(10*time.Hour, -1)
	defer clear()
	if err != nil {
		b.Fatal(err)
	}
	raft := regionStorage.cluster
	regions := newTestHotRegions(1000, 3)
	for _, region := range regions {
		raft.putRegion(region)
	}
	stat := newBenchmarkHotRegoinHistory(raft, time.Now(), regions)
	b.ResetTimer()
	err = regionStorage.packHotRegionInfo(stat, "read")
	if err != nil {
		log.Fatal(err)
	}
	regionStorage.flush()
	b.StopTimer()
}

func BenchmarkInsertAfterMonth(b *testing.B) {
	regionStorage, clear, err := newTestHotRegionStorage(10*time.Hour, -1)
	if err != nil {
		b.Fatal(err)
	}
	defer clear()
	raft := regionStorage.cluster
	endTime := time.Now()
	regions := newTestHotRegions(1000, 3)
	for _, region := range regions {
		raft.putRegion(region)
	}
	//4320=(60*24*30)/10
	writeIntoDB(regionStorage, regions, 4464, endTime)
	stat := newBenchmarkHotRegoinHistory(raft, endTime, regions)
	b.ResetTimer()
	err = regionStorage.packHotRegionInfo(stat, "read")
	if err != nil {
		log.Fatal(err)
	}
	regionStorage.flush()
}

func BenchmarkDelete(b *testing.B) {
	regionStorage, clear, err := newTestHotRegionStorage(10*time.Hour, -1)
	if err != nil {
		b.Fatal(err)
	}
	defer clear()
	raft := regionStorage.cluster
	endTime := time.Now()
	regions := newTestHotRegions(1000, 3)
	for _, region := range regions {
		raft.putRegion(region)
	}
	//4464=(60*24*31)/10
	writeIntoDB(regionStorage, regions, 4464, endTime)
	b.ResetTimer()
	regionStorage.delete()
}

func BenchmarkRead(b *testing.B) {
	//delete data in between today and tomrrow
	regionStorage, clear, err := newTestHotRegionStorage(10*time.Hour, 30)
	if err != nil {
		b.Fatal(err)
	}
	defer clear()
	raft := regionStorage.cluster
	endTime := time.Now()
	startTime := endTime
	regions := newTestHotRegions(1000, 3)
	for _, region := range regions {
		raft.putRegion(region)
	}
	//4320=(60*24*31)/10
	endTime = writeIntoDB(regionStorage, regions, 4320, endTime)
	// f, _ := os.OpenFile("/root/cpu.pprof", os.O_CREATE|os.O_RDWR, 0644)
	// defer f.Close()
	// pprof.StartCPUProfile(f)
	// defer pprof.StopCPUProfile()
	b.ResetTimer()
	iter := regionStorage.NewIterator(hotRegionTypes, startTime.Unix(), endTime.AddDate(0, 1, 0).Unix())
	for next, err := iter.Next(); next != nil && err == nil; next, err = iter.Next() {

	}
	b.StopTimer()
}

func BenchmarkCompaction(b *testing.B) {
	//delete data in between today and tomrrow
	regionStorage, clear, err := newTestHotRegionStorage(10*time.Hour, 30)
	if err != nil {
		b.Fatal(err)
	}
	defer clear()
	raft := regionStorage.cluster
	endTime := time.Now()
	regions := newTestHotRegions(1000, 3)
	for _, region := range regions {
		raft.putRegion(region)
	}
	//leveldb will compaction after 30 times delete
	for i := 0; i < defaultCompactionTime-1; i++ {
		//144=24*60/10
		endTime = writeIntoDB(regionStorage, regions, 144, endTime)
		regionStorage.delete()
		regionStorage.remianedDays--
	}
	b.ResetTimer()
	regionStorage.delete()
	b.StopTimer()
}

func BenchmarkTwoTimesCompaction(b *testing.B) {
	//delete data in between today and tomrrow
	regionStorage, clear, err := newTestHotRegionStorage(10*time.Hour, 30)
	if err != nil {
		b.Fatal(err)
	}
	defer clear()
	raft := regionStorage.cluster
	endTime := time.Now()
	//4464=(60*24*31)/10
	regions := newTestHotRegions(1000, 3)
	for _, region := range regions {
		raft.putRegion(region)
	}
	//leveldb will compaction after 30 times delete
	for i := 0; i < 2*defaultCompactionTime-1; i++ {
		//144=24*60/10
		endTime = writeIntoDB(regionStorage, regions, 144, endTime)
		regionStorage.delete()
		regionStorage.remianedDays--
	}
	b.ResetTimer()
	regionStorage.delete()
	b.StopTimer()
}

func BenchmarkDeleteAfterYear(b *testing.B) {
	regionStorage, clear, err := newTestHotRegionStorage(10*time.Hour, -1)
	if err != nil {
		b.Fatal(err)
	}
	defer clear()
	raft := regionStorage.cluster
	regions := newTestHotRegions(1000, 3)
	for _, region := range regions {
		raft.putRegion(region)
	}
	endTime := time.Now()
	//4464=(60*24*31)/10
	endTime = writeIntoDB(regionStorage, regions, 4464, endTime)
	//334=365-31
	for i := 0; i < 334; i++ {
		regionStorage.delete()
		//144=24*60/10
		endTime = writeIntoDB(regionStorage, regions, 144, endTime)
	}
	b.ResetTimer()
	regionStorage.delete()
	b.StopTimer()
}

func newTestHotRegionStorage(pullInterval time.Duration, remianedDays int64) (
	hotRegionStorage *HotRegionStorage,
	clear func(), err error) {
	writePath := "./tmp"
	ctx := context.Background()
	_, opt, err := newTestScheduleConfig()
	if err != nil {
		return nil, nil, err
	}
	raft := newTestCluster(ctx, opt).RaftCluster
	//delete data in between today and tomrrow
	hotRegionStorage, err = NewHotRegionsHistoryStorage(ctx,
		writePath, nil, raft, nil, remianedDays, pullInterval)
	if err != nil {
		return nil, nil, err
	}
	clear = func() {
		hotRegionStorage.Close()
		PrintDirSize(writePath)
		// os.RemoveAll(writePath)
	}
	return
}
func writeIntoDB(regionStorage *HotRegionStorage,
	regions []*core.RegionInfo, times int,
	endTime time.Time) time.Time {
	raft := regionStorage.cluster
	for i := 0; i < times; i++ {
		if i%1000 == 0 {
			fmt.Println(i)
		}
		stats := newBenchmarkHotRegoinHistory(raft, endTime, regions)
		err := regionStorage.packHotRegionInfo(stats, hotRegionTypes[i%len(hotRegionTypes)])
		if err != nil {
			log.Fatal(err)
		}
		regionStorage.flush()
		endTime = endTime.Add(10 * time.Minute)
	}
	return endTime
}

func newTestHotRegionHistory(
	raft *RaftCluster,
	start time.Time,
	n, np uint64) (regions []*core.RegionInfo,
	statShows []statistics.HotPeerStatShow,
	start_time, end_time int64) {
	regions = newTestHotRegions(n, np)
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
	regions []*core.RegionInfo) (
	stats statistics.StoreHotPeersStat) {
	stats = statistics.StoreHotPeersStat{}
	for _, region := range regions {
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
			ByteRate:       rand.Float64() * 100,
			KeyRate:        rand.Float64() * 100,
			QueryRate:      rand.Float64() * 100,
			AntiCount:      rand.Int(),
		}
		stats[peer.StoreId].Stats =
			append(stats[peer.StoreId].Stats, statShow)
	}
	return
}

// Create n regions (0..n) of n stores (0..n).
// Each region contains np peers, the first peer is the leader.
func newTestHotRegions(n, np uint64) []*core.RegionInfo {
	regions := make([]*core.RegionInfo, 0, n)
	for i := uint64(0); i < n; i++ {
		peers := make([]*metapb.Peer, 0, np)
		for j := uint64(0); j < np; j++ {
			peer := &metapb.Peer{
				Id: i*np + j,
			}
			peer.StoreId = (i + j) % n
			peers = append(peers, peer)
		}
		region := &metapb.Region{
			Id:          i,
			Peers:       peers,
			StartKey:    []byte(fmt.Sprintf("%020d", i)),
			EndKey:      []byte(fmt.Sprintf("%020d", i+1)),
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2},
		}
		regions = append(regions, core.NewRegionInfo(region, peers[0]))
	}
	return regions
}

//Print dir size
func PrintDirSize(path string) {
	size, err := DirSizeB(path)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("file size %d\n", size)
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
