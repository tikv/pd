package main

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"go.uber.org/zap"
)

const (
	aiURL         = "http://192.168.1.127:8000/"
	fetchInterval = time.Hour / 3
)

var hightPerformanceStoreID = []uint64{1}

type hotSpotPeriod struct {
	StartTime int64
	EndTime   int64
	TableKey  []int
}

type hotSpotPeriodSlice []hotSpotPeriod

func (h hotSpotPeriodSlice) Len() int {
	return h.Len()
}

func (h hotSpotPeriodSlice) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h hotSpotPeriodSlice) Less(i, j int) bool {
	return h[i].StartTime < h[j].StartTime
}

type scheduleTime struct {
	StartTime int64
	EndTime   int64
	DeltT     []int64 // delt t for each table
}

type preTableInfo struct {
	Predict        []float64 `json:"predict"`
	StartKey       string    `json:"start_key"`
	EndKey         string    `json:"end_key"`
	MaxValue       float64   `json:"max_value"`
	MinValue       float64   `json:"min_value"`
	HistoryR2Score float64   `json:"history_r2_score"`
}

type predictInfo struct {
	Time                int64          `json:"time"`
	TableNum            int            `json:"table_num"`
	PredictStep         int            `json:"predict_step"`
	HistoryR2ScoreTotal float64        `json:"history_r2_score_tot"`
	TableInfo           []preTableInfo `json:"table_info"`
}

func newPredictInfo() (predictInfo, error) {
	var p predictInfo
	resp, err := http.Get(aiURL)
	if err != nil {
		return predictInfo{}, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return predictInfo{}, err
	}
	log.Info("read json file", zap.String("json data", string(body)))
	//fmt.Println("json:", string(body))

	err2 := json.Unmarshal(body, &p)
	if err2 != nil {
		return predictInfo{}, err2
	}
	log.Info("parse json data", zap.Int("TableNum", p.TableNum), zap.Any("TableInfo", p.TableInfo))
	//fmt.Printf("%+v", predictInfo)
	return p, nil
}

func (p *predictInfo) calculateThreshold() (float64, error) {
	if p.PredictStep <= 0 || p.TableNum <= 0 {
		err := errors.New("predictstep or tablenum is less than 0")
		return 0, err
	}
	preDataTotal := make([]float64, p.PredictStep)
	for i := 0; i < p.PredictStep; i++ {
		preDataTotal[i] = 0
		for j := 0; j < p.TableNum; j++ {
			preDataTotal[i] += p.TableInfo[j].Predict[i]
		}
		preDataTotal[i] = preDataTotal[i] / float64(p.TableNum)
	}
	sort.Float64s(preDataTotal)

	ave := (preDataTotal[p.PredictStep/5] + preDataTotal[p.PredictStep/5+1]) / 2
	log.Info("calculateThreshold", zap.Float64("threshold", ave))
	return ave, nil
}

var gLastPeriod hotSpotPeriod

func (p *predictInfo) calculateScheduleTime(threshold float64) ([]scheduleTime, error) {
	hotPoint := make([][]bool, p.TableNum)
	for i := 0; i < p.TableNum; i++ {
		hotPoint[i] = make([]bool, p.PredictStep)
	}
	hotSpotArr := make([]hotSpotPeriod, 0, p.TableNum*2)
	for i := 0; i < p.TableNum; i++ {
		flag := false
		var startT int64
		var endT int64
		for j := 0; j < p.PredictStep; j++ {
			if p.TableInfo[i].Predict[j] > threshold {
				hotPoint[i][j] = true
			} else {
				hotPoint[i][j] = false
			}
			if hotPoint[i][j] == true && flag == false {
				flag = true
				startT = int64(j+1)*60 + p.Time
			}
			if (hotPoint[i][j] == false && flag == true) || (hotPoint[i][j] == true && flag == true && j == p.PredictStep-1) {
				flag = false
				endT = int64(j)*60 + p.Time
				hotSpotArr = append(hotSpotArr, hotSpotPeriod{startT, endT, []int{i}})
			}
		}
	}
	if len(hotSpotArr) <= 0 {
		err := errors.New("no hot spot")
		return nil, err
	}
	log.Info("before sort hotSpotArr", zap.Any("hotSpotArr", hotSpotArr))
	//sort.Sort(hotSpotPeriodSlice(hotSpotArr))
	bubbleSortHotSpot(hotSpotArr)
	log.Info("after sort hotSpotArr", zap.Any("hotSpotArr", hotSpotArr))
	//fmt.Println(hotSpotArr)

	period := make([]hotSpotPeriod, 0, p.TableNum*2)
	period = append(period, gLastPeriod)
	i := 0
	j := 0
	tableArr := []int{}
	startT := hotSpotArr[0].StartTime
	endT := hotSpotArr[j].EndTime
	for {
		if hotSpotArr[j].StartTime-endT > 4*60 {
			period = append(period, hotSpotPeriod{startT, endT, tableArr})
			startT = hotSpotArr[j].StartTime
			endT = hotSpotArr[j].EndTime
			tableArr = []int{}
			i = j
		} else {
			if hotSpotArr[j].EndTime > hotSpotArr[i].EndTime {
				endT = hotSpotArr[j].EndTime
			}
			tableArr = append(tableArr, hotSpotArr[j].TableKey...)
			j++
		}
		if j == len(hotSpotArr) {
			for _, v := range period {
				if v.StartTime == startT && v.EndTime == endT {
					break
				}
			}
			period = append(period, hotSpotPeriod{startT, endT, tableArr})
			break
		}
	}
	log.Info("generate period", zap.Any("period", period))
	//fmt.Println(period)

	scheduleT := make([]scheduleTime, 0, p.TableNum)
	for i := 1; i < len(period); i++ {
		tmp := (period[i].StartTime + period[i-1].EndTime) / 2
		if period[i].StartTime-period[i-1].EndTime > 4*60 {
			if period[i-1].EndTime == 0 {
				tmp = p.Time
			}
			arr := make([]int64, p.TableNum)
			scheduleT = append(scheduleT, scheduleTime{tmp, period[i].EndTime, arr})
		}
	}
	for _, v := range scheduleT {
		for _, h := range hotSpotArr {
			tmp := (h.StartTime + h.EndTime) / 2
			if tmp >= v.StartTime && tmp <= v.EndTime {
				v.DeltT[h.TableKey[0]] = 1
			} else if tmp > v.EndTime {
				t := (tmp - v.StartTime) / 60
				if v.DeltT[h.TableKey[0]] == 0 || t < v.DeltT[h.TableKey[0]] {
					v.DeltT[h.TableKey[0]] = t
				}
			} else {
				var t int64
				tfloat := float64((tmp - v.StartTime) / 60)
				t = int64(math.Floor(math.Pow(tfloat, 2)))

				if v.DeltT[h.TableKey[0]] == 0 || t < v.DeltT[h.TableKey[0]] {
					v.DeltT[h.TableKey[0]] = t
				}
			}
		}
	}
	log.Info("generate schedule time", zap.Any("schedule time", scheduleT))
	//fmt.Println(scheduleT)

	gLastPeriod = period[len(period)-1]
	return scheduleT, nil
}

func (p *predictInfo) getTableIndexByRegion(meta *metapb.Region) int {
	tableInfo := p.TableInfo
	str := string(core.HexRegionKey(meta.GetStartKey()))
	for index, t := range tableInfo {
		if strings.Compare(t.StartKey, str) <= 0 && (t.EndKey == "" || strings.Compare(t.EndKey, str) > 0) {
			return index
		}
	}
	return 1
}

type dispatchTiming struct {
	predictData  predictInfo
	threshold    float64
	scheduleTime []scheduleTime
}

func generateDispatchT() *dispatchTiming {
	p, err := newPredictInfo()
	if err != nil {
		log.Info("newPredictInfo err", zap.Error(err))
		return nil
	}
	threshold, err := p.calculateThreshold()
	if err != nil {
		log.Info("calculateThreshold err", zap.Error(err))
		return nil
	}
	scheduleTime, err := p.calculateScheduleTime(threshold)
	if err != nil {
		log.Info("calculateScheduleTime err", zap.Error(err))
		return nil
	}
	return &dispatchTiming{
		predictData:  p,
		threshold:    threshold,
		scheduleTime: scheduleTime,
	}
}

func processDispatchTiming(ch chan *dispatchTiming) {
	timer := time.NewTimer(fetchInterval)
	defer timer.Stop()

	ch <- generateDispatchT()
	for {
		select {
		case <-timer.C:
			log.Info("fetch predict info")
			timer.Reset(fetchInterval)
			ch <- generateDispatchT()
		}
	}
}

// RegionHotTable is used to sort region by it's hotdegree.
type RegionHotTable struct {
	count    uint64
	regionID []uint64
}

func getTopK(cluster *server.RaftCluster, index int, dispatchT *dispatchTiming) []uint64 {
	DeltT := dispatchT.scheduleTime[index].DeltT
	regions := cluster.GetRegions()
	if len(regions) <= 0 {
		log.Info("not found region")
		return []uint64{}
	}
	minrw := regions[0].GetRwBytesTotal() / uint64(DeltT[dispatchT.predictData.getTableIndexByRegion(regions[0].GetMeta())])
	maxrw := regions[0].GetRwBytesTotal() / uint64(DeltT[dispatchT.predictData.getTableIndexByRegion(regions[0].GetMeta())])
	tmp := make([]uint64, len(regions))
	for index, v := range regions {
		tmp[index] = v.GetRwBytesTotal() / uint64(DeltT[dispatchT.predictData.getTableIndexByRegion(v.GetMeta())])
		if minrw > tmp[index] {
			minrw = tmp[index]
		}
		if maxrw < tmp[index] {
			maxrw = tmp[index]
		}
	}
	segment := (maxrw - minrw) / uint64(len(regions))
	if segment == 0 {
		segment = 1
	}
	HotDegree := make([]RegionHotTable, len(regions)+1)
	for index, v := range regions {
		data := tmp[index]
		index := (data - minrw) / segment
		HotDegree[index].count++
		HotDegree[index].regionID = append(HotDegree[index].regionID, v.GetID())
	}
	k := 0
	topk := len(regions) / 10
	var retRegionID []uint64
	for _, h := range HotDegree {
		for _, i := range h.regionID {
			retRegionID = append(retRegionID, i)
			k++
			if k == topk {
				break
			}
		}
	}
	//fmt.Println(retRegionID)
	log.Info("GetTopK", zap.Any("TopK regionIDs", retRegionID))
	return retRegionID
}

func generateScheduleInfo(cluster *server.RaftCluster, regionIDs []uint64, scheduleT scheduleTime) {
	schedule.ScheduleInfoLock.Lock()
	defer schedule.ScheduleInfoLock.Unlock()
	for _, id := range regionIDs {
		region := cluster.GetRegion(id)
		moveRegionInfo := schedule.MoveRegion{
			StartKey:  region.GetStartKey(),
			EndKey:    region.GetEndKey(),
			StoreIDs:  hightPerformanceStoreID,
			StartTime: time.Unix(scheduleT.StartTime, 0),
			EndTime:   time.Unix(scheduleT.EndTime, 0),
		}
		schedule.ScheduleInfo = append(schedule.ScheduleInfo, moveRegionInfo)
	}
	log.Info("generateScheduleInfo", zap.Any("ScheudleInfo", schedule.ScheduleInfo))
}

func processTopK(cluster *server.RaftCluster, dispatchT *dispatchTiming, updateCh chan int) {
	scheduleTime := dispatchT.scheduleTime
	log.Info("processTopK", zap.Any("scheduleTime", scheduleTime))
	for index := 0; index < len(scheduleTime); index++ {
		now := time.Now()
		next := time.Unix(scheduleTime[index].StartTime, 0)
		t := time.NewTimer(next.Sub(now))
		<-t.C
		regionIDs := getTopK(cluster, index, dispatchT)
		generateScheduleInfo(cluster, regionIDs, scheduleTime[index])
		updateCh <- 1
	}
}

// ProcessPredictInfo is used to process predict info.
func ProcessPredictInfo(cluster *server.RaftCluster, updateCh chan int) {
	ch := make(chan *dispatchTiming)
	go processDispatchTiming(ch)
	for {
		select {
		case dispatchT := <-ch:
			go processTopK(cluster, dispatchT, updateCh)
		}
	}
}

// CreateUserScheduler create scheduler based on schedule info
func CreateUserScheduler(opController *schedule.OperatorController, cluster schedule.Cluster) []schedule.Scheduler {
	schedule.ScheduleInfoLock.Lock()
	defer schedule.ScheduleInfoLock.Unlock()
	schedulers := []schedule.Scheduler{}

	// produce schedulers
	for id, info := range schedule.ScheduleInfo {
		name := "move-region-use-scheduler-" + strconv.Itoa(id)
		schedulers = append(schedulers,
			newMoveRegionUserScheduler(opController, name,
				info.StartKey, info.EndKey, info.StoreIDs, info.StartTime, info.EndTime))
	}

	return schedulers
}

func bubbleSortHotSpot(values []hotSpotPeriod) {
	for i := 0; i < len(values)-1; i++ {
		flag := true
		for j := 0; j < len(values)-i-1; j++ {
			if values[j].StartTime > values[j+1].StartTime {
				values[j], values[j+1] = values[j+1], values[j]
				flag = false
			}
		}
		if flag == true {
			break
		}
	}
}
