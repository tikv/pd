package main

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/gc"
	"github.com/tikv/pd/client/pkg/caller"
)

func getGCState(ctx context.Context, cli gc.GCStatesClient) (gc.GCState, error) {
	for {
		ret, err := cli.GetGCState(ctx)
		if err != nil {
			errStr := err.Error()
			// This error is too common, sleep and retry on it
			if strings.Contains(errStr, "ErrEtcdTxnConflict") || strings.Contains(errStr, "not leader") {
				time.Sleep(time.Duration(rand.Intn(10)+60) * time.Millisecond)
				continue
			}
		}
		return ret, err
	}
}

func advanceTxnSafePoint(ctx context.Context, cli gc.InternalController, target uint64) (gc.AdvanceTxnSafePointResult, error) {
	for {
		result, err := cli.AdvanceTxnSafePoint(ctx, target)
		if err != nil {
			errStr := err.Error()
			if strings.Contains(errStr, "not leader") || strings.Contains(errStr, "DeadlineExceeded") {
				time.Sleep(time.Duration(rand.Intn(10)+60) * time.Millisecond)
				continue
			}
		}
		return result, err
	}
}

func advanceGCSafePoint(ctx context.Context, cli gc.InternalController, target uint64) (gc.AdvanceGCSafePointResult, error) {
	for {
		result, err := cli.AdvanceGCSafePoint(ctx, target)
		if err != nil {
			errStr := err.Error()
			if strings.Contains(err.Error(), "not leader") || strings.Contains(errStr, "DeadlineExceeded") {
				time.Sleep(time.Duration(rand.Intn(10)+60) * time.Millisecond)
				continue
			}
		}
		return result, err
	}
}

func setGCBarrier(ctx context.Context, cli gc.GCStatesClient, barrierID string, barrierTS uint64, ttl time.Duration) (*gc.GCBarrierInfo, error) {
	for {
		barrierInfo, err := cli.SetGCBarrier(ctx, barrierID, barrierTS, ttl)
		if err != nil {
			errStr := err.Error()
			if strings.Contains(errStr, "not leader") || strings.Contains(errStr, "DeadlineExceeded") {
				time.Sleep(time.Duration(rand.Intn(10)+60) * time.Millisecond)
				continue
			}
		}
		return barrierInfo, err
	}
}

type fuzzOperation interface {
	fuzz(ctx context.Context, wg *sync.WaitGroup)
}

type fuzzSetGCBarrier struct {
	cli pd.Client
	barrierID string
}

func (f fuzzSetGCBarrier) fuzz(ctx context.Context, wg *sync.WaitGroup) {
	// barrier TS use a random value between [now-20min, tso)
	physical, logical, err := f.cli.GetTS(ctx)
	tso := oracle.ComposeTS(physical, logical)
	if err != nil {
		fmt.Println("get tso error?", err)
		return
	}
	lowerBound := oracle.GoTimeToTS(time.Now()) - uint64(20 * time.Minute)
	barrierTS := lowerBound + uint64(rand.Int63n(int64(tso - lowerBound)))
	// fmt.Println("lowerBound = ", lowerBound, "tso=", tso, "barrier ts=", barrierTS)

	keyspaceID := uint32(1)
	gcCli := f.cli.GetGCStatesClient(keyspaceID)
	_, err = setGCBarrier(ctx, gcCli, f.barrierID, barrierTS, time.Duration(math.MaxInt64))
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "context canceled") && ctx.Err() != nil {
			return
		}
		if strings.Contains(errStr, "ErrGCBarrierTSBehindTxnSafePoint") {
			// fmt.Println("barrier ts behind txn safe point", errStr)
			return
		}
		fmt.Printf("SetGCBarrier error = %+v\n", err)
	}
}

type fuzzAdvanceTxnSafePoint struct {
	cli gc.InternalController
}

func (f fuzzAdvanceTxnSafePoint) fuzz(ctx context.Context, wg *sync.WaitGroup) {
	// advance txn safe point use a random value between now - 10min + rand(-10s, 10s)
	target := oracle.GoTimeToTS(time.Now()) -
		uint64(10 * time.Minute - 10 * time.Second) +
		uint64(rand.Int63n(20)) * uint64(time.Second)
	_, err := advanceTxnSafePoint(ctx, f.cli, target)
	if err != nil {
		if strings.Contains(err.Error(), "context canceled") && ctx.Err() != nil {
			return
		}
		if strings.Contains(err.Error(), "ErrDecreasingTxnSafePoint") {
			return
		}
		fmt.Printf("SetGCBarrier error =%+v\n", err)
		time.Sleep(5 * time.Millisecond)
	}
	// if result.NewTxnSafePoint != target {
	// 	// not pushed to target value
	// }
	// txnSafePoint = result.NewTxnSafePoint

	// // Check invariance
	// gcState, err := getGCState(ctx, cli1)
	// if err != nil {
	// 	if strings.Contains(err.Error(), "context canceled") && ctx.Err() != nil {
	// 		return
	// 	}
	// 	fmt.Printf("GetGCState error =%+v\n", err)
	// 	time.Sleep(5 * time.Millisecond)
	// 	continue
	// }
	// if txnSafePoint < gcState.TxnSafePoint {
	// 	panic("fuzzAdvanceTxnSafePoint")
	// }
	// for _, barrier := range gcState.GCBarriers {
	// 	if !(txnSafePoint <= barrier.BarrierTS) {
	// 		panic("txn safe point must <= barrier ts")
	// 	}
	// }
	// if txnSafePoint < gcState.GCSafePoint {
	// 	panic(fmt.Sprintf("txn safe point %d must >= gc safe point %d", txnSafePoint, gcState.GCSafePoint))
	// }
	// if txnSafePoint > gcState.GCSafePoint+30 {
	// 	time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
	// }
}

type fuzzAdvanceGCSafePoint struct {
	cli gc.InternalController
}

func (f fuzzAdvanceGCSafePoint) fuzz(ctx context.Context, wg *sync.WaitGroup) {
	// advance txn safe point use a random value between now - 10min + rand(-10s, 10s)
	target := oracle.GoTimeToTS(time.Now() ) -
		uint64(10 * time.Minute - 10 * time.Second) +
		uint64(rand.Int63n(20)) * uint64(time.Second)
	_, err := advanceGCSafePoint(ctx, f.cli, target)
	if err != nil {
		if strings.Contains(err.Error(), "context canceled") && ctx.Err() != nil {
			return
		}
		if strings.Contains(err.Error(), "ErrGCSafePointExceedsTxnSafePoint") {
			return
		}
		if strings.Contains(err.Error(), "ErrDecreasingGCSafePoint") {
			return
		}

		fmt.Printf("SetGCBarrier error =%+v\n", err)
		time.Sleep(5 * time.Millisecond)
		return
	}
	// if result.NewGCSafePoint == target {
	// 	// Success
	// 	gcSafePoint = result.NewGCSafePoint
	// } else {
	// 	// Blocked
	// }

	// // Check invariance
	// gcState, err := getGCState(ctx, cli1)
	// if err != nil {
	// 	if strings.Contains(err.Error(), "context canceled") && ctx.Err() != nil {
	// 		return
	// 	}
	// 	fmt.Printf("GetGCState error =%+v\n", err)
	// 	time.Sleep(5 * time.Millisecond)
	// 	continue
	// }
	// if gcSafePoint > gcState.TxnSafePoint {
	// 	panic("gc safe point must <= txn safe point")
	// }
}

func checkInvariance(ctx context.Context, wg *sync.WaitGroup, pdcli pd.Client) {
	defer wg.Done()
	// Invariance:
	// 1. txn safe point and gc safe point should never decrease
	// 2. gc safe point <= txn safe point
	// 3. txn safe point <= min{barrier ts}
	// 4. txn safe point <= max{start ts}?
	keyspaceID := uint32(1)
	cli := pdcli.GetGCStatesClient(keyspaceID)

	var lastTxnSafePoint, lastGCSafePoint uint64
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		gcState, err := getGCState(ctx, cli)
		if err != nil {
			if strings.Contains(err.Error(), "context canceled") && ctx.Err() != nil {
				return
			}
			fmt.Printf("GetGCState error =%+v\n", err)
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if lastTxnSafePoint != 0 &&  gcState.TxnSafePoint < lastTxnSafePoint {
			panic("txn safe point jump back")
		}
		if lastGCSafePoint != 0 && gcState.GCSafePoint < lastGCSafePoint {
			panic("gc safe point jump back")
		}
		lastTxnSafePoint, lastGCSafePoint = gcState.TxnSafePoint, gcState.GCSafePoint

		if !(lastTxnSafePoint >= lastGCSafePoint) {
			panic("txn safe point must >= gc safe point")
		}

		fmt.Println("current GC state txn safe point:", gcState.TxnSafePoint, "gc safe point:", gcState.GCSafePoint)
		for _, barrier := range gcState.GCBarriers {
			fmt.Println("barrier id:", barrier.BarrierID, "barrier ts:", barrier.BarrierTS)
			if !(lastTxnSafePoint <= barrier.BarrierTS) {
				panic("txn safe point must <= barrier ts")
			}
		}
		time.Sleep(time.Second)
	}
}

func resignLeader() error {
	resp, err := http.Post("http://127.0.0.1:2379/pd/api/v1/leader/resign", "", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if !strings.Contains(string(body), "The resign command is submitted") {
		fmt.Println("resign leader not success:", string(body))
	}
	return nil
}

func chaosResignLeader(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		err := resignLeader()
		if err != nil {
			if strings.Contains(err.Error(), "context canceled") && ctx.Err() != nil {
				return
			}
			if strings.Contains(err.Error(), "request timed out") {
				// ignore this kind of error
			} else {
				fmt.Println("resign leader err", err)
			}
			time.Sleep(time.Second)
		}
		time.Sleep(time.Duration(3+rand.Intn(4)) * time.Second)
	}
}

func fuzzGoroutine(ctx context.Context, wg *sync.WaitGroup, pdcli pd.Client) {
	defer wg.Done()

	keyspaceID := uint32(1)
	operations := []fuzzOperation{
		fuzzSetGCBarrier{pdcli, "barrier1"},
		fuzzSetGCBarrier{pdcli, "barrier2"},
		fuzzAdvanceTxnSafePoint{pdcli.GetGCInternalController(keyspaceID)},
		fuzzAdvanceGCSafePoint{pdcli.GetGCInternalController(keyspaceID)},
	}
	for {
		// check exit signal
		select {
		case <-ctx.Done():
			return
		default:
		}
		
		// choose a random fuzz operation
		n := rand.Intn(len(operations))
		op := operations[n]

		// run the operation one round
		op.fuzz(ctx, wg)
	}
}

func fuzzMinStartTS(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
}

func main() {
	pdcli, err := pd.NewClient(caller.Component("test"),
		[]string{"127.0.0.1:2379"}, pd.SecurityOption{})
	if err != nil {
		fmt.Println("open pd client fail??", err)
		return
	}
	defer pdcli.Close()

	clusterID := pdcli.GetClusterID(context.Background())
	fmt.Println("cluster id ==", clusterID)
	leader := pdcli.GetLeaderURL()
	fmt.Println("leader url ==", leader)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	Concurrency := 20
	wg.Add(Concurrency + 3)

	for i:=0; i<20; i++ {
		go fuzzGoroutine(ctx, &wg, pdcli)
	}
	go fuzzMinStartTS(ctx, &wg)
	go chaosResignLeader(ctx, &wg)
	go checkInvariance(ctx, &wg, pdcli)

	time.Sleep(10 * time.Minute)

	cancel()
	wg.Wait()
	fmt.Println("test success")
}
