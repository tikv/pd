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

func fuzzSetGCBarrier(ctx context.Context, wg *sync.WaitGroup, cli gc.GCStatesClient, barrierID string) {
	// Action: push forward barrier by random [1-5] using SetGCBarrier()
	// If barrier ts is too much larger than txn safe point, sleep a while for it to catch up
	// Invariance: barrier ts >= txn safe point
	defer wg.Done()
	gcState, err := getGCState(ctx, cli)
	if err != nil {
		fmt.Println("fuzzSetGCBarrier exit with error", err)
		return
	}
	barrierTS := gcState.TxnSafePoint + 1
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		barrierTS = barrierTS + 1 + uint64(rand.Int63n(5))
		barrierInfo, err := setGCBarrier(ctx, cli, barrierID, barrierTS, time.Duration(math.MaxInt64))
		if err != nil {
			errStr := err.Error()
			if strings.Contains(errStr, "context canceled") && ctx.Err() != nil {
				return
			}
			if strings.Contains(errStr, "ErrGCBarrierTSBehindTxnSafePoint") {
				// retry push forward barrierTS with lager value
				continue
			}
			fmt.Printf("SetGCBarrier error = %+v\n", err)
			time.Sleep(5 * time.Millisecond)
			continue
		}
		gcState, err := getGCState(ctx, cli)
		if err != nil {
			if strings.Contains(err.Error(), "context canceled") && ctx.Err() != nil {
				return
			}
			fmt.Printf("GetGCState error = %+v\n", err)
			time.Sleep(5 * time.Millisecond)
			continue
		}
		// Check invariance
		if barrierInfo.BarrierTS < gcState.TxnSafePoint {
			panic("fuzzSetGCBarrier")
		}
		if barrierInfo.BarrierTS > gcState.TxnSafePoint+30 {
			time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
		}
	}
}

func fuzzGetGCState(ctx context.Context, wg *sync.WaitGroup, cli gc.GCStatesClient) {
	// Action: call GetGCState() once a while
	// Invariance: txn safe point and gc safe point should never go backward
	defer wg.Done()

	gcState, err := getGCState(ctx, cli)
	if err != nil {
		fmt.Printf("GetGCState error = %+v\n", err)
		return
	}

	lastTxnSafePoint, lastGCSafePoint := gcState.TxnSafePoint, gcState.GCSafePoint
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
		// Check invariance
		if gcState.TxnSafePoint < lastTxnSafePoint {
			panic("GetGCState find txn safe point jump back")
		}
		if gcState.GCSafePoint < lastGCSafePoint {
			panic("GetGCState find gc safe point jump back")
		}
		lastTxnSafePoint, lastGCSafePoint = gcState.TxnSafePoint, gcState.GCSafePoint

		fmt.Println("current GC state txn safe point:", gcState.TxnSafePoint, "gc safe point:", gcState.GCSafePoint)
		for _, barrier := range gcState.GCBarriers {
			fmt.Println("barrier id:", barrier.BarrierID, "barrier ts:", barrier.BarrierTS)
		}
		time.Sleep(time.Second)
	}
}

func fuzzAdvanceTxnSafePoint(ctx context.Context, wg *sync.WaitGroup, cli gc.InternalController, cli1 gc.GCStatesClient) {
	// Action: push forward txn safe point by random [1-5] using AdvanceTxnSafePoint()
	// Invariance: txn safe point <= barrier ts and txn safe point >= gc safe point
	defer wg.Done()

	gcState, err := getGCState(ctx, cli1)
	if err != nil {
		fmt.Printf("GetGCState error =%+v\n", err)
		return
	}
	txnSafePoint := gcState.TxnSafePoint
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		target := txnSafePoint + 1 + uint64(rand.Int63n(5))
		result, err := advanceTxnSafePoint(ctx, cli, target)
		if err != nil {
			if strings.Contains(err.Error(), "context canceled") && ctx.Err() != nil {
				return
			}
			if strings.Contains(err.Error(), "ErrDecreasingTxnSafePoint") {
				fmt.Println("should this happen?", err)
			} else {
				fmt.Printf("SetGCBarrier error =%+v\n", err)
				time.Sleep(5 * time.Millisecond)
			}
			continue
		}
		if result.NewTxnSafePoint != target {
			// not pushed to target value
		}
		txnSafePoint = result.NewTxnSafePoint

		// Check invariance
		gcState, err := getGCState(ctx, cli1)
		if err != nil {
			if strings.Contains(err.Error(), "context canceled") && ctx.Err() != nil {
				return
			}
			fmt.Printf("GetGCState error =%+v\n", err)
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if txnSafePoint < gcState.TxnSafePoint {
			panic("fuzzAdvanceTxnSafePoint")
		}
		for _, barrier := range gcState.GCBarriers {
			if !(txnSafePoint <= barrier.BarrierTS) {
				panic("txn safe point must <= barrier ts")
			}
		}
		if txnSafePoint < gcState.GCSafePoint {
			panic(fmt.Sprintf("txn safe point %d must >= gc safe point %d", txnSafePoint, gcState.GCSafePoint))
		}
		if txnSafePoint > gcState.GCSafePoint+30 {
			time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
		}
	}
}

func fuzzAdvanceGCSafePoint(ctx context.Context, wg *sync.WaitGroup, cli gc.InternalController, cli1 gc.GCStatesClient) {
	// Action: push forward gc safe point by random [1-5] using AdvanceGCSafePoint()
	// Invariance: txn safe point >= gc safe point
	defer wg.Done()

	gcState, err := getGCState(ctx, cli1)
	if err != nil {
		fmt.Printf("GetGCState error =%+v\n", err)
		return
	}
	gcSafePoint := gcState.GCSafePoint
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		target := gcSafePoint + 1 + uint64(rand.Int63n(5))
		result, err := advanceGCSafePoint(ctx, cli, target)
		if err != nil {
			if strings.Contains(err.Error(), "context canceled") && ctx.Err() != nil {
				return
			}
			if strings.Contains(err.Error(), "ErrGCSafePointExceedsTxnSafePoint") {
				// don't push too harsh in case of this error
				// sleep a while for txn safe point to catch up
				time.Sleep(5 * time.Millisecond)
				continue
			}

			fmt.Printf("SetGCBarrier error =%+v\n", err)
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if result.NewGCSafePoint == target {
			// Success
			gcSafePoint = result.NewGCSafePoint
		} else {
			// Blocked
		}

		// Check invariance
		gcState, err := getGCState(ctx, cli1)
		if err != nil {
			if strings.Contains(err.Error(), "context canceled") && ctx.Err() != nil {
				return
			}
			fmt.Printf("GetGCState error =%+v\n", err)
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if gcSafePoint > gcState.TxnSafePoint {
			panic("gc safe point must <= txn safe point")
		}
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

func fuzzResignLeader(ctx context.Context, wg *sync.WaitGroup) {
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

	keyspaceID := uint32(1)

	cli1 := pdcli.GetGCStatesClient(keyspaceID)
	state, err := getGCState(context.Background(), cli1)
	fmt.Println("get gcstate info ==", state)

	cli2 := pdcli.GetGCInternalController(keyspaceID)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(6)
	go fuzzSetGCBarrier(ctx, &wg, cli1, "barrier1")
	go fuzzSetGCBarrier(ctx, &wg, cli1, "barrier2")
	go fuzzGetGCState(ctx, &wg, cli1)
	go fuzzAdvanceTxnSafePoint(ctx, &wg, cli2, cli1)
	go fuzzAdvanceGCSafePoint(ctx, &wg, cli2, cli1)
	go fuzzResignLeader(ctx, &wg)

	time.Sleep(10 * time.Minute)

	cancel()
	wg.Wait()
	fmt.Println("test success")
}
