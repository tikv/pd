package main

import (
	"fmt"
	"os"
	"strconv"
	"os/exec"
	"context"

	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/client"
	"github.com/tikv/pd/client/constants"
)

func updateServiceGCSafePoint(serviceName string, barrierTS uint64) error {
	cmd := exec.Command("go", "run", "oldAPI/main.go", serviceName, strconv.FormatUint(barrierTS, 10))
	output, err := cmd.Output()
	fmt.Println("====")
	os.Stdout.Write(output)
	return err
}

func main() {
	// This is a test for GC API compatibility
	// The purpose of this test is to check that the old client API always works with the latest API. 
	// The called cmd use the old client and API UpdateServiceGCSafePoint
	// Using the latest client to check, we need to verify the GC block by the old UpdateServiceGCSafePoint API

	pdcli, err := pd.NewClient(caller.Component("test"),
		[]string{"127.0.0.1:2379"}, pd.SecurityOption{})
	if err != nil {
		fmt.Println("open pd client fail??", err)
		return
	}
	defer pdcli.Close()

	cli := pdcli.GetGCInternalController(constants.NullKeyspaceID)
	cli1 := pdcli.GetGCStatesClient(constants.NullKeyspaceID)

	serviceName := "compatibility-test"
	err = updateServiceGCSafePoint(serviceName, 666)
	if err != nil {
		fmt.Println(err)
	}

	// Old updateServiceGCSafePoint is transformed into new GC barrier API.
	// Check the barrier info using the new API.
	state, err := cli1.GetGCState(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println("====", len(state.GCBarriers))
	for _, barrier := range state.GCBarriers {
		fmt.Println("barrier ===", barrier.BarrierID, barrier.BarrierTS)
		// if barrier.BarrierID == serviceName {
		// 	if barrier.BarrierTS != 666 {
		// 		panic("barrier ts incorrect for UpdateServiceGCSafePoint")
		// 	}
		// 	found = true
		// 	break
		// }
	}

	// Check gc safe point is blocked by the old UpdateServiceGCSafePoint API

	result2, err := cli.AdvanceTxnSafePoint(context.Background(), 777)
	// if err != nil {
	// 	panic(err)
	// }
	// if result2.NewTxnSafePoint != 666 {
		fmt.Println("111 advance gc safe point result ==", result2)
		// panic("111 advance txn safe point should be pushed")
	// }

	result1, err := cli.AdvanceGCSafePoint(context.Background(), 777)
	if err != nil {
		fmt.Println("000 advance gc safe point", result1)
		// panic(err)
		// "[PD:gc:ErrGCSafePointExceedsTxnSafePoint]trying to update GC safe point to a too large value
	}

	// ==============================
	// UpdateServiceGCSafePoint and test again, now it should success
	err = updateServiceGCSafePoint(serviceName, 999)
	if err != nil {
		fmt.Println(err)
	}
	// Check gc safe point is blocked by the old UpdateServiceGCSafePoint API
	result2, err = cli.AdvanceTxnSafePoint(context.Background(), 777)
	if err != nil {
		panic(err)
	}
	// if result2.NewGCSafePoint != 667 {
		fmt.Println("333 advance txn safe point result ==", result2)
		// panic("222 advance txn safe point should be pushed")
	// }

	result3, err := cli.AdvanceGCSafePoint(context.Background(), 777)
	// if result2.NewGCSafePoint != 667 {
		fmt.Println("444 advance gc safe point result ==", result3)
		// panic("222 advance txn safe point should be pushed")
	// }

	// ========================
	// Test special behavior, UpdateServiceGCSafePoint(gc_worker) should be equal to advance txn safe point
	// but the old code is not handling that?
	err = updateServiceGCSafePoint("gc_worker", 10001)
	if err != nil {
		fmt.Println("5555 what's wrong", err)
		if raw, ok := err.(*exec.ExitError); ok {
			fmt.Println("lalala", string(raw.Stderr))
		}
	}
	state, err = cli1.GetGCState(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println("???", state, len(state.GCBarriers))
	if len(state.GCBarriers) >= 2 {
		panic("UpdateServiceGCSafePoint(gc_worker should be converted to advance txn safe point)")
	}
	for _, barrier := range state.GCBarriers {
		fmt.Println("234123451234 ===", barrier.BarrierID, barrier.BarrierTS)
	}
	// if state.TxnSafePoint != 10001 {
	// 	panic("txn safe point should be updated by UpdateServiceGCSafePoint(gc_worker)")
	// }
}
