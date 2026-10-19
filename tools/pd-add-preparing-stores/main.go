package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/client/pkg/utils/grpcutil"
	"go.uber.org/zap"
)

const (
	operationTimeout = 30 * time.Second
	storeVersion     = "9.0.0-alpha.1"
)

type options struct {
	addr    string
	startID uint64
	count   uint64
}

// pd-add-preparing-stores addr startID count
// addr example: http://10.200.24.228:2379
func main() {
	opts, err := parseArgs(os.Args)
	if err != nil {
		log.Fatal(err.Error())
	}
	if err := run(context.Background(), opts); err != nil {
		log.Fatal(err.Error())
	}
}

func parseArgs(args []string) (options, error) {
	if len(args) != 4 {
		return options{}, errors.New("usage: pd-add-preparing-stores <addr> <startID> <count>")
	}
	startID, err := strconv.ParseUint(args[2], 10, 64)
	if err != nil || startID == 0 {
		return options{}, fmt.Errorf("invalid startID %q: must be a positive uint64", args[2])
	}
	count, err := strconv.ParseUint(args[3], 10, 64)
	if err != nil || count == 0 {
		return options{}, fmt.Errorf("invalid count %q: must be a positive uint64", args[3])
	}
	if count > math.MaxUint64-startID+1 {
		return options{}, fmt.Errorf("store ID range overflows uint64: startID=%d count=%d", startID, count)
	}
	return options{addr: args[1], startID: startID, count: count}, nil
}

func checkDuplicateStoreIDs(existing map[uint64]struct{}, startID, count uint64) error {
	for offset := uint64(0); offset < count; offset++ {
		id := startID + offset
		if _, ok := existing[id]; ok {
			return fmt.Errorf("store ID %d already exists; choose an unused ID range", id)
		}
	}
	return nil
}

func run(parent context.Context, opts options) error {
	ctx, cancel := context.WithTimeout(parent, operationTimeout)
	defer cancel()

	cc, err := grpcutil.GetClientConn(ctx, opts.addr, nil)
	if err != nil {
		return fmt.Errorf("create client conn failed: %w", err)
	}
	defer cc.Close()
	cli := pdpb.NewPDClient(cc)
	res, err := cli.GetMembers(ctx, &pdpb.GetMembersRequest{})
	if err != nil {
		return fmt.Errorf("get members failed: %w", err)
	}
	if err := responseError(res.GetHeader()); err != nil {
		return fmt.Errorf("get members returned error: %w", err)
	}
	cid := res.GetHeader().GetClusterId()
	hdr := func() *pdpb.RequestHeader { return &pdpb.RequestHeader{ClusterId: cid} }

	stores, err := cli.GetAllStores(ctx, &pdpb.GetAllStoresRequest{Header: hdr()})
	if err != nil {
		return fmt.Errorf("get all stores failed: %w", err)
	}
	if err := responseError(stores.GetHeader()); err != nil {
		return fmt.Errorf("get all stores returned error: %w", err)
	}
	existing := make(map[uint64]struct{}, len(stores.GetStores()))
	for _, store := range stores.GetStores() {
		if store != nil {
			existing[store.GetId()] = struct{}{}
		}
	}
	if err := checkDuplicateStoreIDs(existing, opts.startID, opts.count); err != nil {
		return err
	}

	for offset := uint64(0); offset < opts.count; offset++ {
		i := opts.startID + offset
		store := &metapb.Store{
			Id:        i,
			Address:   fmt.Sprintf("mock://tikv-%d:%d", i, i),
			Version:   storeVersion,
			NodeState: metapb.NodeState_Preparing,
		}
		r, err := cli.PutStore(ctx, &pdpb.PutStoreRequest{Header: hdr(), Store: store})
		if err != nil {
			return fmt.Errorf("put store %d failed: %w", i, err)
		}
		if err := responseError(r.GetHeader()); err != nil {
			return fmt.Errorf("put store %d returned error: %w", i, err)
		}
		log.Info("put preparing store", zap.Uint64("id", i))
	}
	log.Info("done", zap.Uint64("added", opts.count))
	return nil
}

func responseError(header *pdpb.ResponseHeader) error {
	if header == nil || header.GetError() == nil {
		return nil
	}
	return errors.New(header.GetError().String())
}
