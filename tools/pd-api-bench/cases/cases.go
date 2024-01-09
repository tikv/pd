// Copyright 2023 TiKV Project Authors.
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

package cases

import (
	"context"
	"fmt"
	"log"
	"math/rand"

	pd "github.com/tikv/pd/client"
	pdHttp "github.com/tikv/pd/client/http"
)

var (
	// Debug is the flag to print the output of api response for debug.
	Debug bool

	totalRegion int
	totalStore  int
	storesID    []uint64
)

// InitCluster initializes the cluster.
func InitCluster(ctx context.Context, cli pd.Client, httpCli pdHttp.Client) error {
	statsResp, err := httpCli.GetRegionStatusByKeyRange(ctx, pdHttp.NewKeyRange([]byte(""), []byte("")), false)
	if err != nil {
		return err
	}
	totalRegion = statsResp.Count

	stores, err := cli.GetAllStores(ctx)
	if err != nil {
		return err
	}
	totalStore = len(stores)
	storesID = make([]uint64, 0, totalStore)
	for _, store := range stores {
		storesID = append(storesID, store.GetId())
	}
	log.Printf("This cluster has region %d, and store %d[%v]", totalRegion, totalStore, storesID)
	return nil
}

// Case is the interface for all cases.
type Case interface {
	Name() string
	SetQPS(int64)
	GetQPS() int64
	SetBurst(int64)
	GetBurst() int64
}

type baseCase struct {
	name  string
	qps   int64
	burst int64
}

func (c *baseCase) Name() string {
	return c.name
}

func (c *baseCase) SetQPS(qps int64) {
	c.qps = qps
}

func (c *baseCase) GetQPS() int64 {
	return c.qps
}

func (c *baseCase) SetBurst(burst int64) {
	c.burst = burst
}

func (c *baseCase) GetBurst() int64 {
	return c.burst
}

// GRPCCase is the interface for all gRPC cases.
type GRPCCase interface {
	Case
	Unary(context.Context, pd.Client) error
}

// GRPCCraeteFn is function type to create GRPCCase.
type GRPCCraeteFn func() GRPCCase

// GRPCCaseFnMap is the map for all gRPC case creation function.
var GRPCCaseFnMap = map[string]GRPCCraeteFn{
	"GetRegion":   newGetRegion(),
	"GetStore":    newGetStore(),
	"GetStores":   newGetStores(),
	"ScanRegions": newScanRegions(),
	"Tso":         newTso(),
}

// GRPCCaseMap is the map for all gRPC case creation function.
var GRPCCaseMap = map[string]GRPCCase{}

// HTTPCase is the interface for all HTTP cases.
type HTTPCase interface {
	Case
	Do(context.Context, pdHttp.Client) error
}

// HTTPCraeteFn is function type to create GRPCCase.
type HTTPCraeteFn func() HTTPCase

// HTTPCaseFnMap is the map for all HTTP case creation function.
var HTTPCaseFnMap = map[string]HTTPCraeteFn{
	"GetRegionStatus":  newRegionStats(),
	"GetMinResolvedTS": newMinResolvedTS(),
}

// HTTPCaseMap is the map for all HTTP cases.
var HTTPCaseMap = map[string]HTTPCase{}

type minResolvedTS struct {
	*baseCase
}

func newMinResolvedTS() func() HTTPCase {
	return func() HTTPCase {
		return &minResolvedTS{
			baseCase: &baseCase{
				name:  "GetMinResolvedTS",
				qps:   1000,
				burst: 1,
			},
		}
	}
}

func (c *minResolvedTS) Do(ctx context.Context, cli pdHttp.Client) error {
	minResolvedTS, storesMinResolvedTS, err := cli.GetMinResolvedTSByStoresIDs(ctx, storesID)
	if Debug {
		log.Printf("Do %s: minResolvedTS: %d storesMinResolvedTS: %v err: %v", c.name, minResolvedTS, storesMinResolvedTS, err)
	}
	if err != nil {
		return err
	}
	return nil
}

type regionsStats struct {
	*baseCase
	regionSample int
}

func newRegionStats() func() HTTPCase {
	return func() HTTPCase {
		return &regionsStats{
			baseCase: &baseCase{
				name:  "GetRegionStatus",
				qps:   100,
				burst: 1,
			},
			regionSample: 1000,
		}
	}
}

func (c *regionsStats) Do(ctx context.Context, cli pdHttp.Client) error {
	upperBound := totalRegion / c.regionSample
	if upperBound < 1 {
		upperBound = 1
	}
	random := rand.Intn(upperBound)
	startID := c.regionSample*random*4 + 1
	endID := c.regionSample*(random+1)*4 + 1
	regionStats, err := cli.GetRegionStatusByKeyRange(ctx,
		pdHttp.NewKeyRange(generateKeyForSimulator(startID, 56), generateKeyForSimulator(endID, 56)), false)
	if Debug {
		log.Printf("Do %s: regionStats: %v err: %v", c.name, regionStats, err)
	}
	if err != nil {
		return err
	}
	return nil
}

type getRegion struct {
	*baseCase
}

func newGetRegion() func() GRPCCase {
	return func() GRPCCase {
		return &getRegion{
			baseCase: &baseCase{
				name:  "GetRegion",
				qps:   10000,
				burst: 1,
			},
		}
	}
}

func (c *getRegion) Unary(ctx context.Context, cli pd.Client) error {
	id := rand.Intn(totalRegion)*4 + 1
	_, err := cli.GetRegion(ctx, generateKeyForSimulator(id, 56))
	if err != nil {
		return err
	}
	return nil
}

type scanRegions struct {
	*baseCase
	regionSample int
}

func newScanRegions() func() GRPCCase {
	return func() GRPCCase {
		return &scanRegions{
			baseCase: &baseCase{
				name:  "ScanRegions",
				qps:   10000,
				burst: 1,
			},
			regionSample: 10000,
		}
	}
}

func (c *scanRegions) Unary(ctx context.Context, cli pd.Client) error {
	upperBound := totalRegion / c.regionSample
	random := rand.Intn(upperBound)
	startID := c.regionSample*random*4 + 1
	endID := c.regionSample*(random+1)*4 + 1
	_, err := cli.ScanRegions(ctx, generateKeyForSimulator(startID, 56), generateKeyForSimulator(endID, 56), c.regionSample)
	if err != nil {
		return err
	}
	return nil
}

type tso struct {
	*baseCase
}

func newTso() func() GRPCCase {
	return func() GRPCCase {
		return &tso{
			baseCase: &baseCase{
				name:  "Tso",
				qps:   10000,
				burst: 1,
			},
		}
	}
}

func (c *tso) Unary(ctx context.Context, cli pd.Client) error {
	_, _, err := cli.GetTS(ctx)
	if err != nil {
		return err
	}
	return nil
}

type getStore struct {
	*baseCase
}

func newGetStore() func() GRPCCase {
	return func() GRPCCase {
		return &getStore{
			baseCase: &baseCase{
				name:  "GetStore",
				qps:   10000,
				burst: 1,
			},
		}
	}
}

func (c *getStore) Unary(ctx context.Context, cli pd.Client) error {
	storeIdx := rand.Intn(totalStore)
	_, err := cli.GetStore(ctx, storesID[storeIdx])
	if err != nil {
		return err
	}
	return nil
}

type getStores struct {
	*baseCase
}

func newGetStores() func() GRPCCase {
	return func() GRPCCase {
		return &getStores{
			baseCase: &baseCase{
				name:  "GetStores",
				qps:   10000,
				burst: 1,
			},
		}
	}
}

func (c *getStores) Unary(ctx context.Context, cli pd.Client) error {
	_, err := cli.GetAllStores(ctx)
	if err != nil {
		return err
	}
	return nil
}

// nolint
func generateKeyForSimulator(id int, keyLen int) []byte {
	k := make([]byte, keyLen)
	copy(k, fmt.Sprintf("%010d", id))
	return k
}
