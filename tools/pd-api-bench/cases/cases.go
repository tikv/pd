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
	"math/rand"
	"strconv"
	"time"

	"github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	pdHttp "github.com/tikv/pd/client/http"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
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
	log.Info("init cluster info", zap.Int("total-region", totalRegion), zap.Int("total-store", totalStore), zap.Any("store-ids", storesID))
	return nil
}

// Config is the configuration for the case.
type Config struct {
	QPS   int64 `toml:"qps" json:"qps"`
	Burst int64 `toml:"burst" json:"burst"`
}

func newConfig() *Config {
	return &Config{
		Burst: 1,
	}
}

// Clone returns a cloned configuration.
func (c *Config) Clone() *Config {
	cfg := *c
	return &cfg
}

// Case is the interface for all cases.
type Case interface {
	Name() string
	SetQPS(int64)
	GetQPS() int64
	SetBurst(int64)
	GetBurst() int64
	GetConfig() *Config
}

type baseCase struct {
	name string
	cfg  *Config
}

func (c *baseCase) Name() string {
	return c.name
}

func (c *baseCase) SetQPS(qps int64) {
	c.cfg.QPS = qps
}

func (c *baseCase) GetQPS() int64 {
	return c.cfg.QPS
}

func (c *baseCase) SetBurst(burst int64) {
	c.cfg.Burst = burst
}

func (c *baseCase) GetBurst() int64 {
	return c.cfg.Burst
}

func (c *baseCase) GetConfig() *Config {
	return c.cfg.Clone()
}

// ETCDCase is the interface for all etcd api cases.
type ETCDCase interface {
	Case
	Init(context.Context, *clientv3.Client) error
	Unary(context.Context, *clientv3.Client) error
}

// ETCDCraeteFn is function type to create ETCDCase.
type ETCDCraeteFn func() ETCDCase

// ETCDCaseFnMap is the map for all ETCD case creation function.
var ETCDCaseFnMap = map[string]ETCDCraeteFn{
	"Get":    newGetKV(),
	"Put":    newPutKV(),
	"Delete": newDeleteKV(),
	"Txn":    newTxnKV(),
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
	"GetRegion":                newGetRegion(),
	"GetRegionEnableFollower":  newGetRegionEnableFollower(),
	"GetStore":                 newGetStore(),
	"GetStores":                newGetStores(),
	"ScanRegions":              newScanRegions(),
	"Tso":                      newTso(),
	"UpdateGCSafePoint":        newUpdateGCSafePoint(),
	"UpdateServiceGCSafePoint": newUpdateServiceGCSafePoint(),
}

// HTTPCase is the interface for all HTTP cases.
type HTTPCase interface {
	Case
	Do(context.Context, pdHttp.Client) error
}

// HTTPCraeteFn is function type to create HTTPCase.
type HTTPCraeteFn func() HTTPCase

// HTTPCaseFnMap is the map for all HTTP case creation function.
var HTTPCaseFnMap = map[string]HTTPCraeteFn{
	"GetRegionStatus":  newRegionStats(),
	"GetMinResolvedTS": newMinResolvedTS(),
}

type minResolvedTS struct {
	*baseCase
}

func newMinResolvedTS() func() HTTPCase {
	return func() HTTPCase {
		return &minResolvedTS{
			baseCase: &baseCase{
				name: "GetMinResolvedTS",
				cfg:  newConfig(),
			},
		}
	}
}

func (c *minResolvedTS) Do(ctx context.Context, cli pdHttp.Client) error {
	minResolvedTS, storesMinResolvedTS, err := cli.GetMinResolvedTSByStoresIDs(ctx, storesID)
	if Debug {
		log.Info("do HTTP case", zap.String("case", c.name), zap.Uint64("min-resolved-ts", minResolvedTS), zap.Any("store-min-resolved-ts", storesMinResolvedTS), zap.Error(err))
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
				name: "GetRegionStatus",
				cfg:  newConfig(),
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
		log.Info("do HTTP case", zap.String("case", c.name), zap.Any("region-stats", regionStats), zap.Error(err))
	}
	if err != nil {
		return err
	}
	return nil
}

type updateGCSafePoint struct {
	*baseCase
}

func newUpdateGCSafePoint() func() GRPCCase {
	return func() GRPCCase {
		return &updateGCSafePoint{
			baseCase: &baseCase{
				name: "UpdateGCSafePoint",
				cfg:  newConfig(),
			},
		}
	}
}

func (c *updateGCSafePoint) Unary(ctx context.Context, cli pd.Client) error {
	s := time.Now().Unix()
	_, err := cli.UpdateGCSafePoint(ctx, uint64(s))
	if err != nil {
		return err
	}
	return nil
}

type updateServiceGCSafePoint struct {
	*baseCase
}

func newUpdateServiceGCSafePoint() func() GRPCCase {
	return func() GRPCCase {
		return &updateServiceGCSafePoint{
			baseCase: &baseCase{
				name: "UpdateServiceGCSafePoint",
				cfg:  newConfig(),
			},
		}
	}
}

func (c *updateServiceGCSafePoint) Unary(ctx context.Context, cli pd.Client) error {
	s := time.Now().Unix()
	id := rand.Int63n(100) + 1
	_, err := cli.UpdateServiceGCSafePoint(ctx, strconv.FormatInt(id, 10), id, uint64(s))
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
				name: "GetRegion",
				cfg:  newConfig(),
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

type getRegionEnableFollower struct {
	*baseCase
}

func newGetRegionEnableFollower() func() GRPCCase {
	return func() GRPCCase {
		return &getRegionEnableFollower{
			baseCase: &baseCase{
				name: "GetRegionEnableFollower",
				cfg:  newConfig(),
			},
		}
	}
}

func (c *getRegionEnableFollower) Unary(ctx context.Context, cli pd.Client) error {
	id := rand.Intn(totalRegion)*4 + 1
	_, err := cli.GetRegion(ctx, generateKeyForSimulator(id, 56), pd.WithAllowFollowerHandle())
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
				name: "ScanRegions",
				cfg:  newConfig(),
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
				name: "Tso",
				cfg:  newConfig(),
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
				name: "GetStore",
				cfg:  newConfig(),
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
				name: "GetStores",
				cfg:  newConfig(),
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

type getKV struct {
	*baseCase
}

func newGetKV() func() ETCDCase {
	return func() ETCDCase {
		return &getKV{
			baseCase: &baseCase{
				name: "Get",
				cfg:  newConfig(),
			},
		}
	}
}

func (c *getKV) Init(ctx context.Context, cli *clientv3.Client) error {
	for i := 0; i < 100; i++ {
		_, err := cli.Put(ctx, fmt.Sprintf("/test/0001/%4d", i), fmt.Sprintf("%4d", i))
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *getKV) Unary(ctx context.Context, cli *clientv3.Client) error {
	_, err := cli.Get(ctx, "/test/0001", clientv3.WithPrefix())
	return err
}

type putKV struct {
	*baseCase
}

func newPutKV() func() ETCDCase {
	return func() ETCDCase {
		return &putKV{
			baseCase: &baseCase{
				name: "Put",
				cfg:  newConfig(),
			},
		}
	}
}

func (c *putKV) Init(ctx context.Context, cli *clientv3.Client) error { return nil }

func (c *putKV) Unary(ctx context.Context, cli *clientv3.Client) error {
	_, err := cli.Put(ctx, "/test/0001/0000", "test")
	return err
}

type deleteKV struct {
	*baseCase
}

func newDeleteKV() func() ETCDCase {
	return func() ETCDCase {
		return &deleteKV{
			baseCase: &baseCase{
				name: "Put",
				cfg:  newConfig(),
			},
		}
	}
}

func (c *deleteKV) Init(ctx context.Context, cli *clientv3.Client) error { return nil }

func (c *deleteKV) Unary(ctx context.Context, cli *clientv3.Client) error {
	_, err := cli.Delete(ctx, "/test/0001/0000")
	return err
}

type txnKV struct {
	*baseCase
}

func newTxnKV() func() ETCDCase {
	return func() ETCDCase {
		return &txnKV{
			baseCase: &baseCase{
				name: "Put",
				cfg:  newConfig(),
			},
		}
	}
}

func (c *txnKV) Init(ctx context.Context, cli *clientv3.Client) error { return nil }

func (c *txnKV) Unary(ctx context.Context, cli *clientv3.Client) error {
	txn := cli.Txn(ctx)
	txn = txn.If(clientv3.Compare(clientv3.Value("/test/0001/0000"), "=", "test"))
	txn = txn.Then(clientv3.OpPut("/test/0001/0000", "test2"))
	_, err := txn.Commit()
	return err
}
