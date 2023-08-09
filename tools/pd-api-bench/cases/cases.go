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
	"net/http"
	"net/url"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/utils/apiutil"
)

var PDAddress string

var totalRegion int
var totalStore int
var storesID []uint64

func InitCluster(ctx context.Context, cli pd.Client, httpClit *http.Client) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
		PDAddress+"/pd/api/v1/stats/region?start_key=&end_key=&count", nil)
	resp, err := httpClit.Do(req)
	if err != nil {
		return err
	}
	statsResp := &statistics.RegionStats{}
	err = apiutil.ReadJSON(resp.Body, statsResp)
	if err != nil {
		return err
	}
	resp.Body.Close()
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

type Case interface {
	Name() string
	SetQPS(int)
	GetQPS() int
	SetBurst(int)
	GetBurst() int
}

type baseCase struct {
	name  string
	qps   int
	burst int
}

func (c *baseCase) Name() string {
	return c.name
}

func (c *baseCase) SetQPS(qps int) {
	c.qps = qps
}

func (c *baseCase) GetQPS() int {
	return c.qps
}

func (c *baseCase) SetBurst(burst int) {
	c.burst = burst
}

func (c *baseCase) GetBurst() int {
	return c.burst
}

type GRPCCase interface {
	Case
	Unary(context.Context, pd.Client) error
}

var GRPCCaseMap = map[string]GRPCCase{
	"GetRegion":   newGetRegion(),
	"GetStore":    newGetStore(),
	"GetStores":   newGetStores(),
	"ScanRegions": newScanRegions(),
}

type HTTPCase interface {
	Case
	Do(context.Context, *http.Client) error
}

var HTTPCaseMap = map[string]HTTPCase{
	"GetRegionStatus":  newRegionStats(),
	"GetMinResolvedTS": newMinResolvedTS(),
}

type MinResolvedTS struct {
	*baseCase
}

func newMinResolvedTS() *MinResolvedTS {
	return &MinResolvedTS{
		baseCase: &baseCase{
			name:  "GetMinResolvedTS",
			qps:   1000,
			burst: 1,
		},
	}
}

func (c *MinResolvedTS) Do(ctx context.Context, cli *http.Client) error {
	storeIdx := rand.Intn(int(totalStore))
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, PDAddress+fmt.Sprintf("pd/api/v1/min-resolved-ts/%d", storesID[storeIdx]), nil)
	res, err := cli.Do(req)
	if err != nil {
		return err
	}
	res.Body.Close()
	return nil
}

type RegionsStats struct {
	*baseCase
	regionSample int
}

func newRegionStats() *RegionsStats {
	return &RegionsStats{
		baseCase: &baseCase{
			name:  "GetRegionStatus",
			qps:   100,
			burst: 1,
		},
		regionSample: 1000,
	}
}

func (c *RegionsStats) Do(ctx context.Context, cli *http.Client) error {
	upperBound := int(totalRegion) / c.regionSample
	if upperBound < 1 {
		upperBound = 1
	}
	random := rand.Intn(upperBound)
	startID := c.regionSample*random*4 + 1
	endID := c.regionSample*(random+1)*4 + 1
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, PDAddress+fmt.Sprintf("/pd/api/v1/stats/region?start_key=%s&end_key=%s&%s",
		url.QueryEscape(string(generateKeyForSimulator(startID, 56))),
		url.QueryEscape(string(generateKeyForSimulator(endID, 56))),
		"",
	), nil)
	res, err := cli.Do(req)
	if err != nil {
		return err
	}
	res.Body.Close()
	return nil
}

type GetRegion struct {
	*baseCase
}

func newGetRegion() *GetRegion {
	return &GetRegion{
		baseCase: &baseCase{
			name:  "GetRegion",
			qps:   10000,
			burst: 1,
		},
	}
}

func (c *GetRegion) Unary(ctx context.Context, cli pd.Client) error {
	id := rand.Intn(int(totalRegion))*4 + 1
	for i := 0; i < c.burst; i++ {
		_, err := cli.GetRegion(ctx, generateKeyForSimulator(id, 56))
		if err != nil {
			return err
		}
	}
	return nil
}

type ScanRegions struct {
	*baseCase
	regionSample int
}

func newScanRegions() *ScanRegions {
	return &ScanRegions{
		baseCase: &baseCase{
			name:  "ScanRegions",
			qps:   10000,
			burst: 1,
		},
		regionSample: 10000,
	}
}

func (c *ScanRegions) Unary(ctx context.Context, cli pd.Client) error {
	upperBound := int(totalRegion) / c.regionSample
	random := rand.Intn(upperBound)
	startID := c.regionSample*random*4 + 1
	endID := c.regionSample*(random+1)*4 + 1
	for i := 0; i < c.burst; i++ {
		_, err := cli.ScanRegions(ctx, generateKeyForSimulator(startID, 56), generateKeyForSimulator(endID, 56), c.regionSample)
		if err != nil {
			return err
		}
	}
	return nil
}

type GetStore struct {
	*baseCase
}

func newGetStore() *GetStore {
	return &GetStore{
		baseCase: &baseCase{
			name:  "GetStore",
			qps:   10000,
			burst: 1,
		},
	}
}

func (c *GetStore) Unary(ctx context.Context, cli pd.Client) error {
	storeIdx := rand.Intn(int(totalStore))
	for i := 0; i < c.burst; i++ {
		_, err := cli.GetStore(ctx, storesID[storeIdx])
		if err != nil {
			return err
		}
	}
	return nil
}

type GetStores struct {
	*baseCase
}

func newGetStores() *GetStores {
	return &GetStores{
		baseCase: &baseCase{
			name:  "GetStores",
			qps:   10000,
			burst: 1,
		},
	}
}

func (c *GetStores) Unary(ctx context.Context, cli pd.Client) error {
	for i := 0; i < c.burst; i++ {
		_, err := cli.GetAllStores(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func generateKeyForSimulator(id int, keyLen int) []byte {
	k := make([]byte, keyLen)
	copy(k, fmt.Sprintf("%010d", id))
	return k
}
