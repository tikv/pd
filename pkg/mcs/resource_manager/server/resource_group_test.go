package server

import (
	"encoding/json"
	"testing"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/stretchr/testify/require"
)

func TestPatchResourceGroup(t *testing.T) {
	re := require.New(t)
	rg1 := &ResourceGroup{Name: "test", Mode: rmpb.GroupMode_RUMode}
	err := rg1.CheckAndInit()
	re.NoError(err)
	testCaseRU := []struct {
		patchJSONString  string
		expectJSONString string
	}{
		{`{"name":"test", "mode":0, "r_u_settings": {"r_r_u":{"settings":{"fill_rate": 200000}}}}`,
			`{"name":"test","mode":0,"r_u_settings":{"rru":{"token_bucket":{"settings":{"fill_rate":200000}},"initialized":false},"wru":{"initialized":false}}}`},
		{`{"name":"test", "mode":0, "r_u_settings": {"w_r_u":{"settings":{"fill_rate": 200000}}}}`,
			`{"name":"test","mode":0,"r_u_settings":{"rru":{"initialized":false},"wru":{"token_bucket":{"settings":{"fill_rate":200000}},"initialized":false}}}`},
		{`{"name":"test", "mode":0, "r_u_settings": {"w_r_u":{"settings":{"fill_rate": 200000, "burst": 100000}}}}`,
			`{"name":"test","mode":0,"r_u_settings":{"rru":{"initialized":false},"wru":{"token_bucket":{"settings":{"fill_rate":200000}},"initialized":false}}}`},
		{`{"name":"test", "mode":0, "r_u_settings": {"r_r_u":{"settings":{"fill_rate": 200000, "burst": 100000}}}}`,
			`{"name":"test","mode":0,"r_u_settings":{"rru":{"token_bucket":{"settings":{"fill_rate":200000}},"initialized":false},"wru":{"initialized":false}}}`},
		{`{"name":"test", "mode":0, "r_u_settings": {"r_r_u":{"settings":{"fill_rate": 200000, "burst": 100000}}, "w_r_u":{"settings":{"fill_rate": 200000}}}}`,
			`{"name":"test","mode":0,"r_u_settings":{"rru":{"token_bucket":{"settings":{"fill_rate":200000}},"initialized":false},"wru":{"token_bucket":{"settings":{"fill_rate":200000}},"initialized":false}}}`},
	}

	for _, ca := range testCaseRU {
		rg := rg1.Copy()
		patch := &rmpb.ResourceGroup{}
		err := json.Unmarshal([]byte(ca.patchJSONString), patch)
		re.NoError(err)
		err = rg.PatchSettings(patch)
		re.NoError(err)
		res, err := json.Marshal(rg)
		re.NoError(err)
		re.Equal(ca.expectJSONString, string(res))
	}

	rg2 := &ResourceGroup{Name: "test", Mode: rmpb.GroupMode_NativeMode}
	err = rg2.CheckAndInit()
	re.NoError(err)
	testCaseResource := []struct {
		patchJSONString  string
		expectJSONString string
	}{
		{`{"name":"test", "mode":1, "resource_settings": {"cpu":{"settings":{"fill_rate": 200000}}}}`,
			`{"name":"test","mode":1,"resource_settings":{"cpu":{"token_bucket":{"settings":{"fill_rate":200000}},"initialized":false},"io_read_bandwidth":{"initialized":false},"io_write_bandwidth":{"initialized":false}}}`},
		{`{"name":"test", "mode":1, "resource_settings": {"io_read":{"settings":{"fill_rate": 200000}}}}`,
			`{"name":"test","mode":1,"resource_settings":{"cpu":{"initialized":false},"io_read_bandwidth":{"token_bucket":{"settings":{"fill_rate":200000}},"initialized":false},"io_write_bandwidth":{"initialized":false}}}`},
		{`{"name":"test", "mode":1, "resource_settings": {"io_write":{"settings":{"fill_rate": 200000}}}}`,
			`{"name":"test","mode":1,"resource_settings":{"cpu":{"initialized":false},"io_read_bandwidth":{"initialized":false},"io_write_bandwidth":{"token_bucket":{"settings":{"fill_rate":200000}},"initialized":false}}}`},
	}

	for _, ca := range testCaseResource {
		rg := rg2.Copy()
		patch := &rmpb.ResourceGroup{}
		err := json.Unmarshal([]byte(ca.patchJSONString), patch)
		re.NoError(err)
		err = rg.PatchSettings(patch)
		re.NoError(err)
		res, err := json.Marshal(rg)
		re.NoError(err)
		re.Equal(ca.expectJSONString, string(res))
	}
}
