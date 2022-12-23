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
		{`{"mode":0, "r_u_settings": {"r_r_u":{"settings":{"fillrate": 200000}}}}`,
			`{"name":"test","mode":0,"r_u_settings":{"rru":{"token_bucket":{"settings":{"fillrate":200000}},"initialized":false},"wru":{"initialized":false}}}`},
		{`{"mode":0, "r_u_settings": {"w_r_u":{"settings":{"fillrate": 200000}}}}`,
			`{"name":"test","mode":0,"r_u_settings":{"rru":{"initialized":false},"wru":{"token_bucket":{"settings":{"fillrate":200000}},"initialized":false}}}`},
		{`{"mode":0, "r_u_settings": {"w_r_u":{"settings":{"fillrate": 200000, "burst": 100000}}}}`,
			`{"name":"test","mode":0,"r_u_settings":{"rru":{"initialized":false},"wru":{"token_bucket":{"settings":{"fillrate":200000}},"initialized":false}}}`},
		{`{"mode":0, "r_u_settings": {"r_r_u":{"settings":{"fillrate": 200000, "burst": 100000}}}}`,
			`{"name":"test","mode":0,"r_u_settings":{"rru":{"token_bucket":{"settings":{"fillrate":200000}},"initialized":false},"wru":{"initialized":false}}}`},
		{`{"mode":0, "r_u_settings": {"r_r_u":{"settings":{"fillrate": 200000, "burst": 100000}}, "w_r_u":{"settings":{"fillrate": 200000}}}}`,
			`{"name":"test","mode":0,"r_u_settings":{"rru":{"token_bucket":{"settings":{"fillrate":200000}},"initialized":false},"wru":{"token_bucket":{"settings":{"fillrate":200000}},"initialized":false}}}`},
	}

	for _, ca := range testCaseRU {
		rg := rg1.Copy()
		patch := &rmpb.GroupSettings{}
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
		{`{"mode":1, "resource_settings": {"cpu":{"settings":{"fillrate": 200000}}}}`,
			`{"name":"test","mode":1,"resource_settings":{"cpu":{"token_bucket":{"settings":{"fillrate":200000}},"initialized":false},"io_read_bandwidth":{"initialized":false},"io_write_bandwidth":{"initialized":false}}}`},
		{`{"mode":1, "resource_settings": {"io_read":{"settings":{"fillrate": 200000}}}}`,
			`{"name":"test","mode":1,"resource_settings":{"cpu":{"initialized":false},"io_read_bandwidth":{"token_bucket":{"settings":{"fillrate":200000}},"initialized":false},"io_write_bandwidth":{"initialized":false}}}`},
		{`{"mode":1, "resource_settings": {"io_write":{"settings":{"fillrate": 200000}}}}`,
			`{"name":"test","mode":1,"resource_settings":{"cpu":{"initialized":false},"io_read_bandwidth":{"initialized":false},"io_write_bandwidth":{"token_bucket":{"settings":{"fillrate":200000}},"initialized":false}}}`},
	}

	for _, ca := range testCaseResource {
		rg := rg2.Copy()
		patch := &rmpb.GroupSettings{}
		err := json.Unmarshal([]byte(ca.patchJSONString), patch)
		re.NoError(err)
		err = rg.PatchSettings(patch)
		re.NoError(err)
		res, err := json.Marshal(rg)
		re.NoError(err)
		re.Equal(ca.expectJSONString, string(res))
	}
}
