package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/stretchr/testify/require"
)

func TestPatchResourceGroup(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rg := &ResourceGroup{Name: "test", Mode: rmpb.GroupMode_RUMode, RUSettings: NewRequestUnitSettings(ctx, nil)}
	testCaseRU := []struct {
		patchJSONString  string
		expectJSONString string
	}{
		{`{"name":"test", "mode":1, "r_u_settings": {"r_u":{"settings":{"fill_rate": 200000}}}}`,
			`{"name":"test","mode":1,"r_u_settings":{"ru":{"settings":{"fill_rate":200000},"state":{"initialized":false}}}}`},
		{`{"name":"test", "mode":1, "r_u_settings": {"r_u":{"settings":{"fill_rate": 200000, "burst_limit": -1}}}}`,
			`{"name":"test","mode":1,"r_u_settings":{"ru":{"settings":{"fill_rate":200000,"burst_limit":-1},"state":{"initialized":false}}}}`},
	}

	for _, ca := range testCaseRU {
		// rg := rg1.Copy()
		patch := &rmpb.ResourceGroup{}
		err := json.Unmarshal([]byte(ca.patchJSONString), patch)
		re.NoError(err)
		err = rg.PatchSettings(patch)
		re.NoError(err)
		time.Sleep(100 * time.Millisecond)
		res, err := json.Marshal(rg)
		re.NoError(err)
		re.Equal(ca.expectJSONString, string(res))
	}
}
