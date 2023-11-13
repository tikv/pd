package realtiup

import (
	"context"
	"os/exec"
	"testing"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
)

func restartTiUP() {
	log.Info("start to restart TiUP")
	cmd := exec.Command("make", "deploy")
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
	log.Info("TiUP restart success")
}

// https://github.com/tikv/pd/issues/6467
func TestReloadLabel(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()

	resp, _ := pdHTTPCli.GetStores(ctx)
	setStore := resp.Stores[0]
	re.Empty(setStore.Store.Labels, nil)
	storeLabel := map[string]string{
		"zone": "zone1",
	}
	err := pdHTTPCli.SetStoreLabel(ctx, setStore.Store.ID, storeLabel)
	re.NoError(err)

	resp, err = pdHTTPCli.GetStores(ctx)
	re.NoError(err)
	for _, store := range resp.Stores {
		if store.Store.ID == setStore.Store.ID {
			for _, label := range store.Store.Labels {
				re.Equal(label.Value, storeLabel[label.Key])
			}
		}
	}

	restartTiUP()

	resp, err = pdHTTPCli.GetStores(ctx)
	re.NoError(err)
	for _, store := range resp.Stores {
		if store.Store.ID == setStore.Store.ID {
			for _, label := range store.Store.Labels {
				re.Equal(label.Value, storeLabel[label.Key])
			}
		}
	}
}
