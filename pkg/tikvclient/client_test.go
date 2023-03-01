package tikvclient

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func TestGetConn(t *testing.T) {
	re := require.New(t)

	ctx := context.Background()
	client := NewRPCClient()

	conn1, err := client.getClientConn(ctx, "http://127.0.0.1:1234")
	re.NoError(err)
	conn2, err := client.getClientConn(ctx, "http://127.0.0.1:5678")
	re.NoError(err)
	re.False(conn1.Get() == conn2.Get())

	conn3, err := client.getClientConn(ctx, "http://127.0.0.1:1234")
	re.NoError(err)
	conn4, err := client.getClientConn(ctx, "http://127.0.0.1:5678")
	re.NoError(err)
	re.False(conn3.Get() == conn4.Get())

	re.True(conn1.Get() == conn3.Get())
	re.True(conn2.Get() == conn4.Get())
}

func TestGetConnAfterClose(t *testing.T) {
	re := require.New(t)

	ctx := context.Background()
	client := NewRPCClient()

	conn1, err := client.getClientConn(ctx, "http://127.0.0.1:1234")
	re.NoError(err)
	err = conn1.Get().Close()
	re.NoError(err)

	conn2, err := client.getClientConn(ctx, "http://127.0.0.1:1234")
	re.NoError(err)
	re.False(conn1.Get() == conn2.Get())

	conn3, err := client.getClientConn(ctx, "http://127.0.0.1:1234")
	re.NoError(err)
	re.True(conn2.Get() == conn3.Get())
}
