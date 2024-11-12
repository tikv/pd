package caller

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetComponent(t *testing.T) {
	re := require.New(t)

	re.Equal(Component("github.com/tikv/pd/client/caller"), GetComponent(0))
}

func TestGetCallerID(t *testing.T) {
	re := require.New(t)

	re.Equal(ID("caller.test"), GetCallerID())
}
