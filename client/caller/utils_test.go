package caller

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetComponent(t *testing.T) {
	re := require.New(t)

	// Test that GetComponent returns the correct package path
	expectedComponent := "github.com/tikv/pd/client/caller"
	actualComponent := GetComponent(0)
	re.Equal(expectedComponent, actualComponent)
}

func TestGetCallerID(t *testing.T) {
	re := require.New(t)

	// Test that GetCallerID returns the correct process name
	expectedCallerID := "caller.test"
	actualCallerID := GetCallerID()
	re.Equal(expectedCallerID, actualCallerID)
}
