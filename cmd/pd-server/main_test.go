package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {
	re := require.New(t)

	output := generateAlias(nil)
	re.Empty(output)
	output = generateAlias([]string{"api", "tso", "resource-manager"})
	re.Len(output, 11)
	for _, v := range output {
		re.Contains(v, "api")
	}
}
