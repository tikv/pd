package tests

import (
	stderrors "errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWaitServerRunResultsReturnsFastOnAnyError(t *testing.T) {
	re := require.New(t)
	blocked := make(chan error)
	fail := make(chan error, 1)
	fail <- stderrors.New("boom")

	start := time.Now()
	err := waitServerRunResults([]<-chan error{blocked, fail})
	re.Error(err)
	re.ErrorContains(err, "boom")
	re.Less(time.Since(start), 200*time.Millisecond)
}

func TestWaitServerRunResultsAllSuccess(t *testing.T) {
	re := require.New(t)
	c1 := make(chan error, 1)
	c2 := make(chan error, 1)
	c1 <- nil
	c2 <- nil

	re.NoError(waitServerRunResults([]<-chan error{c1, c2}))
}
