package typeutil

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/assert"
)

func Test_DeepClone(t *testing.T) {
	src := &metapb.Region{Id: 1}
	dst := DeepClone(src)
	dst.Id = 2
	assert.Equal(t, src.Id, uint64(1))
}
