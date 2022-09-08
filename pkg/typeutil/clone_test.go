package typeutil

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/assert"
)

func Test_DeepClone(t *testing.T) {
	re := assert.New(t)
	src := &metapb.Region{Id: 1}
	dst := DeepClone(src)
	dst.Id = 2
	re.Equal(src.Id, uint64(1))
	src2 := func() *metapb.Region {
		return nil
	}()
	dst2 := DeepClone(src2)
	re.Nil(dst2)
}
