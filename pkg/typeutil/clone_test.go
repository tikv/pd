package typeutil

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/assert"
)

func TestDeepClone(t *testing.T) {
	re := assert.New(t)
	src := &metapb.Region{Id: 1}
	dst := DeepClone(src, RegionFactory)
	dst.Id = 2
	re.Equal(src.Id, uint64(1))
	re.Equal(dst.Id, uint64(2))

	var src2 *metapb.Region
	re.Nil(DeepClone(src2, RegionFactory))
}

func BenchmarkDeepClone(b *testing.B) {
	for i := 0; i < b.N; i++ {
		src := &metapb.Region{Id: 1}
		dst := DeepClone(src, RegionFactory)
		dst.Id = 1
	}
}

func BenchmarkProtoClone(b *testing.B) {
	clone := func(src *metapb.Region) *metapb.Region {
		dst := proto.Clone(src).(*metapb.Region)
		return dst
	}
	for i := 0; i < b.N; i++ {
		src := &metapb.Region{Id: 1}
		dst := clone(src)
		dst.Id = 1
	}
}
