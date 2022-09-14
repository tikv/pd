package typeutil

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/assert"
)

func Test_DeepClone(t *testing.T) {
	re := assert.New(t)
	src := &metapb.Region{Id: 1}
	dst := &metapb.Region{}
	DeepClone(src, dst)
	dst.Id = 2
	re.Equal(src.Id, uint64(1))
}

func BenchmarkDeepClone(b *testing.B) {
	for i := 0; i < b.N; i++ {
		src := &metapb.Region{Id: 1}
		dst := &metapb.Region{}
		DeepClone(src, dst)
	}
}

func BenchmarkProtoClone(b *testing.B) {
	for i := 0; i < b.N; i++ {
		src := &metapb.Region{Id: 1}
		dst := proto.Clone(src).(*metapb.Region)
		dst.Id = 1
	}
}
