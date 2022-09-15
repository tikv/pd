package typeutil

import (
	"github.com/pingcap/kvproto/pkg/pdpb"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/assert"
)

func TestDeepClone(t *testing.T) {
	re := assert.New(t)
	src := &pdpb.StoreHeartbeatRequest{Stats: &pdpb.StoreStats{StoreId: 1}}
	dst := DeepClone(src, func() *pdpb.StoreHeartbeatRequest {
		return &pdpb.StoreHeartbeatRequest{}
	})
	re.EqualValues(dst.Stats.StoreId, 1)
	dst.Stats.StoreId = 2
	re.EqualValues(src.Stats.StoreId, 1)

	// case2: the source is nil
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
