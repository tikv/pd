package endpoint

import (
	"fmt"
	"math/rand"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRegionPath(t *testing.T) {
	re := require.New(t)
	f := func(id uint64) string {
		return path.Join(regionPathPrefix, fmt.Sprintf("%020d", id))
	}
	rand.New(rand.NewSource(time.Now().Unix()))
	for i := 0; i < 1000; i++ {
		id := rand.Uint64()
		re.Equal(f(id), RegionPath(id))
	}
}

func BenchmarkRegionPath(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = RegionPath(uint64(i))
	}
}

func TestExtractKeyspaceGroupIDFromPath(t *testing.T) {
	re := require.New(t)

	rightCases := []struct {
		path string
		id   uint32
	}{
		{path: "/pd/{cluster_id}/tso/keyspace_groups/membership/00000", id: 0},
		{path: "/pd/{cluster_id}/tso/keyspace_groups/membership/00001", id: 1},
		{path: "/pd/{cluster_id}/tso/keyspace_groups/membership/12345", id: 12345},
		{path: "/pd/{cluster_id}/tso/keyspace_groups/membership/99999", id: 99999},
		{path: "tso/keyspace_groups/membership/00000", id: 0},
		{path: "tso/keyspace_groups/membership/00001", id: 1},
		{path: "tso/keyspace_groups/membership/12345", id: 12345},
		{path: "tso/keyspace_groups/membership/99999", id: 99999},
	}

	for _, tt := range rightCases {
		id, err := ExtractKeyspaceGroupIDFromPath(tt.path)
		re.Equal(tt.id, id)
		re.NoError(err)
	}

	wrongCases := []struct {
		path string
	}{
		{path: ""},
		{path: "00001"},
		{path: "xxx/keyspace_groups/membership/00001"},
		{path: "tso/xxxxxxxxxxxxxxx/membership/00001"},
		{path: "tso/keyspace_groups/xxxxxxxxxx/00001"},
		{path: "/pd/{cluster_id}/tso/keyspace_groups/xxxxxxxxxx/00001"},
		{path: "/pd/{cluster_id}/xxx/keyspace_groups/membership/00001"},
		{path: "/pd/{cluster_id}/tso/xxxxxxxxxxxxxxx/membership/00001"},
		{path: "/pd/{cluster_id}/tso/keyspace_groups/membership/"},
		{path: "/pd/{cluster_id}/tso/keyspace_groups/membership/0"},
		{path: "/pd/{cluster_id}/tso/keyspace_groups/membership/0001"},
		{path: "/pd/{cluster_id}/tso/keyspace_groups/membership/123456"},
		{path: "/pd/{cluster_id}/tso/keyspace_groups/membership/1234a"},
		{path: "/pd/{cluster_id}/tso/keyspace_groups/membership/12345a"},
	}

	for _, tt := range wrongCases {
		_, err := ExtractKeyspaceGroupIDFromPath(tt.path)
		re.Error(err)
	}
}
