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
