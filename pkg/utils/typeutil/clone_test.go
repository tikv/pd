package typeutil

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

type fate struct {
	ID   uint64
	Attr struct {
		Age int64
	}
}

func (f *fate) Marshal() ([]byte, error) {
	return json.Marshal(f)
}

func (f *fate) Unmarshal(data []byte) error {
	return json.Unmarshal(data, f)
}

var fateFactory = func() *fate { return &fate{} }

func TestDeepClone(t *testing.T) {
	re := assert.New(t)
	src := &fate{ID: 1}
	dst := DeepClone(src, fateFactory)
	re.EqualValues(1, dst.ID)
	dst.ID = 2
	re.EqualValues(1, src.ID)

	// case2: the source is nil
	var src2 *fate
	re.Nil(DeepClone(src2, fateFactory))
}
