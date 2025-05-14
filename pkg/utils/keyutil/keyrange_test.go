package keyutil

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCodecKeyRange(t *testing.T) {
	re := require.New(t)

	testCases := []struct {
		ks KeyRange
	}{
		{
			NewKeyRange(fmt.Sprintf("%20d", 0), fmt.Sprintf("%20d", 5)),
		},
		{
			NewKeyRange(fmt.Sprintf("%20d", 0), fmt.Sprintf("%20d", 10)),
		},
	}

	for _, tc := range testCases {
		data, err := tc.ks.MarshalJSON()
		re.NoError(err)
		var ks KeyRange
		re.NoError(ks.UnmarshalJSON(data))
		re.Equal(tc.ks, ks)
	}
}

func TestOverLap(t *testing.T) {
	for _, tc := range []struct {
		name   string
		a, b   KeyRange
		expect bool
	}{
		{
			name:   "overlap",
			a:      NewKeyRange("a", "c"),
			b:      NewKeyRange("b", "d"),
			expect: true,
		},
		{
			name:   "no overlap",
			a:      NewKeyRange("a", "b"),
			b:      NewKeyRange("c", "d"),
			expect: false,
		},
		{
			name:   "continuous",
			a:      NewKeyRange("a", "b"),
			b:      NewKeyRange("b", "d"),
			expect: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			re.Equal(tc.expect, tc.a.OverLapped(&tc.b))
			re.Equal(tc.expect, tc.b.OverLapped(&tc.a))
		})
	}
}

func TestMergeKeyRanges(t *testing.T) {
	re := require.New(t)

	testCases := []struct {
		name   string
		input  []*KeyRange
		expect []*KeyRange
	}{
		{
			name:   "empty",
			input:  []*KeyRange{},
			expect: []*KeyRange{},
		},
		{
			name: "single",
			input: []*KeyRange{
				{StartKey: []byte("a"), EndKey: []byte("b")},
			},
			expect: []*KeyRange{
				{StartKey: []byte("a"), EndKey: []byte("b")},
			},
		},
		{
			name: "non-overlapping",
			input: []*KeyRange{
				{StartKey: []byte("a"), EndKey: []byte("b")},
				{StartKey: []byte("c"), EndKey: []byte("d")},
			},
			expect: []*KeyRange{
				{StartKey: []byte("a"), EndKey: []byte("b")},
				{StartKey: []byte("c"), EndKey: []byte("d")},
			},
		},
		{
			name: "continuous",
			input: []*KeyRange{
				{StartKey: []byte("a"), EndKey: []byte("b")},
				{StartKey: []byte("b"), EndKey: []byte("c")},
			},
			expect: []*KeyRange{
				{StartKey: []byte("a"), EndKey: []byte("c")},
			},
		},
		{
			name: "boundless 1",
			input: []*KeyRange{
				{StartKey: nil, EndKey: []byte("b")},
				{StartKey: []byte("b"), EndKey: []byte("c")},
			},
			expect: []*KeyRange{
				{StartKey: nil, EndKey: []byte("c")},
			},
		},
		{
			name: "boundless 2",
			input: []*KeyRange{
				{StartKey: []byte("a"), EndKey: []byte("b")},
				{StartKey: []byte("b"), EndKey: nil},
			},
			expect: []*KeyRange{
				{StartKey: []byte("a"), EndKey: nil},
			},
		},
	}

	for _, tc := range testCases {
		rs := &KeyRanges{krs: tc.input}
		rs.Merge()
		re.Equal(tc.expect, rs.Ranges(), tc.name)
	}
}
