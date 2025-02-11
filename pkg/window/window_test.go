// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// The MIT License (MIT)
// Copyright (c) 2022 go-kratos Project Authors.

package window

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWindowResetWindow(t *testing.T) {
	re := require.New(t)
	opts := Options{Size: 3}
	window := NewWindow(opts)
	for i := range opts.Size {
		window.Append(i, 1.0)
	}
	window.ResetWindow()
	for i := range opts.Size {
		re.Empty(window.Bucket(i).Points)
	}
}

func TestWindowResetBucket(t *testing.T) {
	re := require.New(t)
	opts := Options{Size: 3}
	window := NewWindow(opts)
	for i := range opts.Size {
		window.Append(i, 1.0)
	}
	window.ResetBucket(1)
	re.Empty(window.Bucket(1).Points)
	re.Equal(float64(1.0), window.Bucket(0).Points[0])
	re.Equal(float64(1.0), window.Bucket(2).Points[0])
}

func TestWindowResetBuckets(t *testing.T) {
	re := require.New(t)
	opts := Options{Size: 3}
	window := NewWindow(opts)
	for i := range opts.Size {
		window.Append(i, 1.0)
	}
	window.ResetBuckets(0, 3)
	for i := range opts.Size {
		re.Empty(window.Bucket(i).Points)
	}
}

func TestWindowAppend(t *testing.T) {
	re := require.New(t)
	opts := Options{Size: 3}
	window := NewWindow(opts)
	for i := range opts.Size {
		window.Append(i, 1.0)
	}
	for i := 1; i < opts.Size; i++ {
		window.Append(i, 2.0)
	}
	for i := range opts.Size {
		re.Equal(float64(1.0), window.Bucket(i).Points[0])
	}
	for i := 1; i < opts.Size; i++ {
		re.Equal(float64(2.0), window.Bucket(i).Points[1])
	}
}

func TestWindowAdd(t *testing.T) {
	re := require.New(t)
	opts := Options{Size: 3}
	window := NewWindow(opts)
	window.Append(0, 1.0)
	window.Add(0, 1.0)
	re.Equal(float64(2.0), window.Bucket(0).Points[0])

	window = NewWindow(opts)
	window.Add(0, 1.0)
	window.Add(0, 1.0)
	re.Equal(float64(2.0), window.Bucket(0).Points[0])
}

func TestWindowSize(t *testing.T) {
	re := require.New(t)
	opts := Options{Size: 3}
	window := NewWindow(opts)
	re.Equal(3, window.Size())
}
