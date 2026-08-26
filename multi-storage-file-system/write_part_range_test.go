// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"math/rand/v2"
	"testing"
)

// referenceFilled recomputes coverage the slow, obvious way, so the incremental
// bookkeeping in addRange has something independent to be checked against.
func referenceFilled(t *testing.T, bounds []writeRange, size uint64) uint64 {
	t.Helper()
	covered := make([]bool, size)
	for _, r := range bounds {
		for offset := r.start; offset < r.end; offset++ {
			covered[offset] = true
		}
	}
	var filled uint64
	for _, set := range covered {
		if set {
			filled++
		}
	}
	return filled
}

// assertInvariant checks what addRange's binary search depends on: ranges are
// sorted, non-empty, and separated by a real gap. Adjacent ranges must have been
// merged, or a later insert could bridge them and double-count.
func assertInvariant(t *testing.T, part *writePart) {
	t.Helper()
	var filled uint64
	for i, r := range part.ranges {
		if r.end <= r.start {
			t.Fatalf("ranges[%d] = %+v is empty or inverted: %v", i, r, part.ranges)
		}
		if i > 0 && r.start <= part.ranges[i-1].end {
			t.Fatalf("ranges[%d] = %+v is unsorted or unmerged against %+v: %v",
				i, r, part.ranges[i-1], part.ranges)
		}
		filled += r.end - r.start
	}
	if filled != part.filled {
		t.Fatalf("filled = %d, ranges sum to %d: %v", part.filled, filled, part.ranges)
	}
}

func TestWritePartAddRange(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		adds   [][2]uint64
		want   []writeRange
		filled uint64
	}{
		{
			name: "single", adds: [][2]uint64{{0, 10}},
			want: []writeRange{{0, 10}}, filled: 10,
		},
		{
			name: "sequential merges to one", adds: [][2]uint64{{0, 10}, {10, 20}, {20, 30}},
			want: []writeRange{{0, 30}}, filled: 30,
		},
		{
			name: "reverse order merges to one", adds: [][2]uint64{{20, 30}, {10, 20}, {0, 10}},
			want: []writeRange{{0, 30}}, filled: 30,
		},
		{
			name: "disjoint stays separate", adds: [][2]uint64{{0, 10}, {20, 30}},
			want: []writeRange{{0, 10}, {20, 30}}, filled: 20,
		},
		{
			// The bridging case: a write landing in a gap must absorb both
			// neighbors into one range rather than leaving three.
			name: "gap filler bridges both neighbors",
			adds: [][2]uint64{{0, 10}, {20, 30}, {10, 20}},
			want: []writeRange{{0, 30}}, filled: 30,
		},
		{
			name: "spanning write absorbs several",
			adds: [][2]uint64{{0, 10}, {20, 30}, {40, 50}, {5, 45}},
			want: []writeRange{{0, 50}}, filled: 50,
		},
		{
			name: "overlap extends", adds: [][2]uint64{{0, 10}, {5, 15}},
			want: []writeRange{{0, 15}}, filled: 15,
		},
		{
			name: "rewrite of the same range is idempotent",
			adds: [][2]uint64{{0, 10}, {0, 10}, {2, 8}},
			want: []writeRange{{0, 10}}, filled: 10,
		},
		{
			name: "insert before everything", adds: [][2]uint64{{20, 30}, {0, 10}},
			want: []writeRange{{0, 10}, {20, 30}}, filled: 20,
		},
		{
			name: "empty range ignored", adds: [][2]uint64{{0, 10}, {5, 5}},
			want: []writeRange{{0, 10}}, filled: 10,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			part := &writePart{}
			for _, add := range testCase.adds {
				part.addRange(add[0], add[1])
				assertInvariant(t, part)
			}
			if len(part.ranges) != len(testCase.want) {
				t.Fatalf("ranges = %v, want %v", part.ranges, testCase.want)
			}
			for i := range part.ranges {
				if part.ranges[i] != testCase.want[i] {
					t.Fatalf("ranges = %v, want %v", part.ranges, testCase.want)
				}
			}
			if part.filled != testCase.filled {
				t.Fatalf("filled = %d, want %d", part.filled, testCase.filled)
			}
		})
	}
}

// Random arrival order is the case the old implementation degraded on, and the
// case where an off-by-one in the merge would go unnoticed by the table above.
func TestWritePartAddRangeRandomOrderMatchesReference(t *testing.T) {
	const (
		size  = 4096
		block = 16
	)

	source := rand.New(rand.NewPCG(20260826, 0))
	for trial := range 20 {
		offsets := source.Perm(size / block)
		part := &writePart{}
		applied := make([]writeRange, 0, len(offsets))

		for _, index := range offsets {
			start := uint64(index * block)
			part.addRange(start, start+block)
			applied = append(applied, writeRange{start: start, end: start + block})
			assertInvariant(t, part)
		}

		if want := referenceFilled(t, applied, size); part.filled != want {
			t.Fatalf("trial %d: filled = %d, reference = %d", trial, part.filled, want)
		}
		// Every byte was written exactly once, so the part must be one range.
		if len(part.ranges) != 1 || part.ranges[0] != (writeRange{0, size}) {
			t.Fatalf("trial %d: a fully written part collapsed to %v", trial, part.ranges)
		}
	}
}

// Partial coverage in random order, checked against the byte-map reference.
func TestWritePartAddRangeSparseRandomMatchesReference(t *testing.T) {
	const size = 2048

	source := rand.New(rand.NewPCG(9276, 0))
	for trial := range 20 {
		part := &writePart{}
		applied := make([]writeRange, 0, 40)
		for range 40 {
			start := uint64(source.IntN(size - 1))
			end := start + 1 + uint64(source.IntN(min(64, size-int(start)-1)))
			part.addRange(start, end)
			applied = append(applied, writeRange{start: start, end: end})
			assertInvariant(t, part)
		}
		if want := referenceFilled(t, applied, size); part.filled != want {
			t.Fatalf("trial %d: filled = %d, reference = %d; ranges %v",
				trial, part.filled, want, part.ranges)
		}
	}
}

// A 5 MiB part written by 4 KiB random writes is the shape that collapsed the
// P5 benchmark family: 1,280 fragments, and the old implementation did three
// O(n) passes per write.
func BenchmarkWritePartAddRangeRandom(b *testing.B) {
	const (
		partSize  = 5 << 20
		blockSize = 4 << 10
		blocks    = partSize / blockSize
	)

	source := rand.New(rand.NewPCG(1, 0))
	order := source.Perm(blocks)

	for b.Loop() {
		part := &writePart{}
		for _, index := range order {
			start := uint64(index * blockSize)
			part.addRange(start, start+blockSize)
		}
	}
}

func BenchmarkWritePartAddRangeSequential(b *testing.B) {
	const (
		partSize  = 5 << 20
		blockSize = 4 << 10
		blocks    = partSize / blockSize
	)

	for b.Loop() {
		part := &writePart{}
		for index := range blocks {
			start := uint64(index * blockSize)
			part.addRange(start, start+blockSize)
		}
	}
}
