package main

import (
	"testing"
)

func dirtySegments(bounds ...[2]uint64) []writeSegment {
	segments := make([]writeSegment, 0, len(bounds))
	for _, bound := range bounds {
		segments = append(segments, writeSegment{
			offset: bound[0],
			data:   make([]byte, bound[1]-bound[0]),
		})
	}
	return segments
}

// Segments arrive one per FUSE write in arbitrary order and may overlap, so the
// merge has to normalize them before any coverage question can be asked.
func TestMergedDirtyRanges(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		segments []writeSegment
		want     []writeRange
	}{
		{name: "no segments", segments: nil, want: nil},
		{
			name:     "single segment",
			segments: dirtySegments([2]uint64{0, 100}),
			want:     []writeRange{{start: 0, end: 100}},
		},
		{
			name:     "out of order segments are sorted",
			segments: dirtySegments([2]uint64{200, 300}, [2]uint64{0, 100}),
			want:     []writeRange{{start: 0, end: 100}, {start: 200, end: 300}},
		},
		{
			// The sequential-rewrite case: adjacent writes must collapse to one
			// range, otherwise the per-part coverage test degrades to a scan.
			name:     "adjacent segments merge",
			segments: dirtySegments([2]uint64{0, 100}, [2]uint64{100, 200}, [2]uint64{200, 300}),
			want:     []writeRange{{start: 0, end: 300}},
		},
		{
			name:     "overlapping segments merge",
			segments: dirtySegments([2]uint64{0, 150}, [2]uint64{100, 200}),
			want:     []writeRange{{start: 0, end: 200}},
		},
		{
			name:     "a contained segment does not shrink the range",
			segments: dirtySegments([2]uint64{0, 500}, [2]uint64{100, 200}),
			want:     []writeRange{{start: 0, end: 500}},
		},
		{
			name:     "empty segments are ignored",
			segments: dirtySegments([2]uint64{0, 0}, [2]uint64{100, 200}),
			want:     []writeRange{{start: 100, end: 200}},
		},
		{
			name:     "a gap is preserved",
			segments: dirtySegments([2]uint64{0, 100}, [2]uint64{101, 200}),
			want:     []writeRange{{start: 0, end: 100}, {start: 101, end: 200}},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			state := writeState{segments: testCase.segments}
			got := state.mergedDirtyRanges()
			if len(got) != len(testCase.want) {
				t.Fatalf("mergedDirtyRanges() = %v, expected %v", got, testCase.want)
			}
			for index := range got {
				if got[index] != testCase.want[index] {
					t.Fatalf("mergedDirtyRanges() = %v, expected %v", got, testCase.want)
				}
			}
		})
	}
}

// A false positive here means zero-filling bytes the base object owned, so the
// predicate has to fail closed on every partial case.
func TestRangesCover(t *testing.T) {
	fullyDirty := []writeRange{{start: 0, end: 1000}}
	twoRuns := []writeRange{{start: 0, end: 100}, {start: 200, end: 400}}

	for _, testCase := range []struct {
		name   string
		ranges []writeRange
		offset uint64
		length uint64
		want   bool
	}{
		{name: "no ranges cover nothing", ranges: nil, offset: 0, length: 10, want: false},
		{name: "exact cover", ranges: fullyDirty, offset: 0, length: 1000, want: true},
		{name: "interior cover", ranges: fullyDirty, offset: 400, length: 100, want: true},
		{name: "runs past the end", ranges: fullyDirty, offset: 900, length: 200, want: false},
		{name: "starts before the range", ranges: twoRuns, offset: 0, length: 150, want: false},
		{name: "spans a gap", ranges: twoRuns, offset: 50, length: 300, want: false},
		{name: "inside the second run", ranges: twoRuns, offset: 250, length: 100, want: true},
		{name: "begins in a gap", ranges: twoRuns, offset: 150, length: 10, want: false},
		{name: "zero length is vacuously covered", ranges: nil, offset: 0, length: 0, want: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got := rangesCover(testCase.ranges, testCase.offset, testCase.length)
			if got != testCase.want {
				t.Fatalf("rangesCover(%v, %d, %d) = %v, expected %v",
					testCase.ranges, testCase.offset, testCase.length, got, testCase.want)
			}
		})
	}
}

// The case the change exists for: a whole-object rewrite arrives as one segment
// per FUSE write, and every part of it must come back covered so no part issues
// a base read it would discard.
func TestSequentialRewriteCoversEveryPart(t *testing.T) {
	const (
		objectSize = 64 << 20
		writeSize  = 128 << 10
		partSize   = 5 << 20
	)

	segments := make([]writeSegment, 0, objectSize/writeSize)
	for offset := uint64(0); offset < objectSize; offset += writeSize {
		segments = append(segments, writeSegment{offset: offset, data: make([]byte, writeSize)})
	}
	state := writeState{segments: segments}

	ranges := state.mergedDirtyRanges()
	if len(ranges) != 1 {
		t.Fatalf("a sequential rewrite merged to %d ranges, expected 1", len(ranges))
	}

	for offset := uint64(0); offset < objectSize; offset += partSize {
		length := uint64(partSize)
		if offset+length > objectSize {
			length = objectSize - offset
		}
		if !rangesCover(ranges, offset, length) {
			t.Fatalf("part [%d,%d) reported uncovered during a full rewrite", offset, offset+length)
		}
	}

	// One hole is enough to force the base read back for the part containing it.
	holed := make([]writeSegment, 0, len(segments)-1)
	holed = append(holed, segments[:8]...)
	holed = append(holed, segments[9:]...)
	state.segments = holed
	ranges = state.mergedDirtyRanges()
	if rangesCover(ranges, 0, partSize) {
		t.Fatalf("a part with an unwritten hole reported covered")
	}
}
