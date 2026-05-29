package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestManifestPartPath(t *testing.T) {
	tests := []struct {
		manifestDir string
		objectPath  string
		expected    string
	}{
		{"/tmp/manifest", "", "/tmp/manifest/_root.tsv"},
		{"/tmp/manifest", "dir-r0/", "/tmp/manifest/dir-r0.tsv"},
		{"/tmp/manifest", "dir-r0/r0/", "/tmp/manifest/dir-r0/r0.tsv"},
		{"/tmp/manifest", "dir-r0/r0/d5/", "/tmp/manifest/dir-r0/r0/d5.tsv"},
	}
	for _, tt := range tests {
		result := manifestPartPath(tt.manifestDir, tt.objectPath)
		if result != tt.expected {
			t.Errorf("manifestPartPath(%q, %q) = %q, want %q", tt.manifestDir, tt.objectPath, result, tt.expected)
		}
	}
}

func TestReadManifestPart(t *testing.T) {
	tmpDir := t.TempDir()
	partPath := filepath.Join(tmpDir, "test.tsv")

	content := "d\tsubdir1\t0\t-\t2026-03-01T00:00:00Z\n" +
		"f\tfile1.bin\t1024\tabc123\t2026-03-02T00:00:00Z\n" +
		"f\tfile2.txt\t2048\tdef456\t2026-03-03T00:00:00Z\n"

	if err := os.WriteFile(partPath, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	entries, err := readManifestPart(partPath)
	if err != nil {
		t.Fatalf("readManifestPart: %v", err)
	}

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	if entries[0].Kind != "d" || entries[0].Basename != "subdir1" {
		t.Errorf("entry 0: got kind=%q basename=%q", entries[0].Kind, entries[0].Basename)
	}
	if entries[1].Kind != "f" || entries[1].Basename != "file1.bin" || entries[1].Size != 1024 {
		t.Errorf("entry 1: got kind=%q basename=%q size=%d", entries[1].Kind, entries[1].Basename, entries[1].Size)
	}
	if entries[2].ETag != "def456" {
		t.Errorf("entry 2: got etag=%q", entries[2].ETag)
	}
}

func TestReadManifestPartSkipsBlankAndComment(t *testing.T) {
	tmpDir := t.TempDir()
	partPath := filepath.Join(tmpDir, "test.tsv")

	content := "# comment line\n" +
		"\n" +
		"f\tfile1.bin\t100\te1\t2026-01-01T00:00:00Z\n"

	if err := os.WriteFile(partPath, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	entries, err := readManifestPart(partPath)
	if err != nil {
		t.Fatalf("readManifestPart: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
}

func TestReadManifestPartNonexistentFile(t *testing.T) {
	_, err := readManifestPart("/nonexistent/path/test.tsv")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestLookupInManifestPart(t *testing.T) {
	tmpDir := t.TempDir()
	partPath := filepath.Join(tmpDir, "test.tsv")

	content := "d\tsubdir1\t0\t-\t2026-03-01T00:00:00Z\n" +
		"f\tfile1.bin\t1024\tabc123\t2026-03-02T00:00:00Z\n" +
		"f\tfile2.txt\t2048\tdef456\t2026-03-03T00:00:00Z\n"

	if err := os.WriteFile(partPath, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	entry, found := lookupInManifestPart(partPath, "file1.bin")
	if !found {
		t.Fatal("expected to find file1.bin")
	}
	if entry.Kind != "f" || entry.Size != 1024 || entry.ETag != "abc123" {
		t.Errorf("got kind=%q size=%d etag=%q", entry.Kind, entry.Size, entry.ETag)
	}

	entry, found = lookupInManifestPart(partPath, "subdir1")
	if !found {
		t.Fatal("expected to find subdir1")
	}
	if entry.Kind != "d" {
		t.Errorf("subdir1: got kind=%q, want d", entry.Kind)
	}

	_, found = lookupInManifestPart(partPath, "nonexistent")
	if found {
		t.Error("expected not to find nonexistent")
	}
}

func TestLookupInManifestPartNonexistentFile(t *testing.T) {
	_, found := lookupInManifestPart("/nonexistent/path/test.tsv", "foo")
	if found {
		t.Error("expected not to find anything in nonexistent file")
	}
}

func TestWriteManifestIndex(t *testing.T) {
	tmpDir := t.TempDir()

	err := writeManifestIndex(tmpDir, "test-bucket", "data/", 1000, 50)
	if err != nil {
		t.Fatalf("writeManifestIndex: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(tmpDir, "manifest_index.json"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	var index manifestIndex
	if err := json.Unmarshal(data, &index); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if index.Version != 2 {
		t.Errorf("version: got %d, want 2", index.Version)
	}
	if index.Format != "tsv" {
		t.Errorf("format: got %q, want tsv", index.Format)
	}
	if index.Bucket != "test-bucket" {
		t.Errorf("bucket: got %q", index.Bucket)
	}
	if index.Prefix != "data/" {
		t.Errorf("prefix: got %q", index.Prefix)
	}
	if index.TotalObjects != 1000 {
		t.Errorf("total_objects: got %d, want 1000", index.TotalObjects)
	}
	if index.TotalDirectories != 50 {
		t.Errorf("total_directories: got %d, want 50", index.TotalDirectories)
	}
	if index.CreatedAt == "" {
		t.Error("created_at should not be empty")
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input    uint64
		contains string
	}{
		{0, "0 B"},
		{512, "B"},
		{1024, "KiB"},
		{1048576, "MiB"},
		{1073741824, "GiB"},
		{1099511627776, "TiB"},
	}
	for _, tt := range tests {
		result := formatBytes(tt.input)
		if !strings.Contains(result, tt.contains) {
			t.Errorf("formatBytes(%d) = %q, expected to contain %q", tt.input, result, tt.contains)
		}
	}
}

func TestBackendNames(t *testing.T) {
	globals.backendsToMount = map[string]*backendStruct{
		"s3-data":  {},
		"s3-model": {},
	}
	defer func() { globals.backendsToMount = nil }()

	names := backendNames()
	if len(names) != 2 {
		t.Fatalf("expected 2 names, got %d", len(names))
	}

	nameSet := map[string]bool{}
	for _, n := range names {
		nameSet[n] = true
	}
	if !nameSet["s3-data"] || !nameSet["s3-model"] {
		t.Errorf("expected s3-data and s3-model, got %v", names)
	}
}

func TestManifestEntryTSVRoundTrip(t *testing.T) {
	entry := manifestDirEntry{
		Kind:     "f",
		Basename: "test-file.bin",
		Size:     42,
		ETag:     "abc",
	}

	line := entry.Kind + "\t" + entry.Basename + "\t" + strconv.FormatUint(entry.Size, 10) + "\t" + entry.ETag + "\t2025-06-15T12:30:45Z"
	fields := strings.Split(line, "\t")
	if len(fields) != 5 {
		t.Fatalf("expected 5 fields, got %d", len(fields))
	}
	if fields[0] != "f" {
		t.Errorf("expected kind=f, got %q", fields[0])
	}
	if fields[1] != "test-file.bin" {
		t.Errorf("expected basename=test-file.bin, got %q", fields[1])
	}
}

func TestGenerateSubPrefixes(t *testing.T) {
	tests := []struct {
		chars    string
		depth    int
		expected int
	}{
		{"0123456789", 1, 10},
		{"0123456789", 2, 100},
		{"ab", 1, 2},
		{"ab", 2, 4},
		{"ab", 3, 8},
		{"x", 1, 1},
		{"abc", 0, 3},
	}
	for _, tt := range tests {
		result := generateSubPrefixes(tt.chars, tt.depth)
		if len(result) != tt.expected {
			t.Errorf("generateSubPrefixes(%q, %d) = %d prefixes, want %d", tt.chars, tt.depth, len(result), tt.expected)
		}
	}

	prefixes := generateSubPrefixes("01", 2)
	expected := []string{"00", "01", "10", "11"}
	if len(prefixes) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, prefixes)
	}
	for i, p := range prefixes {
		if p != expected[i] {
			t.Errorf("prefix[%d] = %q, want %q", i, p, expected[i])
		}
	}
}

func TestLexInterpolate(t *testing.T) {
	splits := lexInterpolate("a", "z", 5)
	if len(splits) != 4 {
		t.Fatalf("expected 4 split points, got %d: %v", len(splits), splits)
	}

	for i := 1; i < len(splits); i++ {
		if splits[i] <= splits[i-1] {
			t.Errorf("splits not monotonically increasing: splits[%d]=%q <= splits[%d]=%q",
				i, splits[i], i-1, splits[i-1])
		}
	}

	if splits[0] <= "a" {
		t.Errorf("first split %q should be > %q", splits[0], "a")
	}
	if splits[len(splits)-1] >= "z" {
		t.Errorf("last split %q should be < %q", splits[len(splits)-1], "z")
	}
}

func TestLexInterpolateSamePrefix(t *testing.T) {
	splits := lexInterpolate("file-0000001", "file-1000000", 10)
	if len(splits) != 9 {
		t.Fatalf("expected 9 splits, got %d", len(splits))
	}
	for i := 1; i < len(splits); i++ {
		if splits[i] <= splits[i-1] {
			t.Errorf("not monotonic: [%d]=%q <= [%d]=%q", i, splits[i], i-1, splits[i-1])
		}
	}
	if splits[0] <= "file-0000001" {
		t.Errorf("first split %q should be > start", splits[0])
	}
}

func TestLexInterpolateEdgeCases(t *testing.T) {
	splits := lexInterpolate("a", "b", 1)
	if len(splits) != 1 {
		t.Errorf("n=1 should return 1 element, got %d", len(splits))
	}

	splits = lexInterpolate("a", "a", 5)
	if len(splits) != 1 {
		t.Errorf("equal keys should return 1 element, got %d", len(splits))
	}
}
