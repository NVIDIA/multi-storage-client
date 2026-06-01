package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseTSVLine(t *testing.T) {
	line := "dir-r0/d5/file.bin\t1024\tabc123\t2025-01-15T10:30:00Z"
	fields := strings.SplitN(line, "\t", 4)
	if len(fields) != 4 {
		t.Fatalf("expected 4 fields, got %d", len(fields))
	}
	if fields[0] != "dir-r0/d5/file.bin" {
		t.Errorf("path: got %q", fields[0])
	}
	if fields[1] != "1024" {
		t.Errorf("size: got %q", fields[1])
	}
	if fields[2] != "abc123" {
		t.Errorf("etag: got %q", fields[2])
	}
	if fields[3] != "2025-01-15T10:30:00Z" {
		t.Errorf("mtime: got %q", fields[3])
	}
}

func TestParseTSVSkipsCommentHeader(t *testing.T) {
	tmpDir := t.TempDir()
	manifestPath := filepath.Join(tmpDir, "manifest.tsv")

	content := "# version:1\tbucket:test\tprefix:data/\ttotal_objects:2\tgenerated_at:2026-03-19T00:00:00Z\n" +
		"a/file1.bin\t100\te1\t2025-01-01T00:00:00Z\n" +
		"a/file2.bin\t200\te2\t2025-01-02T00:00:00Z\n"

	if err := os.WriteFile(manifestPath, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	f, err := os.Open(manifestPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	var stats ingestStats
	batchCh := make(chan *dirBatch, 64)
	go ingestReader(f, batchCh, &stats)

	var totalEntries int
	for batch := range batchCh {
		totalEntries += len(batch.entries)
	}

	if totalEntries != 2 {
		t.Errorf("expected 2 entries (skipping header), got %d", totalEntries)
	}
}

func TestIngestReaderGroupsByParentDir(t *testing.T) {
	tmpDir := t.TempDir()
	manifestPath := filepath.Join(tmpDir, "manifest.tsv")

	content := "# header\n" +
		"dir-a/file1.bin\t10\te1\t2025-01-01T00:00:00Z\n" +
		"dir-a/file2.bin\t20\te2\t2025-01-01T00:00:00Z\n" +
		"dir-b/file3.bin\t30\te3\t2025-01-01T00:00:00Z\n"

	if err := os.WriteFile(manifestPath, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	f, err := os.Open(manifestPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	var stats ingestStats
	batchCh := make(chan *dirBatch, 64)
	go ingestReader(f, batchCh, &stats)

	batches := make([]*dirBatch, 0)
	for batch := range batchCh {
		batches = append(batches, batch)
	}

	if len(batches) < 2 {
		t.Fatalf("expected at least 2 batches (one per parent dir), got %d", len(batches))
	}

	if batches[0].parentPath != "dir-a/" {
		t.Errorf("first batch parentPath: expected dir-a/, got %q", batches[0].parentPath)
	}
	if batches[len(batches)-1].parentPath != "dir-b/" {
		t.Errorf("last batch parentPath: expected dir-b/, got %q", batches[len(batches)-1].parentPath)
	}
}
