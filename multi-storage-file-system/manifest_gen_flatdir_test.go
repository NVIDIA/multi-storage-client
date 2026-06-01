package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func init() {
	if globals.logger == nil {
		globals.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime)
	}
}

// mockObject represents a single object in the mock backend.
type mockObject struct {
	key   string
	size  uint64
	eTag  string
	mTime time.Time
}

// mockBackendContext implements backendContextIf for testing manifest generation.
// It holds a sorted list of objects and simulates S3 ListObjectsV2 behavior.
type mockBackendContext struct {
	backend *backendStruct
	objects []mockObject
}

func (m *mockBackendContext) backendCommon() *backendStruct { return m.backend }

func (m *mockBackendContext) deleteFile(_ *deleteFileInputStruct) (*deleteFileOutputStruct, error) {
	return nil, errors.New("not implemented")
}

func (m *mockBackendContext) listDirectory(input *listDirectoryInputStruct) (*listDirectoryOutputStruct, error) {
	prefix := m.backend.prefix + input.dirPath
	delimiter := "/"

	fullStartAfter := ""
	if input.startAfter != "" && input.continuationToken == "" {
		fullStartAfter = m.backend.prefix + input.startAfter
	}

	startIdx := 0
	if input.continuationToken != "" {
		idx := 0
		fmt.Sscanf(input.continuationToken, "%d", &idx)
		startIdx = idx
	}

	maxItems := 1000
	if input.maxItems != 0 {
		maxItems = int(input.maxItems)
	}

	output := &listDirectoryOutputStruct{
		subdirectory: make([]string, 0),
		file:         make([]listDirectoryOutputFileStruct, 0),
	}

	seenPrefixes := make(map[string]bool)
	count := 0
	globalIdx := 0

	for _, obj := range m.objects {
		if !strings.HasPrefix(obj.key, prefix) {
			continue
		}
		if fullStartAfter != "" && obj.key <= fullStartAfter {
			globalIdx++
			continue
		}
		if globalIdx < startIdx {
			globalIdx++
			continue
		}

		relative := strings.TrimPrefix(obj.key, prefix)

		if delimIdx := strings.Index(relative, delimiter); delimIdx >= 0 {
			subdir := relative[:delimIdx]
			if !seenPrefixes[subdir] {
				seenPrefixes[subdir] = true
				output.subdirectory = append(output.subdirectory, subdir)
				count++
			}
		} else {
			output.file = append(output.file, listDirectoryOutputFileStruct{
				basename: relative,
				eTag:     obj.eTag,
				mTime:    obj.mTime,
				size:     obj.size,
			})
			count++
		}

		globalIdx++
		if count >= maxItems {
			output.isTruncated = true
			output.nextContinuationToken = strconv.Itoa(globalIdx)
			break
		}
	}

	return output, nil
}

// listObjects simulates S3 ListObjectsV2 over the sorted mock object list, honoring
// prefix (server-side narrowing), startAfter, continuationToken, and maxItems. The
// stopAt bound is applied by listPrefixWrapper, not here. Object paths are returned
// relative to backend.prefix (so they include the sub-prefix), matching the real
// backends.
func (m *mockBackendContext) listObjects(input *listObjectsInputStruct) (*listObjectsOutputStruct, error) {
	fullPrefix := m.backend.prefix + input.prefix
	fullStartAfter := ""
	if input.startAfter != "" {
		fullStartAfter = m.backend.prefix + input.startAfter
	}

	startIdx := 0
	if input.continuationToken != "" {
		fmt.Sscanf(input.continuationToken, "%d", &startIdx)
	}

	maxItems := 1000
	if input.maxItems != 0 {
		maxItems = int(input.maxItems)
	}

	output := &listObjectsOutputStruct{
		object: make([]listObjectsOutputObjectStruct, 0),
	}

	count := 0
	globalIdx := 0

	for _, obj := range m.objects {
		if !strings.HasPrefix(obj.key, fullPrefix) {
			if obj.key > fullPrefix+"\xff" {
				break
			}
			continue
		}
		if fullStartAfter != "" && obj.key <= fullStartAfter {
			globalIdx++
			continue
		}
		if globalIdx < startIdx {
			globalIdx++
			continue
		}

		output.object = append(output.object, listObjectsOutputObjectStruct{
			path:  strings.TrimPrefix(obj.key, m.backend.prefix),
			eTag:  obj.eTag,
			mTime: obj.mTime,
			size:  obj.size,
		})
		count++
		globalIdx++

		if count >= maxItems {
			output.isTruncated = true
			output.nextContinuationToken = strconv.Itoa(globalIdx)
			break
		}
	}

	return output, nil
}

func (m *mockBackendContext) redactSecrets(s string) string {
	return s
}

func (m *mockBackendContext) readFile(_ *readFileInputStruct) (*readFileOutputStruct, error) {
	return nil, errors.New("not implemented")
}

func (m *mockBackendContext) statDirectory(_ *statDirectoryInputStruct) (*statDirectoryOutputStruct, error) {
	return nil, errors.New("not implemented")
}

func (m *mockBackendContext) statFile(_ *statFileInputStruct) (*statFileOutputStruct, error) {
	return nil, errors.New("not implemented")
}

func newMockBackend(prefix string, objects []mockObject) *backendStruct {
	sort.Slice(objects, func(i, j int) bool { return objects[i].key < objects[j].key })
	backend := &backendStruct{
		dirName:                  "mock",
		bucketContainerName:      "test-bucket",
		prefix:                   prefix,
		readOnly:                 true,
		manifestPath:             "",
		flatDirHints:             nil,
		flatDirConfirmationPages: defaultFlatDirConfirmationPages,
		manifestGenWorkers:       defaultManifestGenWorkers,
	}
	mock := &mockBackendContext{
		backend: backend,
		objects: objects,
	}
	backend.context = mock
	return backend
}

func generateFlatObjects(prefix string, count int, namePattern string) []mockObject {
	objects := make([]mockObject, count)
	now := time.Now()
	for i := range count {
		objects[i] = mockObject{
			key:   prefix + fmt.Sprintf(namePattern, i+1),
			size:  10,
			eTag:  fmt.Sprintf("etag-%d", i+1),
			mTime: now,
		}
	}
	return objects
}

func generateHierarchicalObjects(prefix, dirName string, fileCount int) []mockObject {
	objects := make([]mockObject, fileCount)
	now := time.Now()
	for i := range fileCount {
		objects[i] = mockObject{
			key:   prefix + dirName + "/" + fmt.Sprintf("data-%06d.bin", i+1),
			size:  10,
			eTag:  fmt.Sprintf("etag-%s-%d", dirName, i+1),
			mTime: now,
		}
	}
	return objects
}

func TestFlatDirAccelerationPureFlat(t *testing.T) {
	tmpDir := t.TempDir()
	objects := generateFlatObjects("test/", 10000, "file-%07d.bin")
	backend := newMockBackend("test/", objects)
	backend.manifestPath = filepath.Join(tmpDir, "manifest")

	cfg := &manifestGenConfig{
		workers:     10,
		outputPath:  backend.manifestPath,
		backendName: "mock",
		backend:     backend,
	}

	err := generateManifest(cfg)
	if err != nil {
		t.Fatalf("generateManifest failed: %v", err)
	}

	rootTSV := manifestPartPath(backend.manifestPath, "")
	entries, readErr := readManifestPart(rootTSV)
	if readErr != nil {
		t.Fatalf("readManifestPart failed: %v", readErr)
	}

	if len(entries) != 10000 {
		t.Errorf("expected 10000 entries, got %d", len(entries))
	}

	t.Logf("Pure flat: %d entries in root TSV", len(entries))
}

func TestFlatDirAccelerationHybrid(t *testing.T) {
	tmpDir := t.TempDir()

	var allObjects []mockObject
	allObjects = append(allObjects, generateFlatObjects("test/", 8000, "file-%07d.bin")...)
	allObjects = append(allObjects, generateHierarchicalObjects("test/", "subdir-a", 1000)...)
	allObjects = append(allObjects, generateHierarchicalObjects("test/", "subdir-b", 500)...)
	allObjects = append(allObjects, generateHierarchicalObjects("test/", "subdir-c", 200)...)

	backend := newMockBackend("test/", allObjects)
	backend.manifestPath = filepath.Join(tmpDir, "manifest")
	backend.flatDirConfirmationPages = 3

	cfg := &manifestGenConfig{
		workers:     10,
		outputPath:  backend.manifestPath,
		backendName: "mock",
		backend:     backend,
	}

	err := generateManifest(cfg)
	if err != nil {
		t.Fatalf("generateManifest failed: %v", err)
	}

	rootTSV := manifestPartPath(backend.manifestPath, "")
	rootEntries, readErr := readManifestPart(rootTSV)
	if readErr != nil {
		t.Fatalf("readManifestPart(root) failed: %v", readErr)
	}

	fileCount := 0
	dirCount := 0
	for _, e := range rootEntries {
		if e.Kind == "f" {
			fileCount++
		} else if e.Kind == "d" {
			dirCount++
		}
	}

	t.Logf("Hybrid: root TSV has %d files + %d dirs = %d total entries", fileCount, dirCount, len(rootEntries))

	if fileCount < 8000 {
		t.Errorf("expected at least 8000 flat files in root, got %d", fileCount)
	}
	if dirCount < 3 {
		t.Errorf("expected at least 3 subdirectories (subdir-a, subdir-b, subdir-c), got %d", dirCount)
	}

	for _, dir := range []struct {
		name     string
		expected int
	}{
		{"subdir-a", 1000},
		{"subdir-b", 500},
		{"subdir-c", 200},
	} {
		dirTSV := manifestPartPath(backend.manifestPath, dir.name+"/")
		dirEntries, dirErr := readManifestPart(dirTSV)
		if dirErr != nil {
			t.Errorf("readManifestPart(%s/) failed: %v", dir.name, dirErr)
			continue
		}
		if len(dirEntries) != dir.expected {
			t.Errorf("%s/: expected %d entries, got %d", dir.name, dir.expected, len(dirEntries))
		} else {
			t.Logf("%s/: %d entries OK", dir.name, len(dirEntries))
		}
	}
}

func TestFlatDirAccelerationSmallDir(t *testing.T) {
	tmpDir := t.TempDir()

	objects := generateFlatObjects("test/", 500, "item-%06d.txt")
	backend := newMockBackend("test/", objects)
	backend.manifestPath = filepath.Join(tmpDir, "manifest")

	cfg := &manifestGenConfig{
		workers:     5,
		outputPath:  backend.manifestPath,
		backendName: "mock",
		backend:     backend,
	}

	err := generateManifest(cfg)
	if err != nil {
		t.Fatalf("generateManifest failed: %v", err)
	}

	rootTSV := manifestPartPath(backend.manifestPath, "")
	entries, readErr := readManifestPart(rootTSV)
	if readErr != nil {
		t.Fatalf("readManifestPart failed: %v", readErr)
	}

	if len(entries) != 500 {
		t.Errorf("expected 500 entries (small dir, no flat acceleration), got %d", len(entries))
	}

	t.Logf("Small dir: %d entries (should use normal listing, not flat acceleration)", len(entries))
}

func TestFlatDirAccelerationNoDuplicates(t *testing.T) {
	tmpDir := t.TempDir()
	objects := generateFlatObjects("test/", 10000, "obj-%07d.dat")
	backend := newMockBackend("test/", objects)
	backend.manifestPath = filepath.Join(tmpDir, "manifest")

	cfg := &manifestGenConfig{
		workers:     10,
		outputPath:  backend.manifestPath,
		backendName: "mock",
		backend:     backend,
	}

	err := generateManifest(cfg)
	if err != nil {
		t.Fatalf("generateManifest failed: %v", err)
	}

	rootTSV := manifestPartPath(backend.manifestPath, "")
	entries, readErr := readManifestPart(rootTSV)
	if readErr != nil {
		t.Fatalf("readManifestPart failed: %v", readErr)
	}

	seen := make(map[string]int)
	for _, e := range entries {
		seen[e.Basename]++
	}

	duplicates := 0
	for name, count := range seen {
		if count > 1 {
			duplicates++
			if duplicates <= 5 {
				t.Errorf("duplicate entry: %q appears %d times", name, count)
			}
		}
	}

	if duplicates > 0 {
		t.Errorf("total %d duplicate basenames out of %d entries", duplicates, len(entries))
	} else {
		t.Logf("No duplicates: %d unique entries", len(entries))
	}
}
