// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/NVIDIA/fission/v4"
)

// manifestIngestTestUp brings up a filesystem whose RAM backend is writable and
// manifest-backed, which is the configuration a writable manifest-backed mount
// uses. Manifest bootstrap itself is driven explicitly by each test, since
// main() is what schedules it in production.
func manifestIngestTestUp(t *testing.T, manifestPath string) *backendStruct {
	t.Helper()

	if err := os.Setenv("MSFS_MOUNTPOINT", testGlobals.testMountPoint); err != nil {
		t.Fatalf("os.Setenv(MSFS_MOUNTPOINT) failed: %v", err)
	}

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".json"]))

	config := fmt.Sprintf(`{
		"msfs_version": 1,
		"backends": [
			{
				"dir_name": "ram",
				"bucket_container_name": "ignored",
				"backend_type": "RAM",
				"readonly": false,
				"flush_on_close": true,
				"manifest_path": %q
			}
		]
	}`, manifestPath)
	if err := os.WriteFile(globals.configFilePath, []byte(config), 0o600); err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}
	if err := checkConfigFile(); err != nil {
		t.Fatalf("checkConfigFile() unexpectedly failed: %v", err)
	}

	initFS()
	processToMountList()

	backend := globals.config.backends["ram"]
	if backend == nil {
		t.Fatal("RAM backend was not mounted")
	}
	if backend.manifestPath != manifestPath {
		t.Fatalf("backend.manifestPath = %q, want %q", backend.manifestPath, manifestPath)
	}
	return backend
}

func manifestIngestPutBaseObjects(t *testing.T, backend *backendStruct, basenames ...string) {
	t.Helper()

	ram, ok := backend.context.(*ramContextStruct)
	if !ok {
		t.Fatalf("backend context is %T, want *ramContextStruct", backend.context)
	}
	for _, basename := range basenames {
		if ok := ram.rootDir.fileMap.Put(basename, []byte("base")); !ok {
			t.Fatalf("RAM fileMap.Put(%q) returned !ok", basename)
		}
	}
}

func manifestIngestGenerateBase(t *testing.T, backend *backendStruct) {
	t.Helper()

	err := generateManifest(&manifestGenConfig{
		workers:     2,
		outputPath:  backend.manifestPath,
		backendName: backend.dirName,
		backend:     backend,
	})
	if err != nil {
		t.Fatalf("generateManifest: %v", err)
	}
}

func manifestIngestAppendDelta(t *testing.T, manifestDir, parentPath, op string, entry *manifestDirEntry) {
	t.Helper()

	if err := appendManifestDeltaRecord(manifestDir, parentPath, op, entry); err != nil {
		t.Fatalf("appendManifestDeltaRecord(%q, %q, %q): %v", parentPath, op, entry.Basename, err)
	}
}

// TestReadAllManifestDeltasGroupsByParentNewestWins pins the single-parse
// grouping ingest relies on: every parent path in the log is represented, the
// last record for a basename wins, and the result matches the per-directory scan
// it replaces.
func TestReadAllManifestDeltasGroupsByParentNewestWins(t *testing.T) {
	manifestDir := filepath.Join(t.TempDir(), "manifest")
	mTime := mustParseTime(t, "2026-04-04T00:00:00Z")

	manifestIngestAppendDelta(t, manifestDir, "", manifestDeltaUpsert,
		&manifestDirEntry{Kind: "f", Basename: "root.bin", Size: 1, ETag: "root-v1", MTime: mTime})
	manifestIngestAppendDelta(t, manifestDir, "sub/", manifestDeltaUpsert,
		&manifestDirEntry{Kind: "f", Basename: "sub.bin", Size: 2, ETag: "sub-v1", MTime: mTime})
	manifestIngestAppendDelta(t, manifestDir, "sub/", manifestDeltaUpsert,
		&manifestDirEntry{Kind: "f", Basename: "sub.bin", Size: 3, ETag: "sub-v2", MTime: mTime})
	manifestIngestAppendDelta(t, manifestDir, "sub/deeper/", manifestDeltaDelete,
		&manifestDirEntry{Kind: "f", Basename: "gone.bin", MTime: mTime})

	byParent, err := readAllManifestDeltas(manifestDir)
	if err != nil {
		t.Fatalf("readAllManifestDeltas: %v", err)
	}

	wantParents := []string{"", "sub/", "sub/deeper/"}
	if len(byParent) != len(wantParents) {
		t.Fatalf("grouped %d parent paths, want %d: %+v", len(byParent), len(wantParents), byParent)
	}
	for _, parentPath := range wantParents {
		if _, found := byParent[parentPath]; !found {
			t.Errorf("parent %q missing from grouped deltas", parentPath)
		}
	}

	if got := byParent["sub/"]["sub.bin"].Entry; got.Size != 3 || got.ETag != "sub-v2" {
		t.Errorf("sub/sub.bin = %+v, want the later record (size 3, etag sub-v2)", got)
	}
	if got := byParent["sub/deeper/"]["gone.bin"].Op; got != manifestDeltaDelete {
		t.Errorf("gone.bin op = %q, want %q", got, manifestDeltaDelete)
	}

	for _, parentPath := range wantParents {
		perParent, perParentErr := readManifestDeltasForParent(manifestDir, parentPath)
		if perParentErr != nil {
			t.Fatalf("readManifestDeltasForParent(%q): %v", parentPath, perParentErr)
		}
		if !reflect.DeepEqual(perParent, byParent[parentPath]) {
			t.Errorf("parent %q: single parse = %+v, per-parent scan = %+v", parentPath, byParent[parentPath], perParent)
		}
	}
}

func TestReadAllManifestDeltasWithoutLog(t *testing.T) {
	byParent, err := readAllManifestDeltas(filepath.Join(t.TempDir(), "manifest"))
	if err != nil {
		t.Fatalf("readAllManifestDeltas on a manifest with no delta log: %v", err)
	}
	if byParent != nil {
		t.Errorf("grouped deltas = %+v, want nil when the log does not exist", byParent)
	}
}

// A path component may hold any byte but "/" and NUL. Written raw, a tab shifts
// every following field and a newline splits the record, so the record is
// dropped and the mutation it carried is lost.
func TestManifestDeltaFieldsSurviveDelimiters(t *testing.T) {
	manifestDir := t.TempDir()

	for _, basename := range []string{
		"plain.bin",
		"tab\there.bin",
		"newline\nhere.bin",
		"carriage\rreturn.bin",
		`back\slash.bin`,
		"all\t\n\r\\.bin",
	} {
		parentPath := "dir\twith\ttabs/"
		if err := appendManifestDeltaRecord(manifestDir, parentPath, manifestDeltaUpsert, &manifestDirEntry{
			Kind:     "f",
			Basename: basename,
			Size:     11,
			ETag:     "etag\twith\ttab",
			MTime:    mustParseTime(t, "2026-01-01T00:00:00Z"),
		}); err != nil {
			t.Fatalf("appendManifestDeltaRecord(%q): %v", basename, err)
		}

		byParent, err := readAllManifestDeltas(manifestDir)
		if err != nil {
			t.Fatalf("readAllManifestDeltas after %q: %v", basename, err)
		}
		record, ok := byParent[parentPath][basename]
		if !ok {
			t.Fatalf("record for basename %q was dropped; parents present: %v", basename, byParent)
		}
		if record.Entry.Basename != basename {
			t.Errorf("basename round-trip = %q, want %q", record.Entry.Basename, basename)
		}
		if record.ParentPath != parentPath {
			t.Errorf("parentPath round-trip = %q, want %q", record.ParentPath, parentPath)
		}
		if record.Entry.ETag != "etag\twith\ttab" {
			t.Errorf("eTag round-trip = %q, want %q", record.Entry.ETag, "etag\twith\ttab")
		}
		if record.Entry.Size != 11 {
			t.Errorf("size round-trip = %d, want 11", record.Entry.Size)
		}
	}
}

// A backend delete that failed must not leave a tombstone behind. DeleteObject is
// idempotent and reports success for a key that is already gone, so an error
// means the delete did not happen and the object may still be present. A record
// written then hides a live object, and the overlay outlives the mount, so the
// divergence is durable in a way the in-memory inode removal is not.
func TestManifestDeltaSkipsTombstoneWhenBackendDeleteFails(t *testing.T) {
	manifestDir := filepath.Join(t.TempDir(), "manifest")
	backend := manifestIngestTestUp(t, manifestDir)
	defer drainFS()

	manifestIngestPutBaseObjects(t, backend, "kept.bin", "vanished.bin")

	lookup := func(parent uint64, name string) uint64 {
		t.Helper()
		out, errno := globals.DoLookup(&fission.InHeader{NodeID: parent}, &fission.LookupIn{Name: []byte(name)})
		if errno != 0 {
			t.Fatalf("DoLookup(%d, %q) failed (errno: %v)", parent, name, errno)
		}
		return out.EntryOut.NodeID
	}

	ramDirIno := lookup(FUSERootDirInodeNumber, "ram")
	lookup(ramDirIno, "kept.bin")
	lookup(ramDirIno, "vanished.bin")

	// Drop the object behind vanished.bin so the backend reports a delete
	// failure, standing in for the permission or network error S3 would return.
	ram, ok := backend.context.(*ramContextStruct)
	if !ok {
		t.Fatalf("backend context is %T, want *ramContextStruct", backend.context)
	}
	if ok := ram.rootDir.fileMap.DeleteByKey("vanished.bin"); !ok {
		t.Fatal("could not remove the backing object for vanished.bin")
	}

	for _, name := range []string{"kept.bin", "vanished.bin"} {
		errno := globals.DoUnlink(&fission.InHeader{NodeID: ramDirIno}, &fission.UnlinkIn{Name: []byte(name)})
		if errno != 0 {
			t.Fatalf("DoUnlink(%q) failed (errno: %v)", name, errno)
		}
	}

	byParent, err := readAllManifestDeltas(manifestDir)
	if err != nil {
		t.Fatalf("readAllManifestDeltas() failed: %v", err)
	}

	var kept, vanished bool
	for _, records := range byParent {
		if record, present := records["kept.bin"]; present && record.Op == manifestDeltaDelete {
			kept = true
		}
		if record, present := records["vanished.bin"]; present && record.Op == manifestDeltaDelete {
			vanished = true
		}
	}
	if !kept {
		t.Errorf("a successful delete did not record a tombstone; parents: %v", byParent)
	}
	if vanished {
		t.Error("a failed backend delete recorded a tombstone, which hides an object that may still exist")
	}
}

// The writer rejects an unknown op, so one reaching the reader means the record
// is corrupt. Admitting it would apply an operation nothing wrote, and consumers
// read any kind but the directory kind as a file.
func TestManifestDeltaRejectsUnsupportedOpAndKind(t *testing.T) {
	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".json"]))

	manifestDir := t.TempDir()
	deltaDir := filepath.Join(manifestDir, manifestDeltaDirName)
	if err := os.MkdirAll(deltaDir, 0o755); err != nil {
		t.Fatalf("os.MkdirAll(%q) failed: %v", deltaDir, err)
	}

	log := "u\tf\tdir/\tgood.bin\t10\tetag\t2026-01-01T00:00:00Z\n" +
		"x\tf\tdir/\tunsupported-op.bin\t10\tetag\t2026-01-01T00:00:00Z\n" +
		"u\tz\tdir/\tunsupported-kind.bin\t10\tetag\t2026-01-01T00:00:00Z\n" +
		"d\t\tdir/\ttombstone-without-kind.bin\t0\t\t2026-01-01T00:00:00Z\n"
	if err := os.WriteFile(filepath.Join(deltaDir, manifestDeltaFileName), []byte(log), 0o600); err != nil {
		t.Fatalf("os.WriteFile(delta log) failed: %v", err)
	}

	byParent, err := readAllManifestDeltas(manifestDir)
	if err != nil {
		t.Fatalf("readAllManifestDeltas() failed: %v", err)
	}

	if _, ok := byParent["dir/"]["good.bin"]; !ok {
		t.Fatalf("the well-formed record was dropped; parents present: %v", byParent)
	}
	for _, basename := range []string{"unsupported-op.bin", "unsupported-kind.bin"} {
		if _, ok := byParent["dir/"][basename]; ok {
			t.Errorf("record %q was admitted despite an unsupported field", basename)
		}
	}
	// A delete drops an entry without consulting its kind, so screening one on
	// that field would discard the tombstone and resurrect the object.
	tombstone, ok := byParent["dir/"]["tombstone-without-kind.bin"]
	if !ok {
		t.Fatalf("a tombstone carrying no kind was dropped; parents present: %v", byParent)
	}
	if tombstone.Op != manifestDeltaDelete {
		t.Errorf("tombstone op = %q, want %q", tombstone.Op, manifestDeltaDelete)
	}
}

// The base manifest is a snapshot, so ingesting it without the deltas reverts
// the namespace to generation time. That must fail rather than mount stale.
func TestManifestIngestFailsOnUnreadableDeltaLog(t *testing.T) {
	manifestDir := filepath.Join(t.TempDir(), "manifest")
	if err := os.MkdirAll(filepath.Join(manifestDir, manifestDeltaDirName), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(manifestPartPath(manifestDir, ""), []byte(
		"f\tbase.bin\t100\tbase-etag\t2026-01-01T00:00:00Z\n",
	), 0o600); err != nil {
		t.Fatalf("WriteFile(base part): %v", err)
	}
	// A directory where the log file belongs makes the read fail without
	// depending on running as a non-root user.
	if err := os.MkdirAll(manifestDeltaPath(manifestDir), 0o755); err != nil {
		t.Fatalf("MkdirAll(delta log path): %v", err)
	}

	if _, err := readAllManifestDeltas(manifestDir); err == nil {
		t.Fatal("readAllManifestDeltas unexpectedly succeeded on an unreadable log")
	}
}

// An unresolvable parent must not fall back to the backend root: the entry would
// be reconstructed directly under the mount point, and a tombstone filed there
// would not hide the real base entry, so a deleted object would reappear.
func TestManifestDeltaDerivesParentWhenParentInodeMissing(t *testing.T) {
	manifestDir := filepath.Join(t.TempDir(), "manifest")
	backend := manifestIngestTestUp(t, manifestDir)
	defer drainFS()
	backend.manifestPath = manifestDir

	globalsLock("manifest_ingest_delta_test.go:346:2:TestManifestDeltaDerivesParentWhenParentInodeMissing")
	orphan := &inodeStruct{
		inodeNumber:       fetchNonce(),
		inodeType:         FileObject,
		parentInodeNumber: fetchNonce(), // never inserted, so the lookup misses
		backendNonce:      backend.nonce,
		objectPath:        "deep/nested/dir/file.bin",
		basename:          "file.bin",
		sizeInMemory:      7,
		eTag:              "orphan-etag",
		mTime:             mustParseTime(t, "2026-01-01T00:00:00Z"),
		fhSet:             make(map[uint64]struct{}),
	}
	appendManifestDeltaForInodeLocked(backend, orphan, manifestDeltaDelete)
	globalsUnlock()

	byParent, err := readAllManifestDeltas(manifestDir)
	if err != nil {
		t.Fatalf("readAllManifestDeltas: %v", err)
	}
	if _, atRoot := byParent[""]["file.bin"]; atRoot {
		t.Fatal("record was misfiled at the backend root")
	}
	record, ok := byParent["deep/nested/dir/"]["file.bin"]
	if !ok {
		t.Fatalf("record not filed under its real parent; parents present: %v", byParent)
	}
	if record.Op != manifestDeltaDelete {
		t.Errorf("op = %q, want %q", record.Op, manifestDeltaDelete)
	}
}

// TestManifestPartParentPathRoundTrip checks that ingest derives the same
// directory that generation partitioned by, since the delta overlay is keyed on
// that path.
func TestManifestPartParentPathRoundTrip(t *testing.T) {
	manifestDir := t.TempDir()

	for _, parentPath := range []string{"", "a/", "a/b/", "dir-r0/r0/"} {
		partPath := manifestPartPath(manifestDir, parentPath)
		relPath, err := filepath.Rel(manifestDir, partPath)
		if err != nil {
			t.Fatalf("filepath.Rel(%q, %q): %v", manifestDir, partPath, err)
		}
		if got := manifestPartParentPath(relPath); got != parentPath {
			t.Errorf("manifestPartParentPath(%q) = %q, want %q", relPath, got, parentPath)
		}
	}
}

// TestManifestIngestCoversDeltaOnlyDirectory is the regression guard for ingest
// enumerating base TSVs only. A directory first written after the manifest was
// generated has no base part, so it is reachable only through the delta log's
// parent paths.
func TestManifestIngestCoversDeltaOnlyDirectory(t *testing.T) {
	manifestDir := filepath.Join(t.TempDir(), "manifest")
	backend := manifestIngestTestUp(t, manifestDir)
	defer drainFS()

	manifestIngestPutBaseObjects(t, backend, "keep.bin", "drop.bin")
	manifestIngestGenerateBase(t, backend)

	writeMTime := mustParseTime(t, "2026-05-05T00:00:00Z")
	manifestIngestAppendDelta(t, manifestDir, "fresh/", manifestDeltaUpsert,
		&manifestDirEntry{Kind: "f", Basename: "new.bin", Size: 40, ETag: "new-etag", MTime: writeMTime})
	manifestIngestAppendDelta(t, manifestDir, "", manifestDeltaUpsert,
		&manifestDirEntry{Kind: "f", Basename: "keep.bin", Size: 99, ETag: "keep-v2", MTime: writeMTime})
	manifestIngestAppendDelta(t, manifestDir, "", manifestDeltaDelete,
		&manifestDirEntry{Kind: "f", Basename: "drop.bin", MTime: writeMTime})

	if err := ingestManifest(manifestDir, backend); err != nil {
		t.Fatalf("ingestManifest: %v", err)
	}

	rootInodeNumber := backend.inode.inodeNumber

	keep, ok := globals.physChildDirEntryMap.getByBasename(rootInodeNumber, "keep.bin")
	if !ok {
		t.Fatal("keep.bin was not ingested")
	}
	if keep.Size != 99 {
		t.Errorf("keep.bin size = %d, want 99 from the superseding delta record", keep.Size)
	}

	if _, ok = globals.physChildDirEntryMap.getByBasename(rootInodeNumber, "drop.bin"); ok {
		t.Error("drop.bin was ingested despite its tombstone")
	}

	fresh, ok := globals.physChildDirEntryMap.getByBasename(rootInodeNumber, "fresh")
	if !ok {
		t.Fatal("the delta-only directory \"fresh\" was not ingested")
	}
	if fresh.InodeType != PseudoDir {
		t.Fatalf("fresh InodeType = %d, want %d", fresh.InodeType, PseudoDir)
	}

	freshInode, ok := globals.inodeMap.get(fresh.InodeNumber)
	if !ok {
		t.Fatal("globals.inodeMap.get(fresh) returned !ok")
	}
	newEntry, ok := globals.physChildDirEntryMap.getByBasename(freshInode.inodeNumber, "new.bin")
	if !ok {
		t.Fatal("fresh/new.bin was not ingested from the delta log")
	}
	if newEntry.Size != 40 {
		t.Errorf("fresh/new.bin size = %d, want 40", newEntry.Size)
	}
	if newEntry.MTimeUnixNano != writeMTime.UnixNano() {
		t.Errorf("fresh/new.bin mtime = %d, want %d", newEntry.MTimeUnixNano, writeMTime.UnixNano())
	}
}

// TestManifestIngestRemountReconstructsFUSEWrites drives a create and an unlink
// through the FUSE callbacks, then bootstraps a second session from the base
// manifest plus the delta log alone. The second session's backend is empty, so
// anything that resolves came from the manifest planes rather than a live LIST.
func TestManifestIngestRemountReconstructsFUSEWrites(t *testing.T) {
	manifestDir := filepath.Join(t.TempDir(), "manifest")
	writeData := []byte("written before the remount")

	func() {
		backend := manifestIngestTestUp(t, manifestDir)
		defer drainFS()

		manifestIngestPutBaseObjects(t, backend, "kept.bin", "removed.bin")
		manifestIngestGenerateBase(t, backend)

		lookupOut, errno := globals.DoLookup(
			&fission.InHeader{NodeID: FUSERootDirInodeNumber},
			&fission.LookupIn{Name: []byte("ram")},
		)
		if errno != 0 {
			t.Fatalf("DoLookup(ram) failed (errno: %v)", errno)
		}
		ramDirIno := lookupOut.EntryOut.NodeID

		createOut, errno := globals.DoCreate(
			&fission.InHeader{NodeID: ramDirIno},
			&fission.CreateIn{
				Flags: fission.FOpenRequestWRONLY,
				Mode:  0o666,
				Name:  []byte("created.bin"),
			},
		)
		if errno != 0 {
			t.Fatalf("DoCreate(created.bin) failed (errno: %v)", errno)
		}

		inHeader := &fission.InHeader{NodeID: createOut.NodeID}
		if _, errno = globals.DoWrite(inHeader, &fission.WriteIn{
			FH:   createOut.FH,
			Size: uint32(len(writeData)),
			Data: writeData,
		}); errno != 0 {
			t.Fatalf("DoWrite(created.bin) failed (errno: %v)", errno)
		}
		if errno = globals.DoRelease(inHeader, &fission.ReleaseIn{FH: createOut.FH}); errno != 0 {
			t.Fatalf("DoRelease(created.bin) failed (errno: %v)", errno)
		}

		if errno = globals.DoUnlink(
			&fission.InHeader{NodeID: ramDirIno},
			&fission.UnlinkIn{Name: []byte("removed.bin")},
		); errno != 0 {
			t.Fatalf("DoUnlink(removed.bin) failed (errno: %v)", errno)
		}
	}()

	deltas, err := readAllManifestDeltas(manifestDir)
	if err != nil {
		t.Fatalf("readAllManifestDeltas: %v", err)
	}
	if got, found := deltas[""]["created.bin"]; !found || got.Op != manifestDeltaUpsert {
		t.Fatalf("created.bin delta record = %+v (found=%v), want an upsert", got, found)
	}
	if got, found := deltas[""]["removed.bin"]; !found || got.Op != manifestDeltaDelete {
		t.Fatalf("removed.bin delta record = %+v (found=%v), want a tombstone", got, found)
	}

	backend := manifestIngestTestUp(t, manifestDir)
	defer drainFS()

	if err := ingestManifest(manifestDir, backend); err != nil {
		t.Fatalf("ingestManifest: %v", err)
	}

	rootInodeNumber := backend.inode.inodeNumber

	created, ok := globals.physChildDirEntryMap.getByBasename(rootInodeNumber, "created.bin")
	if !ok {
		t.Fatal("created.bin was not reconstructed after the remount")
	}
	if created.Size != uint64(len(writeData)) {
		t.Errorf("created.bin size = %d, want %d", created.Size, len(writeData))
	}
	if created.MTimeUnixNano == 0 {
		t.Error("created.bin mtime was not reconstructed")
	}

	if _, ok = globals.physChildDirEntryMap.getByBasename(rootInodeNumber, "removed.bin"); ok {
		t.Error("removed.bin survived its tombstone across the remount")
	}
	if _, ok = globals.physChildDirEntryMap.getByBasename(rootInodeNumber, "kept.bin"); !ok {
		t.Error("kept.bin from the base manifest is missing after the remount")
	}
}
