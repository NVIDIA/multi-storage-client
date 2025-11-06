package main

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"os"
	"syscall"
	"testing"

	"github.com/NVIDIA/fission/v3"
)

const (
	testFissionFileBLen = uint64(100 * 1024 * 1024)
)

const (
	testFissionReadDirBufSize     = 4 * 1024
	testFissionReadDirPlusBufSize = 4 * 1024
	testFissionReadBufSize        = 4 * 1024
)

var (
	testFissionFileBContent []byte
	testFissionFileBMD5     string
)

func fissionTestUp(t *testing.T) {
	var (
		backend             *backendStruct
		dir1                = newRamDir("dir1")
		dir2                = newRamDir("dir2")
		dir3                = newRamDir("dir3")
		dir4                = newRamDir("dir4")
		err                 error
		fileBMD5AsByteSlice [md5.Size]byte
		ok                  bool
	)

	err = os.Setenv("MSFS_MOUNTPOINT", testGlobals.testMountPoint)
	if err != nil {
		t.Fatalf("os.Setenv(\"MSFS_MOUNTPOINT\", testGlobals.testMountPoint) failed: %v", err)
	}

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".json"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
	{
		"msfs_version": 1,
		"backends": [
			{
				"dir_name": "ram",
				"bucket_container_name": "ignored",
				"backend_type": "RAM"
			}
		]
	}
	`), 0o600)
	if err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}

	err = checkConfigFile()
	if err != nil {
		t.Fatalf("checkConfigFile() unexpectedly failed: %v", err)
	}

	initFS()

	processToMountList()

	// Note: Rapid FUSE mounts/unmounts is often an issue, so we currently disable the following:

	// err = performFissionMount()
	// if err != nil {
	// 	t.Fatalf("unable to perform FUSE mount [Err: %v]", err)
	// }

	// Note: Since we didn't do performFissionMount() above, we need to perform the fission.NewVolme()
	//       embedded in that function so that we may test the fission callbacks directly bypassing FUSE.

	globals.fissionVolume = fission.NewVolume(globals.config.mountName, globals.config.mountPoint, fuseSubtype, maxRead, maxWrite, true, globals.config.allowOther, &globals, globals.logger, globals.errChan)

	backend, ok = globals.config.backends["ram"]
	if !ok {
		t.Fatalf("globals.config.backends[\"ram\"] returned !ok")
	}

	ok = backend.context.(*ramContextStruct).rootDir.dirMap.Put("dir1", dir1)
	if !ok {
		t.Fatalf("backend.context.(*ramContextStruct).rootDir.dirMap.Put(\"dir1\", dir1) returned !ok")
	}
	ok = backend.context.(*ramContextStruct).rootDir.dirMap.Put("dir2", dir2)
	if !ok {
		t.Fatalf("backend.context.(*ramContextStruct).rootDir.dirMap.Put(\"dir2\", dir2) returned !ok")
	}
	ok = backend.context.(*ramContextStruct).rootDir.fileMap.Put("fileA", []byte("/fileA\n"))
	if !ok {
		t.Fatalf("backend.context.(*ramContextStruct).rootDir.fileMap.Put(\"fileA\", []byte(\"/fileA\\n\")) returned !ok")
	}

	testFissionFileBContent = make([]byte, testFissionFileBLen)
	_, err = rand.Read(testFissionFileBContent)
	if err != nil {
		t.Fatalf("rand.Read(testFissionFileBContent) failed: %v", err)
	}
	fileBMD5AsByteSlice = md5.Sum(testFissionFileBContent)
	testFissionFileBMD5 = hex.EncodeToString(fileBMD5AsByteSlice[:])

	ok = backend.context.(*ramContextStruct).rootDir.fileMap.Put("fileB", testFissionFileBContent)
	if !ok {
		t.Fatalf("backend.context.(*ramContextStruct).rootDir.fileMap.Put(\"fileB\", testFissionFileBContent) returned !ok")
	}

	ok = dir1.dirMap.Put("dir3", dir3)
	if !ok {
		t.Fatalf("dir1.dirMap.Put(\"dir3\", dir3) returned !ok")
	}
	ok = dir1.fileMap.Put("fileC", []byte("/dir1/fileC\n"))
	if !ok {
		t.Fatalf("dir1.fileMap.Put(\"fileC\", []byte(\"/dir1/fileC\\n\")) returned !ok")
	}

	ok = dir2.dirMap.Put("dir4", dir4)
	if !ok {
		t.Fatalf("dir2.dirMap.Put(\"dir4\", dir4) returned !ok")
	}

	ok = dir3.fileMap.Put("fileD", []byte("/dir1/dir3/fileD\n"))
	if !ok {
		t.Fatalf("dir3.fileMap.Put(\"fileD\", []byte(\"/dir1/dir3/fileD\\n\")) returned !ok")
	}

	ok = dir4.fileMap.Put("fileE", []byte("/dir2/dir4/fileE\n"))
	if !ok {
		t.Fatalf("dir4.fileMap.Put(\"fileE\", []byte(\"/dir2/dir4/fileE\\n\")) returned !ok")
	}

	backend.context.(*ramContextStruct).curTotalObjects += 5
	backend.context.(*ramContextStruct).curTotalObjectSpace += 7 + testFissionFileBLen + 12 + 17 + 17

	// We will comment the following out for now... but left here for documnetation and possibly later use
	// globals.logger.Printf("[backend.dirName: \"%s\"] ramContext.rootDir populated with:", backend.dirName)
	// globals.logger.Printf("       ├── dir1")
	// globals.logger.Printf("       │   ├── dir3")
	// globals.logger.Printf("       │   │   └── fileD (containing \"/dir1/dir3/fileD\\n\")")
	// globals.logger.Printf("       │   └── fileC (containing \"/dir1/fileC\\n\")")
	// globals.logger.Printf("       ├── dir2")
	// globals.logger.Printf("       │   └── dir4")
	// globals.logger.Printf("       │       └── fileE (containing \"/dir2/dir4/fileE\\n\")")
	// globals.logger.Printf("       ├── fileA (containing \"/fileA\\n\")")
	// globals.logger.Printf("       └── fileB (containing %v random bytes with md5sum %v)", testFissionFileBLen, testFissionFileBMD5)
}

func fissionTestDown(t *testing.T) {
	var (
	// err error
	)

	// Note: Rapid FUSE mounts/unmounts is often an issue, so we currently disable the following

	// err = performFissionUnmount()
	// if err != nil {
	// 	t.Fatalf("unexpected error during FUSE unmount: %v", err)
	// }

	drainFS()
}

// `DISABLEDTestFissionReadDir` exhaustively enumerates the test mountpoint
// assuming a FUSE Mount has been performed. Unfortunately, there are issues
// with repetitive FUSE mount/unmount sequences within the same process that
// would result in occasional and unpredictable hangs or unumount failures.
// As such, this test is explicitly marked disabled here.
func DISABLEDTestFissionReadDir(t *testing.T) {
	var (
		dirEntrySlice []os.DirEntry
		err           error
	)

	fissionTestUp(t)
	defer fissionTestDown(t)

	// Note that "." and ".." are not returned by os.ReadDir()

	dirEntrySlice, err = os.ReadDir(testGlobals.testMountPoint)
	if err != nil {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint) failed: %v", err)
	}
	if len(dirEntrySlice) != 1 {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint) returned bad len(dirEntrySlice): %v (expected: %v)", len(dirEntrySlice), 1)
	}
	if dirEntrySlice[0].Name() != "ram" {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint) returned bad dirEntrySlice[0].Name(): \"%s\" (expected: \"%s\")", dirEntrySlice[0].Name(), "ram")
	}
	if !dirEntrySlice[0].IsDir() {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint) returned bad dirEntrySlice[0].IsDir(): %v (expected: %v)", dirEntrySlice[0].IsDir(), true)
	}

	dirEntrySlice, err = os.ReadDir(testGlobals.testMountPoint + "/ram")
	if err != nil {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram\") failed: %v", err)
	}
	if len(dirEntrySlice) != 4 {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram\") returned bad len(dirEntrySlice): %v (expected: %v)", len(dirEntrySlice), 4)
	}
	if dirEntrySlice[0].Name() != "dir1" {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram\") returned bad dirEntrySlice[0].Name(): \"%s\" (expected: \"%s\")", dirEntrySlice[0].Name(), "dir1")
	}
	if !dirEntrySlice[0].IsDir() {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram\") returned bad dirEntrySlice[0].IsDir(): %v (expected: %v)", dirEntrySlice[0].IsDir(), true)
	}
	if dirEntrySlice[1].Name() != "dir2" {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram\") returned bad dirEntrySlice[1].Name(): \"%s\" (expected: \"%s\")", dirEntrySlice[1].Name(), "dir2")
	}
	if !dirEntrySlice[1].IsDir() {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram\") returned bad dirEntrySlice[1].IsDir(): %v (expected: %v)", dirEntrySlice[1].IsDir(), true)
	}
	if dirEntrySlice[2].Name() != "fileA" {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram\") returned bad dirEntrySlice[1].Name(): \"%s\" (expected: \"%s\")", dirEntrySlice[2].Name(), "fileA")
	}
	if dirEntrySlice[2].IsDir() {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram\") returned bad dirEntrySlice[1].IsDir(): %v (expected: %v)", dirEntrySlice[2].IsDir(), false)
	}
	if dirEntrySlice[3].Name() != "fileB" {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram\") returned bad dirEntrySlice[1].Name(): \"%s\" (expected: \"%s\")", dirEntrySlice[2].Name(), "fileB")
	}
	if dirEntrySlice[3].IsDir() {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram\") returned bad dirEntrySlice[1].IsDir(): %v (expected: %v)", dirEntrySlice[2].IsDir(), false)
	}

	dirEntrySlice, err = os.ReadDir(testGlobals.testMountPoint + "/ram/dir1")
	if err != nil {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram/dir1\") failed: %v", err)
	}
	if len(dirEntrySlice) != 2 {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram/dir1\") returned bad len(dirEntrySlice): %v (expected: %v)", len(dirEntrySlice), 2)
	}
	if dirEntrySlice[0].Name() != "dir3" {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram/dir1\") returned bad dirEntrySlice[0].Name(): \"%s\" (expected: \"%s\")", dirEntrySlice[0].Name(), "dir3")
	}
	if !dirEntrySlice[0].IsDir() {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram/dir1\") returned bad dirEntrySlice[0].IsDir(): %v (expected: %v)", dirEntrySlice[0].IsDir(), true)
	}
	if dirEntrySlice[1].Name() != "fileC" {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram/dir1\") returned bad dirEntrySlice[1].Name(): \"%s\" (expected: \"%s\")", dirEntrySlice[1].Name(), "fileA")
	}
	if dirEntrySlice[1].IsDir() {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram/dir1\") returned bad dirEntrySlice[1].IsDir(): %v (expected: %v)", dirEntrySlice[1].IsDir(), false)
	}

	dirEntrySlice, err = os.ReadDir(testGlobals.testMountPoint + "/ram/dir2")
	if err != nil {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram/dir2\") failed: %v", err)
	}
	if len(dirEntrySlice) != 1 {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram/dir2\") returned bad len(dirEntrySlice): %v (expected: %v)", len(dirEntrySlice), 1)
	}
	if dirEntrySlice[0].Name() != "dir4" {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram/dir2\") returned bad dirEntrySlice[0].Name(): \"%s\" (expected: \"%s\")", dirEntrySlice[0].Name(), "dir4")
	}
	if !dirEntrySlice[0].IsDir() {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram/dir2\") returned bad dirEntrySlice[0].IsDir(): %v (expected: %v)", dirEntrySlice[0].IsDir(), true)
	}

	dirEntrySlice, err = os.ReadDir(testGlobals.testMountPoint + "/ram/dir1/dir3")
	if err != nil {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram/dir1/dir3\") failed: %v", err)
	}
	if len(dirEntrySlice) != 1 {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram/dir1/dir3\") returned bad len(dirEntrySlice): %v (expected: %v)", len(dirEntrySlice), 1)
	}
	if dirEntrySlice[0].Name() != "fileD" {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram/dir1/dir3\") returned bad dirEntrySlice[0].Name(): \"%s\" (expected: \"%s\")", dirEntrySlice[0].Name(), "fileD")
	}
	if dirEntrySlice[0].IsDir() {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram/dir1/dir3\") returned bad dirEntrySlice[0].IsDir(): %v (expected: %v)", dirEntrySlice[0].IsDir(), false)
	}

	dirEntrySlice, err = os.ReadDir(testGlobals.testMountPoint + "/ram/dir2/dir4")
	if err != nil {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram/dir2/dir4\") failed: %v", err)
	}
	if len(dirEntrySlice) != 1 {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram/dir2/dir4\") returned bad len(dirEntrySlice): %v (expected: %v)", len(dirEntrySlice), 1)
	}
	if dirEntrySlice[0].Name() != "fileE" {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram/dir2/dir4\") returned bad dirEntrySlice[0].Name(): \"%s\" (expected: \"%s\")", dirEntrySlice[0].Name(), "file#")
	}
	if dirEntrySlice[0].IsDir() {
		t.Fatalf("os.ReadDir(testGlobals.testMountPoint+\"/ram/dir2/dir4\") returned bad dirEntrySlice[0].IsDir(): %v (expected: %v)", dirEntrySlice[0].IsDir(), false)
	}
}

func TestFissionDoInit(t *testing.T) {
	var (
		errno    syscall.Errno
		inHeader *fission.InHeader
		initIn   *fission.InitIn
		initOut  *fission.InitOut
	)

	fissionTestUp(t)
	defer fissionTestDown(t)

	inHeader = &fission.InHeader{}
	initIn = &fission.InitIn{
		Major: 7,
		Minor: 44,
	}
	initOut, errno = globals.DoInit(inHeader, initIn)
	if errno != 0 {
		t.Fatalf("DoInit() unexpectedly failed (errno: %v)", errno)
	}
	if (initOut.Major != initIn.Major) || (initOut.Minor != initIn.Minor) {
		t.Fatalf("DoInit() returned unexpected initOut")
	}
}

func TestFissionDoStatFS(t *testing.T) {
	var (
		errno    syscall.Errno
		inHeader *fission.InHeader
	)

	fissionTestUp(t)
	defer fissionTestDown(t)

	inHeader = &fission.InHeader{}
	_, errno = globals.DoStatFS(inHeader)
	if errno != 0 {
		t.Fatalf("DoStatFS() unexpectedly failed (errno: %v)", errno)
	}
}

func TestFissionDoLookup(t *testing.T) {
	var (
		errno     syscall.Errno
		fileAIno  uint64
		inHeader  *fission.InHeader
		lookupIn  *fission.LookupIn
		lookupOut *fission.LookupOut
		ramDirIno uint64
	)

	fissionTestUp(t)
	defer fissionTestDown(t)

	inHeader = &fission.InHeader{
		NodeID: FUSERootDirInodeNumber,
	}
	lookupIn = &fission.LookupIn{
		Name: []byte("not_there"),
	}
	_, errno = globals.DoLookup(inHeader, lookupIn)
	if errno == 0 {
		t.Fatalf("DoLookup(FUSERootDirInodeNumber,Name:\"not_there\") unexpectedly succeeded")
	}

	inHeader = &fission.InHeader{
		NodeID: FUSERootDirInodeNumber,
	}
	lookupIn = &fission.LookupIn{
		Name: []byte("ram"),
	}
	lookupOut, errno = globals.DoLookup(inHeader, lookupIn)
	if errno != 0 {
		t.Fatalf("DoLookup(FUSERootDirInodeNumber,Name:\"ram\") unexpectedly failed (errno: %v)", errno)
	}

	ramDirIno = lookupOut.EntryOut.NodeID

	inHeader = &fission.InHeader{
		NodeID: ramDirIno,
	}
	lookupIn = &fission.LookupIn{
		Name: []byte("not_there"),
	}
	_, errno = globals.DoLookup(inHeader, lookupIn)
	if errno == 0 {
		t.Fatalf("DoLookup(ramDir,Name:\"not_there\") unexpectedly succeeded")
	}

	inHeader = &fission.InHeader{
		NodeID: ramDirIno,
	}
	lookupIn = &fission.LookupIn{
		Name: []byte("fileA"),
	}
	lookupOut, errno = globals.DoLookup(inHeader, lookupIn)
	if errno != 0 {
		t.Fatalf("DoLookup(ramDir,Name:\"fileA\") unexpectedly failed (errno: %v)", errno)
	}

	fileAIno = lookupOut.EntryOut.NodeID

	inHeader = &fission.InHeader{
		NodeID: fileAIno,
	}
	lookupIn = &fission.LookupIn{
		Name: []byte("cannot_be_there"),
	}
	_, errno = globals.DoLookup(inHeader, lookupIn)
	if errno == 0 {
		t.Fatalf("DoLookup(fileA,Name:\"cannot_be_there\") unexpectedly succeeded")
	}
}

func TestFissionDoGetAttrStatX(t *testing.T) {
	var (
		dir1Ino           uint64
		errno             syscall.Errno
		fileAIno          uint64
		getAttrIn         *fission.GetAttrIn
		getAttrOut        *fission.GetAttrOut
		inHeader          *fission.InHeader
		lookupIn          *fission.LookupIn
		lookupOut         *fission.LookupOut
		ramDirIno         uint64
		statXIn           *fission.StatXIn
		statXOut          *fission.StatXOut
		unusedInodeNumber uint64
	)

	fissionTestUp(t)
	defer fissionTestDown(t)

	globals.Lock()
	unusedInodeNumber = fetchNonce()
	globals.Unlock()

	inHeader = &fission.InHeader{
		NodeID: unusedInodeNumber,
	}
	getAttrIn = &fission.GetAttrIn{}
	_, errno = globals.DoGetAttr(inHeader, getAttrIn)
	if errno == 0 {
		t.Fatalf("DoGetAttr(unusedInodeNumber) unexpectedly succeeded")
	}

	inHeader = &fission.InHeader{
		NodeID: unusedInodeNumber,
	}
	statXIn = &fission.StatXIn{}
	_, errno = globals.DoStatX(inHeader, statXIn)
	if errno == 0 {
		t.Fatalf("DoStatX(unusedInodeNumber) unexpectedly succeeded")
	}

	inHeader = &fission.InHeader{
		NodeID: FUSERootDirInodeNumber,
	}
	getAttrIn = &fission.GetAttrIn{}
	getAttrOut, errno = globals.DoGetAttr(inHeader, getAttrIn)
	if errno != 0 {
		t.Fatalf("DoGetAttr(FUSERootDirInodeNumber) unexpectedly failed (errno: %v)", errno)
	}
	if getAttrOut.Attr.Ino != FUSERootDirInodeNumber {
		t.Fatalf("DoGetAttr(FUSERootDirInodeNumber) returned unexpected getAttrOut")
	}

	inHeader = &fission.InHeader{
		NodeID: FUSERootDirInodeNumber,
	}
	statXIn = &fission.StatXIn{}
	statXOut, errno = globals.DoStatX(inHeader, statXIn)
	if errno != 0 {
		t.Fatalf("DoStatX(FUSERootDirInodeNumber) unexpectedly failed (errno: %v)", errno)
	}
	if statXOut.StatX.Ino != FUSERootDirInodeNumber {
		t.Fatalf("DoStatX(FUSERootDirInodeNumber) returned unexpected statXOut")
	}

	inHeader = &fission.InHeader{
		NodeID: FUSERootDirInodeNumber,
	}
	lookupIn = &fission.LookupIn{
		Name: []byte("ram"),
	}
	lookupOut, errno = globals.DoLookup(inHeader, lookupIn)
	if errno != 0 {
		t.Fatalf("DoLookup(FUSERootDirInodeNumber,Name:\"ram\") unexpectedly failed (errno: %v)", errno)
	}

	ramDirIno = lookupOut.EntryOut.NodeID

	inHeader = &fission.InHeader{
		NodeID: ramDirIno,
	}
	getAttrIn = &fission.GetAttrIn{}
	getAttrOut, errno = globals.DoGetAttr(inHeader, getAttrIn)
	if errno != 0 {
		t.Fatalf("DoGetAttr(ramDirIno) unexpectedly failed (errno: %v)", errno)
	}
	if getAttrOut.Attr.Ino != ramDirIno {
		t.Fatalf("DoGetAttr(ramDirIno) returned unexpected getAttrOut")
	}

	inHeader = &fission.InHeader{
		NodeID: ramDirIno,
	}
	statXIn = &fission.StatXIn{}
	statXOut, errno = globals.DoStatX(inHeader, statXIn)
	if errno != 0 {
		t.Fatalf("DoStatX(ramDirIno) unexpectedly failed (errno: %v)", errno)
	}
	if statXOut.StatX.Ino != ramDirIno {
		t.Fatalf("DoStatX(ramDirIno) returned unexpected statXOut")
	}

	inHeader = &fission.InHeader{
		NodeID: ramDirIno,
	}
	lookupIn = &fission.LookupIn{
		Name: []byte("dir1"),
	}
	lookupOut, errno = globals.DoLookup(inHeader, lookupIn)
	if errno != 0 {
		t.Fatalf("DoLookup(ramDir,Name:\"dir1\") unexpectedly failed (errno: %v)", errno)
	}

	dir1Ino = lookupOut.EntryOut.NodeID

	inHeader = &fission.InHeader{
		NodeID: dir1Ino,
	}
	getAttrIn = &fission.GetAttrIn{}
	getAttrOut, errno = globals.DoGetAttr(inHeader, getAttrIn)
	if errno != 0 {
		t.Fatalf("DoGetAttr(dir1Ino) unexpectedly failed (errno: %v)", errno)
	}
	if getAttrOut.Attr.Ino != dir1Ino {
		t.Fatalf("DoGetAttr(dir1Ino) returned unexpected getAttrOut")
	}

	inHeader = &fission.InHeader{
		NodeID: dir1Ino,
	}
	statXIn = &fission.StatXIn{}
	statXOut, errno = globals.DoStatX(inHeader, statXIn)
	if errno != 0 {
		t.Fatalf("DoStatX(dir1Ino) unexpectedly failed (errno: %v)", errno)
	}
	if statXOut.StatX.Ino != dir1Ino {
		t.Fatalf("DoStatX(dir1Ino) returned unexpected statXOut")
	}

	inHeader = &fission.InHeader{
		NodeID: ramDirIno,
	}
	lookupIn = &fission.LookupIn{
		Name: []byte("fileA"),
	}
	lookupOut, errno = globals.DoLookup(inHeader, lookupIn)
	if errno != 0 {
		t.Fatalf("DoLookup(ramDir,Name:\"fileA\") unexpectedly failed (errno: %v)", errno)
	}

	fileAIno = lookupOut.EntryOut.NodeID

	inHeader = &fission.InHeader{
		NodeID: fileAIno,
	}
	getAttrIn = &fission.GetAttrIn{}
	getAttrOut, errno = globals.DoGetAttr(inHeader, getAttrIn)
	if errno != 0 {
		t.Fatalf("DoGetAttr(fileAIno) unexpectedly failed (errno: %v)", errno)
	}
	if getAttrOut.Attr.Ino != fileAIno {
		t.Fatalf("DoGetAttr(fileAIno) returned unexpected getAttrOut")
	}

	inHeader = &fission.InHeader{
		NodeID: fileAIno,
	}
	statXIn = &fission.StatXIn{}
	statXOut, errno = globals.DoStatX(inHeader, statXIn)
	if errno != 0 {
		t.Fatalf("DoStatX(fileAIno) unexpectedly failed (errno: %v)", errno)
	}
	if statXOut.StatX.Ino != fileAIno {
		t.Fatalf("DoStatX(fileAIno) returned unexpected statXOut")
	}
}

func TestFissionDoOpenDirReadDirReadDirPlusReleaseDir(t *testing.T) {
	var (
		errno             syscall.Errno
		inHeader          *fission.InHeader
		lookupIn          *fission.LookupIn
		lookupOut         *fission.LookupOut
		openDirIn         *fission.OpenDirIn
		openDirOut        *fission.OpenDirOut
		ramDirFH          uint64
		ramDirIno         uint64
		readDirIn         *fission.ReadDirIn
		readDirOut        *fission.ReadDirOut
		readDirPlusIn     *fission.ReadDirPlusIn
		readDirPlusOut    *fission.ReadDirPlusOut
		releaseDirIn      *fission.ReleaseDirIn
		rootDirFH         uint64
		unusedInodeNumber uint64
	)

	fissionTestUp(t)
	defer fissionTestDown(t)

	globals.Lock()
	unusedInodeNumber = fetchNonce()
	globals.Unlock()

	inHeader = &fission.InHeader{
		NodeID: unusedInodeNumber,
	}
	openDirIn = &fission.OpenDirIn{}
	_, errno = globals.DoOpenDir(inHeader, openDirIn)
	if errno == 0 {
		t.Fatalf("DoOpenDir(unusedInodeNumber) unexpectedly succeeded")
	}

	inHeader = &fission.InHeader{
		NodeID: FUSERootDirInodeNumber,
	}
	openDirIn = &fission.OpenDirIn{}
	openDirOut, errno = globals.DoOpenDir(inHeader, openDirIn)
	if errno != 0 {
		t.Fatalf("DoOpenDir(FUSERootDirInodeNumber) unexpectedly failed (errno: %v)", errno)
	}

	rootDirFH = openDirOut.FH

	inHeader = &fission.InHeader{
		NodeID: FUSERootDirInodeNumber,
	}
	readDirIn = &fission.ReadDirIn{
		FH:     rootDirFH,
		Offset: 0,
		Size:   testFissionReadDirBufSize,
	}
	readDirOut, errno = globals.DoReadDir(inHeader, readDirIn)
	if errno != 0 {
		t.Fatalf("DoReadDir(rootDirFH, Offset: 0) unexpectedly failed (errno: %v)", errno)
	}
	if len(readDirOut.DirEnt) != 3 {
		t.Fatalf("DoReadDir(rootDirFH, Offset: 0) returned bad len(readDirOut.DirEnt): %v (expected: 3)", len(readDirOut.DirEnt))
	}
	if string(readDirOut.DirEnt[0].Name) != "." {
		t.Fatalf("DoReadDir(rootDirFH, Offset: 0) returned wrong DirEnt[0]")
	}
	if string(readDirOut.DirEnt[1].Name) != ".." {
		t.Fatalf("DoReadDir(rootDirFH, Offset: 0) returned wrong DirEnt[1]")
	}
	if string(readDirOut.DirEnt[2].Name) != "ram" {
		t.Fatalf("DoReadDir(rootDirFH, Offset: 0) returned wrong DirEnt[2]")
	}

	inHeader = &fission.InHeader{
		NodeID: FUSERootDirInodeNumber,
	}
	readDirIn = &fission.ReadDirIn{
		FH:     rootDirFH,
		Offset: readDirOut.DirEnt[2].Off,
		Size:   testFissionReadDirBufSize,
	}
	readDirOut, errno = globals.DoReadDir(inHeader, readDirIn)
	if errno != 0 {
		t.Fatalf("DoReadDir(rootDirFH, Offset: %v) unexpectedly failed (errno: %v)", readDirIn.Offset, errno)
	}
	if len(readDirOut.DirEnt) != 0 {
		t.Fatalf("DoReadDir(rootDirFH, Offset: %v) returned bad len(readDirOut.DirEnt): %v (expected: 0)", readDirIn.Offset, len(readDirOut.DirEnt))
	}

	inHeader = &fission.InHeader{
		NodeID: FUSERootDirInodeNumber,
	}
	readDirPlusIn = &fission.ReadDirPlusIn{
		FH:     rootDirFH,
		Offset: 0,
		Size:   testFissionReadDirPlusBufSize,
	}
	readDirPlusOut, errno = globals.DoReadDirPlus(inHeader, readDirPlusIn)
	if errno != 0 {
		t.Fatalf("DoReadDirPlus(rootDirFH, Offset: 0) unexpectedly failed (errno: %v)", errno)
	}
	if len(readDirPlusOut.DirEntPlus) != 3 {
		t.Fatalf("DoReadDirPlus(rootDirFH, Offset: 0) returned bad len(readDirPlusOut.DirEntPlus): %v (expected: 3)", len(readDirPlusOut.DirEntPlus))
	}
	if string(readDirPlusOut.DirEntPlus[0].Name) != "." {
		t.Fatalf("DoReadDirPlus(rootDirFH, Offset: 0) returned wrong DirEntPlus[0]")
	}
	if string(readDirPlusOut.DirEntPlus[1].Name) != ".." {
		t.Fatalf("DoReadDirPlus(rootDirFH, Offset: 0) returned wrong DirEntPlus[1]")
	}
	if string(readDirPlusOut.DirEntPlus[2].Name) != "ram" {
		t.Fatalf("DoReadDirPlus(rootDirFH, Offset: 0) returned wrong DirEntPlus[2]")
	}

	inHeader = &fission.InHeader{
		NodeID: FUSERootDirInodeNumber,
	}
	readDirPlusIn = &fission.ReadDirPlusIn{
		FH:     rootDirFH,
		Offset: readDirPlusOut.DirEntPlus[2].Off,
		Size:   testFissionReadDirPlusBufSize,
	}
	readDirPlusOut, errno = globals.DoReadDirPlus(inHeader, readDirPlusIn)
	if errno != 0 {
		t.Fatalf("DoReadDirPlus(rootDirFH, Offset: %v) unexpectedly failed (errno: %v)", readDirPlusIn.Offset, errno)
	}
	if len(readDirPlusOut.DirEntPlus) != 0 {
		t.Fatalf("DoReadDirPlus(rootDirFH, Offset: %v) returned bad len(readDirPlusOut.DirEntPlus): %v (expected: 0)", readDirPlusIn.Offset, len(readDirPlusOut.DirEntPlus))
	}

	inHeader = &fission.InHeader{
		NodeID: FUSERootDirInodeNumber,
	}
	releaseDirIn = &fission.ReleaseDirIn{
		FH: rootDirFH,
	}
	errno = globals.DoReleaseDir(inHeader, releaseDirIn)
	if errno != 0 {
		t.Fatalf("DoReleaseDir(rootDirFH) unexpectedly failed (errno: %v)", errno)
	}

	inHeader = &fission.InHeader{
		NodeID: FUSERootDirInodeNumber,
	}
	lookupIn = &fission.LookupIn{
		Name: []byte("ram"),
	}
	lookupOut, errno = globals.DoLookup(inHeader, lookupIn)
	if errno != 0 {
		t.Fatalf("DoLookup(FUSERootDirInodeNumber,Name:\"ram\") unexpectedly failed (errno: %v)", errno)
	}

	ramDirIno = lookupOut.EntryOut.NodeID

	inHeader = &fission.InHeader{
		NodeID: ramDirIno,
	}
	openDirIn = &fission.OpenDirIn{}
	openDirOut, errno = globals.DoOpenDir(inHeader, openDirIn)
	if errno != 0 {
		t.Fatalf("DoOpenDir(ramDirIno) unexpectedly failed (errno: %v)", errno)
	}

	ramDirFH = openDirOut.FH

	inHeader = &fission.InHeader{
		NodeID: ramDirIno,
	}
	readDirIn = &fission.ReadDirIn{
		FH:     ramDirFH,
		Offset: 0,
		Size:   testFissionReadDirBufSize,
	}
	readDirOut, errno = globals.DoReadDir(inHeader, readDirIn)
	if errno != 0 {
		t.Fatalf("DoReadDir(ramDirFH, Offset: 0) unexpectedly failed (errno: %v)", errno)
	}
	if len(readDirOut.DirEnt) != 6 {
		t.Fatalf("DoReadDir(ramDirFH, Offset: 0) returned bad len(readDirOut.DirEnt): %v (expected: 6)", len(readDirOut.DirEnt))
	}
	if string(readDirOut.DirEnt[0].Name) != "fileA" {
		t.Fatalf("DoReadDir(ramDirFS, Offset: 0) returned wrong DirEnt[0]")
	}
	if string(readDirOut.DirEnt[1].Name) != "fileB" {
		t.Fatalf("DoReadDir(ramDirFS, Offset: 0) returned wrong DirEnt[1]")
	}
	if string(readDirOut.DirEnt[2].Name) != "dir1" {
		t.Fatalf("DoReadDir(ramDirFS, Offset: 0) returned wrong DirEnt[2]")
	}
	if string(readDirOut.DirEnt[3].Name) != "dir2" {
		t.Fatalf("DoReadDir(ramDirFS, Offset: 0) returned wrong DirEnt[3]")
	}
	if string(readDirOut.DirEnt[4].Name) != "." {
		t.Fatalf("DoReadDir(ramDirFS, Offset: 0) returned wrong DirEnt[4]")
	}
	if string(readDirOut.DirEnt[5].Name) != ".." {
		t.Fatalf("DoReadDir(ramDirFS, Offset: 0) returned wrong DirEnt[5]")
	}

	inHeader = &fission.InHeader{
		NodeID: ramDirIno,
	}
	readDirIn = &fission.ReadDirIn{
		FH:     ramDirFH,
		Offset: readDirOut.DirEnt[5].Off,
		Size:   testFissionReadDirBufSize,
	}
	readDirOut, errno = globals.DoReadDir(inHeader, readDirIn)
	if errno != 0 {
		t.Fatalf("DoReadDir(ramDirFH, Offset: %v) unexpectedly failed (errno: %v)", readDirIn.Offset, errno)
	}
	if len(readDirOut.DirEnt) != 0 {
		t.Fatalf("DoReadDir(ramDirFH, Offset: %v) returned bad len(readDirOut.DirEnt): %v (expected: 0)", readDirIn.Offset, len(readDirOut.DirEnt))
	}

	inHeader = &fission.InHeader{
		NodeID: ramDirIno,
	}
	readDirPlusIn = &fission.ReadDirPlusIn{
		FH:     ramDirFH,
		Offset: 0,
		Size:   testFissionReadDirPlusBufSize,
	}
	readDirPlusOut, errno = globals.DoReadDirPlus(inHeader, readDirPlusIn)
	if errno != 0 {
		t.Fatalf("DoReadDirPlus(ramDirFH, Offset: 0) unexpectedly failed (errno: %v)", errno)
	}
	if len(readDirPlusOut.DirEntPlus) != 6 {
		t.Fatalf("DoReadDirPlus(ramDirFH, Offset: 0) returned bad len(readDirPlusOut.DirEntPlus): %v (expected: 6)", len(readDirPlusOut.DirEntPlus))
	}
	if string(readDirPlusOut.DirEntPlus[0].Name) != "fileA" {
		t.Fatalf("DoReadDirPlus(ramDirFH, Offset: 0) returned wrong DirEntPlus[0]")
	}
	if string(readDirPlusOut.DirEntPlus[1].Name) != "fileB" {
		t.Fatalf("DoReadDirPlus(ramDirFH, Offset: 0) returned wrong DirEntPlus[1]")
	}
	if string(readDirPlusOut.DirEntPlus[2].Name) != "dir1" {
		t.Fatalf("DoReadDirPlus(ramDirFH, Offset: 0) returned wrong DirEntPlus[2]")
	}
	if string(readDirPlusOut.DirEntPlus[3].Name) != "dir2" {
		t.Fatalf("DoReadDirPlus(ramDirFH, Offset: 0) returned wrong DirEntPlus[3]")
	}
	if string(readDirPlusOut.DirEntPlus[4].Name) != "." {
		t.Fatalf("DoReadDirPlus(ramDirFH, Offset: 0) returned wrong DirEntPlus[4]")
	}
	if string(readDirPlusOut.DirEntPlus[5].Name) != ".." {
		t.Fatalf("DoReadDirPlus(ramDirFH, Offset: 0) returned wrong DirEntPlus[5]")
	}

	inHeader = &fission.InHeader{
		NodeID: ramDirIno,
	}
	readDirPlusIn = &fission.ReadDirPlusIn{
		FH:     ramDirFH,
		Offset: readDirPlusOut.DirEntPlus[5].Off,
		Size:   testFissionReadDirPlusBufSize,
	}
	readDirPlusOut, errno = globals.DoReadDirPlus(inHeader, readDirPlusIn)
	if errno != 0 {
		t.Fatalf("DoReadDirPlus(ramDirFH, Offset: %v) unexpectedly failed (errno: %v)", readDirPlusIn.Offset, errno)
	}
	if len(readDirPlusOut.DirEntPlus) != 0 {
		t.Fatalf("DoReadDirPlus(ramDirFH, Offset: %v) returned bad len(readDirPlusOut.DirEntPlus): %v (expected: 0)", readDirPlusIn.Offset, len(readDirPlusOut.DirEntPlus))
	}

	inHeader = &fission.InHeader{
		NodeID: ramDirIno,
	}
	releaseDirIn = &fission.ReleaseDirIn{
		FH: ramDirFH,
	}
	errno = globals.DoReleaseDir(inHeader, releaseDirIn)
	if errno != 0 {
		t.Fatalf("DoReleaseDir(ramDirFH) unexpectedly failed (errno: %v)", errno)
	}
}

func TestFissionDoOpenReadRelease(t *testing.T) {
	var (
		errno       syscall.Errno
		fileBFH     uint64
		fileBIno    uint64
		fileBOffset uint64
		getAttrIn   *fission.GetAttrIn
		getAttrOut  *fission.GetAttrOut
		inHeader    *fission.InHeader
		lookupIn    *fission.LookupIn
		lookupOut   *fission.LookupOut
		openIn      *fission.OpenIn
		openOut     *fission.OpenOut
		ramDirIno   uint64
		readIn      *fission.ReadIn
		readOut     *fission.ReadOut
		releaseIn   *fission.ReleaseIn
	)

	fissionTestUp(t)
	defer fissionTestDown(t)

	inHeader = &fission.InHeader{
		NodeID: FUSERootDirInodeNumber,
	}
	lookupIn = &fission.LookupIn{
		Name: []byte("ram"),
	}
	lookupOut, errno = globals.DoLookup(inHeader, lookupIn)
	if errno != 0 {
		t.Fatalf("DoLookup(FUSERootDirInodeNumber,Name:\"ram\") unexpectedly failed (errno: %v)", errno)
	}

	ramDirIno = lookupOut.EntryOut.NodeID

	inHeader = &fission.InHeader{
		NodeID: ramDirIno,
	}
	lookupIn = &fission.LookupIn{
		Name: []byte("fileB"),
	}
	lookupOut, errno = globals.DoLookup(inHeader, lookupIn)
	if errno != 0 {
		t.Fatalf("DoLookup(ramDirIno,Name:\"fileB\") unexpectedly failed (errno: %v)", errno)
	}

	fileBIno = lookupOut.EntryOut.NodeID

	inHeader = &fission.InHeader{
		NodeID: fileBIno,
	}
	getAttrIn = &fission.GetAttrIn{}
	getAttrOut, errno = globals.DoGetAttr(inHeader, getAttrIn)
	if errno != 0 {
		t.Fatalf("DoGetAttr(fileBIno) unexpectedly failed (errno: %v)", errno)
	}
	if getAttrOut.Attr.Size != testFissionFileBLen {
		t.Fatalf("DoGetAttr(fileBIno) unexpectedly returned .Size: %v (expected: %v)", getAttrOut.Attr.Size, testFissionFileBLen)
	}

	inHeader = &fission.InHeader{
		NodeID: fileBIno,
	}
	openIn = &fission.OpenIn{
		Flags: fission.FOpenRequestRDONLY,
	}
	openOut, errno = globals.DoOpen(inHeader, openIn)
	if errno != 0 {
		t.Fatalf("DoOpen(fileBIno, Flags: fission.FOpenRequestRDONLY) unexpectedly failed (errno: %v)", errno)
	}

	fileBFH = openOut.FH

	fileBOffset = 0

	for fileBOffset < testFissionFileBLen {
		inHeader = &fission.InHeader{
			NodeID: fileBIno,
		}
		if (fileBOffset + testFissionReadBufSize) > testFissionFileBLen {
			readIn = &fission.ReadIn{
				FH:     fileBFH,
				Offset: fileBOffset,
				Size:   uint32(testFissionFileBLen - fileBOffset),
			}
		} else {
			readIn = &fission.ReadIn{
				FH:     fileBFH,
				Offset: fileBOffset,
				Size:   uint32(testFissionReadBufSize),
			}
		}
		readOut, errno = globals.DoRead(inHeader, readIn)
		if errno != 0 {
			t.Fatalf("DoRead(FH: fileBFH, Offset: %v) unexpectedly failed (errno: %v)", readIn.Offset, errno)
		}
		if (fileBOffset + uint64(len(readOut.Data))) > testFissionFileBLen {
			t.Fatalf("DoRead(FH: fileBFH, Offset: %v) unexpectedly returned bytes beyond .Size", readIn.Offset)
		}
		if !bytes.Equal(readOut.Data, testFissionFileBContent[fileBOffset:(fileBOffset+uint64(len(readOut.Data)))]) {
			t.Fatalf("DoRead(FH: fileBFH, Offset: %v) unexpectedly returned mismatched bytes", readIn.Offset)
		}
		fileBOffset += uint64(len(readOut.Data))
	}

	inHeader = &fission.InHeader{
		NodeID: fileBIno,
	}
	releaseIn = &fission.ReleaseIn{
		FH: fileBFH,
	}
	errno = globals.DoRelease(inHeader, releaseIn)
	if errno != 0 {
		t.Fatalf("DoRelease(fileBFH) unexpectedly failed (errno: %v)", errno)
	}
}
