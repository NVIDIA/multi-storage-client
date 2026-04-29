package main

import (
	"syscall"
	"testing"

	"github.com/NVIDIA/fission/v4"
)

const (
	TestReadDirInSize = uint32(64 * 1024)
)

func TestRAMBackendViaFission(t *testing.T) {
	var (
		errno             syscall.Errno
		inHeader          *fission.InHeader
		lookupIn          *fission.LookupIn
		lookupOut         *fission.LookupOut
		openDirIn         *fission.OpenDirIn
		openDirOut        *fission.OpenDirOut
		ramDirFH          uint64
		ramDirInodeNumber uint64
		readDirIn         *fission.ReadDirIn
		releaseDirIn      *fission.ReleaseDirIn
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

	ramDirInodeNumber = lookupOut.EntryOut.NodeID

	inHeader.NodeID = ramDirInodeNumber

	openDirIn = &fission.OpenDirIn{}

	openDirOut, errno = globals.DoOpenDir(inHeader, openDirIn)
	if errno != 0 {
		t.Fatalf("DoOpenDir(ramDirInodeNumber) unexpectedly failed (errno: %v)", errno)
	}

	ramDirFH = openDirOut.FH

	readDirIn = &fission.ReadDirIn{
		FH:   ramDirFH,
		Size: TestReadDirInSize,
	}

	_, errno = globals.DoReadDir(inHeader, readDirIn)
	if errno != 0 {
		t.Fatalf("DoReadDir(ramDirFH) unexpectedly failed (errno: %v)", errno)
	}

	releaseDirIn = &fission.ReleaseDirIn{
		FH: ramDirFH,
	}

	errno = globals.DoReleaseDir(inHeader, releaseDirIn)
	if errno != 0 {
		t.Fatalf("DoReleaseDir(ramDirFH) unexpectedly failed (errno: %v)", errno)
	}
}

func TestRAMBackendDirectly(t *testing.T) {
	var (
		deleteFileInput     *deleteFileInputStruct
		err                 error
		listDirectoryInput  *listDirectoryInputStruct
		listDirectoryOutput *listDirectoryOutputStruct
		listObjectsInput    *listObjectsInputStruct
		listObjectsOutput   *listObjectsOutputStruct
		ok                  bool
		ramBackend          *backendStruct
		readFileInput       *readFileInputStruct
		readFileOutput      *readFileOutputStruct
		statDirectoryInput  *statDirectoryInputStruct
		statFileInput       *statFileInputStruct
		statFileOutput      *statFileOutputStruct
	)

	fissionTestUp(t)
	defer fissionTestDown(t)

	ramBackend, ok = globals.config.backends["ram"]
	if !ok {
		t.Fatalf("globals.config.backends[\"ram\"] returned !ok")
	}

	listObjectsInput = &listObjectsInputStruct{
		startAfter:        "",
		continuationToken: "",
		maxItems:          3,
	}

	listObjectsOutput, err = listObjectsWrapper(ramBackend.context, listObjectsInput)
	if err != nil {
		t.Fatalf("listObjectsWrapper(ramBackend.context, listObjectsInput) failed: %v [case 1]", err)
	}
	if (len(listObjectsOutput.object) != 3) || (listObjectsOutput.object[0].path != "dir1/dir3/fileD") || (listObjectsOutput.object[1].path != "dir1/fileC") || (listObjectsOutput.object[2].path != "dir2/dir4/fileE") {
		t.Fatalf("listDirectoryOutput.object unexpected [case 1]")
	}
	if listObjectsOutput.nextContinuationToken != "3" {
		t.Fatalf("listObjectsOutput.nextContinuationToken unexpected [case 1]")
	}
	if !listObjectsOutput.isTruncated {
		t.Fatalf("listObjectsOutput.isTruncated unexpected [case 1]")
	}

	listObjectsInput.continuationToken = listObjectsOutput.nextContinuationToken
	listObjectsInput.maxItems = 0

	listObjectsOutput, err = listObjectsWrapper(ramBackend.context, listObjectsInput)
	if err != nil {
		t.Fatalf("listObjectsWrapper(ramBackend.context, listObjectsInput) failed: %v [case 2]", err)
	}
	if (len(listObjectsOutput.object) != 2) || (listObjectsOutput.object[0].path != "fileA") || (listObjectsOutput.object[1].path != "fileB") {
		t.Fatalf("listDirectoryOutput.object unexpected [case 2]")
	}
	if listObjectsOutput.nextContinuationToken != "" {
		t.Fatalf("listObjectsOutput.nextContinuationToken unexpected [case 2]")
	}
	if listObjectsOutput.isTruncated {
		t.Fatalf("listObjectsOutput.isTruncated unexpected [case 2]")
	}

	listObjectsInput.startAfter = "dir1/fileC"
	listObjectsInput.continuationToken = ""

	listObjectsOutput, err = listObjectsWrapper(ramBackend.context, listObjectsInput)
	if err != nil {
		t.Fatalf("listObjectsWrapper(ramBackend.context, listObjectsInput) failed: %v [case 3]", err)
	}
	if (len(listObjectsOutput.object) != 3) || (listObjectsOutput.object[0].path != "dir2/dir4/fileE") || (listObjectsOutput.object[1].path != "fileA") || (listObjectsOutput.object[2].path != "fileB") {
		t.Fatalf("listDirectoryOutput.object unexpected [case 3]")
	}
	if listObjectsOutput.nextContinuationToken != "" {
		t.Fatalf("listObjectsOutput.nextContinuationToken unexpected [case 3]")
	}
	if listObjectsOutput.isTruncated {
		t.Fatalf("listObjectsOutput.isTruncated unexpected [case 3]")
	}

	listObjectsInput.startAfter = "foo"
	listObjectsInput.continuationToken = "bar"

	_, err = listObjectsWrapper(ramBackend.context, listObjectsInput)
	if err == nil {
		t.Fatalf("listObjectsWrapper(ramBackend.context, listObjectsInput) succeeded unexpectedly [case 4]")
	}

	listDirectoryInput = &listDirectoryInputStruct{
		continuationToken: "",
		maxItems:          3,
		dirPath:           "",
	}

	listDirectoryOutput, err = listDirectoryWrapper(ramBackend.context, listDirectoryInput)
	if err != nil {
		t.Fatalf("listDirectoryWrapper(ramBackend.context, listDirectoryInput) failed: %v [case 1]", err)
	}
	if (len(listDirectoryOutput.subdirectory) != 2) || (listDirectoryOutput.subdirectory[0] != "dir1") || (listDirectoryOutput.subdirectory[1] != "dir2") {
		t.Fatalf("listDirectoryOutput.subdirectory unexpected [case 1]")
	}
	if (len(listDirectoryOutput.file) != 1) || (listDirectoryOutput.file[0].basename != "fileA") {
		t.Fatalf("listDirectoryOutput.file unexpected [case 1]")
	}
	if listDirectoryOutput.nextContinuationToken != "3" {
		t.Fatalf("listDirectoryOutput.nextContinuationToken unexpected [case 1]")
	}
	if !listDirectoryOutput.isTruncated {
		t.Fatalf("listDirectoryOutput.isTruncated unexpected [case 1]")
	}

	listDirectoryInput.continuationToken = listDirectoryOutput.nextContinuationToken
	listDirectoryInput.maxItems = 0

	listDirectoryOutput, err = listDirectoryWrapper(ramBackend.context, listDirectoryInput)
	if err != nil {
		t.Fatalf("listDirectoryWrapper(ramBackend.context, listDirectoryInput) failed: %v [case 2]", err)
	}
	if len(listDirectoryOutput.subdirectory) != 0 {
		t.Fatalf("listDirectoryOutput.subdirectory unexpected [case 2]")
	}
	if (len(listDirectoryOutput.file) != 1) || (listDirectoryOutput.file[0].basename != "fileB") {
		t.Fatalf("listDirectoryOutput.file unexpected [case 2]")
	}
	if listDirectoryOutput.nextContinuationToken != "" {
		t.Fatalf("listDirectoryOutput.nextContinuationToken unexpected [case 2]")
	}
	if listDirectoryOutput.isTruncated {
		t.Fatalf("listDirectoryOutput.isTruncated unexpected [case 2]")
	}

	statDirectoryInput = &statDirectoryInputStruct{
		dirPath: "dir1",
	}

	_, err = statDirectoryWrapper(ramBackend.context, statDirectoryInput)
	if err != nil {
		t.Fatalf("statDirectoryWrapper(ramBackend.context, statDirectoryInput) failed: %v", err)
	}

	statFileInput = &statFileInputStruct{
		filePath: "fileA",
		ifMatch:  "",
	}

	statFileOutput, err = statFileWrapper(ramBackend.context, statFileInput)
	if err != nil {
		t.Fatalf("statFileWrapper(ramBackend.context, statFileInput) failed: %v [case 1]", err)
	}
	if statFileOutput.size != 7 {
		t.Fatalf("statFileOutput.size unexpected [case 1]")
	}

	readFileInput = &readFileInputStruct{
		filePath:        "fileA",
		offsetCacheLine: 0,
		ifMatch:         "",
	}

	readFileOutput, err = readFileWrapper(ramBackend.context, readFileInput)
	if err != nil {
		t.Fatalf("readFileWrapper(ramBackend.context, readFileInput) failed: %v", err)
	}
	if string(readFileOutput.buf) != "/fileA\n" {
		t.Fatalf("readFileOutput.buf unexpected")
	}

	deleteFileInput = &deleteFileInputStruct{
		filePath: "fileA",
		ifMatch:  "",
	}

	_, err = deleteFileWrapper(ramBackend.context, deleteFileInput)
	if err != nil {
		t.Fatalf("deleteFileWrapper(ramBackend.context, deleteFileInput) failed: %v", err)
	}

	statFileInput = &statFileInputStruct{
		filePath: "fileA",
		ifMatch:  "",
	}

	_, err = statFileWrapper(ramBackend.context, statFileInput)
	if err == nil {
		t.Fatalf("statFileWrapper(ramBackend.context, statFileInput) succeeded unexpectedly [case 2]")
	}
}
