package main

import (
	"testing"
)

func TestPSEUDOBackendCheckPathElement(t *testing.T) {
	var (
		comparison        int
		pathElementNumber uint64
	)

	pathElementNumber, comparison = checkPathElement(PSEUDOFileNameFormat, "file_0000000/", uint64(0x180))
	if (pathElementNumber != 0x00000000) || (comparison != -1) {
		t.Fatalf("checkPathElement(PSEUDOFileNameFormat, \"file_0000000/\", uint64(0x180)) returned (%08X,%d)... expected (00000000,-1)", pathElementNumber, comparison)
	}

	pathElementNumber, comparison = checkPathElement(PSEUDOFileNameFormat, "file_00000000", uint64(0x180))
	if (pathElementNumber != 0x00000000) || (comparison != 0) {
		t.Fatalf("checkPathElement(PSEUDOFileNameFormat, \"file_00000000\", uint64(0x180)) returned (%08X,%d)... expected (00000000,0)", pathElementNumber, comparison)
	}

	pathElementNumber, comparison = checkPathElement(PSEUDOFileNameFormat, "file_00000005", uint64(0x180))
	if (pathElementNumber != 0x00000005) || (comparison != 0) {
		t.Fatalf("checkPathElement(PSEUDOFileNameFormat, \"file_00000005\", uint64(0x180)) returned (%08X,%d)... expected (00000005,0)", pathElementNumber, comparison)
	}

	pathElementNumber, comparison = checkPathElement(PSEUDOFileNameFormat, "file_00000005X", uint64(0x180))
	if (pathElementNumber != 0x00000005) || (comparison != 1) {
		t.Fatalf("checkPathElement(PSEUDOFileNameFormat, \"file_00000005X\", uint64(0x180)) returned (%08X,%d)... expected (00000005,1)", pathElementNumber, comparison)
	}

	pathElementNumber, comparison = checkPathElement(PSEUDOFileNameFormat, "file_00000009", uint64(0x180))
	if (pathElementNumber != 0x00000009) || (comparison != 0) {
		t.Fatalf("checkPathElement(PSEUDOFileNameFormat, \"file_00000009\", uint64(0x180)) returned (%08X,%d)... expected (00000009,0)", pathElementNumber, comparison)
	}

	pathElementNumber, comparison = checkPathElement(PSEUDOFileNameFormat, "file_0000000@", uint64(0x180))
	if (pathElementNumber != 0x00000009) || (comparison != 1) {
		t.Fatalf("checkPathElement(PSEUDOFileNameFormat, \"file_0000000@\", uint64(0x180)) returned (%08X,%d)... expected (00000009,1)", pathElementNumber, comparison)
	}

	pathElementNumber, comparison = checkPathElement(PSEUDOFileNameFormat, "file_0000000A", uint64(0x180))
	if (pathElementNumber != 0x0000000A) || (comparison != 0) {
		t.Fatalf("checkPathElement(PSEUDOFileNameFormat, \"file_0000000A\", uint64(0x180)) returned (%08X,%d)... expected (0000000A,0)", pathElementNumber, comparison)
	}

	pathElementNumber, comparison = checkPathElement(PSEUDOFileNameFormat, "file_0000000C", uint64(0x180))
	if (pathElementNumber != 0x0000000C) || (comparison != 0) {
		t.Fatalf("checkPathElement(PSEUDOFileNameFormat, \"file_0000000C\", uint64(0x180)) returned (%08X,%d)... expected (0000000C,0)", pathElementNumber, comparison)
	}

	pathElementNumber, comparison = checkPathElement(PSEUDOFileNameFormat, "file_0000000F", uint64(0x180))
	if (pathElementNumber != 0x0000000F) || (comparison != 0) {
		t.Fatalf("checkPathElement(PSEUDOFileNameFormat, \"file_0000000F\", uint64(0x180)) returned (%08X,%d)... expected (0000000F,0)", pathElementNumber, comparison)
	}

	pathElementNumber, comparison = checkPathElement(PSEUDOFileNameFormat, "file_0000000G", uint64(0x180))
	if (pathElementNumber != 0x0000000F) || (comparison != 1) {
		t.Fatalf("checkPathElement(PSEUDOFileNameFormat, \"file_0000000G\", uint64(0x180)) returned (%08X,%d)... expected (0000000F,1)", pathElementNumber, comparison)
	}

	pathElementNumber, comparison = checkPathElement(PSEUDOFileNameFormat, "file_0000001", uint64(0x180))
	if (pathElementNumber != 0x00000010) || (comparison != -1) {
		t.Fatalf("checkPathElement(PSEUDOFileNameFormat, \"file_0000001\", uint64(0x180)) returned (%08X,%d)... expected (00000010,-1)", pathElementNumber, comparison)
	}
}

func TestPSEUDOBackendDirectly(t *testing.T) {
	var (
		err                 error
		listDirectoryInput  *listDirectoryInputStruct
		listDirectoryOutput *listDirectoryOutputStruct
		listObjectsInput    *listObjectsInputStruct
		listObjectsOutput   *listObjectsOutputStruct
		ok                  bool
		pseudoBackend       *backendStruct
		readFileInput       *readFileInputStruct
		readFileOutput      *readFileOutputStruct
		statDirectoryInput  *statDirectoryInputStruct
		statFileInput       *statFileInputStruct
		statFileOutput      *statFileOutputStruct
	)

	fissionTestUp(t)
	defer fissionTestDown(t)

	pseudoBackend, ok = globals.config.backends["pseudo"]
	if !ok {
		t.Fatalf("globals.config.backends[\"pseudo\"] returned !ok")
	}

	listObjectsInput = &listObjectsInputStruct{
		startAfter:        "",
		continuationToken: "",
		maxItems:          3,
	}

	listObjectsOutput, err = listObjectsWrapper(pseudoBackend.context, listObjectsInput)
	if err != nil {
		t.Fatalf("listObjectsWrapper(pseudoBackend.context, listObjectsInput) failed: %v [case 1]", err)
	}
	if (len(listObjectsOutput.object) != 3) || (listObjectsOutput.object[0].path != "/dir_00000000/file_00000000") || (listObjectsOutput.object[1].path != "/dir_00000000/file_00000001") || (listObjectsOutput.object[2].path != "/dir_00000001/file_00000000") {
		t.Fatalf("listObjectsOutput.object unexpected [case 1]")
	}
	if listObjectsOutput.nextContinuationToken != "0000000000000003" {
		t.Fatalf("listObjectsOutput.nextContinuationToken unexpected [case 1]")
	}
	if !listObjectsOutput.isTruncated {
		t.Fatalf("listObjectsOutput.isTruncated unexpected [case 1]")
	}

	listObjectsInput.continuationToken = listObjectsOutput.nextContinuationToken
	listObjectsInput.maxItems = 0

	listObjectsOutput, err = listObjectsWrapper(pseudoBackend.context, listObjectsInput)
	if err != nil {
		t.Fatalf("listObjectsWrapper(pseudoBackend.context, listObjectsInput) failed: %v [case 2]", err)
	}
	if (len(listObjectsOutput.object) != 2) || (listObjectsOutput.object[0].path != "/dir_00000001/file_00000001") || (listObjectsOutput.object[1].path != "/file_00000000") {
		t.Fatalf("listObjectsOutput.object unexpected [case 2]")
	}
	if listObjectsOutput.nextContinuationToken != "" {
		t.Fatalf("listObjectsOutput.nextContinuationToken unexpected [case 2]")
	}
	if listObjectsOutput.isTruncated {
		t.Fatalf("listObjectsOutput.isTruncated unexpected [case 2]")
	}

	listObjectsInput.startAfter = "dir_00000001/file_00000000"
	listObjectsInput.continuationToken = ""

	listObjectsOutput, err = listObjectsWrapper(pseudoBackend.context, listObjectsInput)
	if err != nil {
		t.Fatalf("listObjectsWrapper(pseudoBackend.context, listObjectsInput) failed: %v [case 3]", err)
	}
	if (len(listObjectsOutput.object) != 2) || (listObjectsOutput.object[0].path != "/dir_00000001/file_00000001") || (listObjectsOutput.object[1].path != "/file_00000000") {
		t.Fatalf("listObjectsOutput.object unexpected [case 3]")
	}
	if listObjectsOutput.nextContinuationToken != "" {
		t.Fatalf("listObjectsOutput.nextContinuationToken unexpected [case 3]")
	}
	if listObjectsOutput.isTruncated {
		t.Fatalf("listObjectsOutput.isTruncated unexpected [case 3]")
	}

	listObjectsInput.startAfter = "foo"
	listObjectsInput.continuationToken = "bar"

	_, err = listObjectsWrapper(pseudoBackend.context, listObjectsInput)
	if err == nil {
		t.Fatalf("listObjectsWrapper(pseudoBackend.context, listObjectsInput) succeeded unexpectedly [case 4]")
	}

	listDirectoryInput = &listDirectoryInputStruct{
		continuationToken: "",
		maxItems:          1,
		dirPath:           "",
	}

	listDirectoryOutput, err = listDirectoryWrapper(pseudoBackend.context, listDirectoryInput)
	if err != nil {
		t.Fatalf("listDirectoryWrapper(pseudoBackend.context, listDirectoryInput) failed: %v [case 1]", err)
	}
	if (len(listDirectoryOutput.subdirectory) != 1) || (listDirectoryOutput.subdirectory[0] != "dir_00000000") {
		t.Fatalf("listDirectoryOutput.subdirectory unexpected [case 1]")
	}
	if len(listDirectoryOutput.file) != 0 {
		t.Fatalf("listDirectoryOutput.file unexpected [case 1]")
	}
	if listDirectoryOutput.nextContinuationToken != "0000000000000001" {
		t.Fatalf("listDirectoryOutput.nextContinuationToken unexpected [case 1]")
	}
	if !listDirectoryOutput.isTruncated {
		t.Fatalf("listDirectoryOutput.isTruncated unexpected [case 1]")
	}

	listDirectoryInput.continuationToken = listDirectoryOutput.nextContinuationToken
	listDirectoryInput.maxItems = 0

	listDirectoryOutput, err = listDirectoryWrapper(pseudoBackend.context, listDirectoryInput)
	if err != nil {
		t.Fatalf("listDirectoryWrapper(pseudoBackend.context, listDirectoryInput) failed: %v [case 2]", err)
	}
	if (len(listDirectoryOutput.subdirectory) != 1) || (listDirectoryOutput.subdirectory[0] != "dir_00000001") {
		t.Fatalf("listDirectoryOutput.subdirectory unexpected [case 2]")
	}
	if (len(listDirectoryOutput.file) != 1) || (listDirectoryOutput.file[0].basename != "file_00000000") {
		t.Fatalf("listDirectoryOutput.file unexpected [case 2]")
	}
	if listDirectoryOutput.nextContinuationToken != "" {
		t.Fatalf("listDirectoryOutput.nextContinuationToken unexpected [case 2]")
	}
	if listDirectoryOutput.isTruncated {
		t.Fatalf("listDirectoryOutput.isTruncated unexpected [case 2]")
	}

	statDirectoryInput = &statDirectoryInputStruct{
		dirPath: "dir_00000000",
	}

	_, err = statDirectoryWrapper(pseudoBackend.context, statDirectoryInput)
	if err != nil {
		t.Fatalf("statDirectoryWrapper(pseudoBackend.context, statDirectoryInput) failed: %v", err)
	}

	statFileInput = &statFileInputStruct{
		filePath: "file_00000000",
		ifMatch:  "",
	}

	statFileOutput, err = statFileWrapper(pseudoBackend.context, statFileInput)
	if err != nil {
		t.Fatalf("statFileWrapper(pseudoBackend.context, statFileInput) failed: %v", err)
	}
	if statFileOutput.size != 15 {
		t.Fatalf("statFileOutput.size unexpected")
	}

	readFileInput = &readFileInputStruct{
		filePath:        "file_00000000",
		offsetCacheLine: 0,
		ifMatch:         "",
	}

	readFileOutput, err = readFileWrapper(pseudoBackend.context, readFileInput)
	if err != nil {
		t.Fatalf("readFileWrapper(pseudoBackend.context, readFileInput) failed: %v", err)
	}
	if string(readFileOutput.buf) != "/file_00000000\n" {
		t.Fatalf("readFileOutput.buf unexpected")
	}
}
