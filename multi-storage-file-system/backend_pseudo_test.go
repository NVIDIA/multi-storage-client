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
		err               error
		listObjectsInput  *listObjectsInputStruct
		listObjectsOutput *listObjectsOutputStruct
		ok                bool
		pseudoBackend     *backendStruct
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
		maxItems:          0,
	}

	t.Logf("listObjectsInput: %#v", listObjectsInput)

	listObjectsOutput, err = listObjectsWrapper(pseudoBackend.context, listObjectsInput)
	if err != nil {
		t.Fatalf("listObjectsWrapper(pseudoBackend.context, listObjectsInput) failed: %v", err)
	}

	t.Logf("listObjectsOutput: %#v", listObjectsOutput)
}
