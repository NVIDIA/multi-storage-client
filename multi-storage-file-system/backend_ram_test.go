package main

import (
	"syscall"
	"testing"

	"github.com/NVIDIA/fission/v3"
)

const (
	TestReadDirInSize = uint32(64 * 1024)
)

func TestRAMBackend(t *testing.T) {
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
