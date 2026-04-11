package main

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func BenchmarkBPTreePageInsertion(b *testing.B) {
	const (
		OBJECTS_PER_PAGE = 1000
	)

	var (
		dirEntryBasename    string
		dirEntryInodeNumber uint64
		err                 error
		inode               *inodeStruct
		ok                  bool
		ramInodeNumber      uint64
	)

	err = os.Setenv("MSFS_MOUNTPOINT", testGlobals.testMountPoint)
	if err != nil {
		b.Fatalf("os.Setenv(\"MSFS_MOUNTPOINT\", testGlobals.testMountPoint) failed: %v", err)
	}

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yaml"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: ram,
    bucket_container_name: ignored,
    backend_type: RAM,
  },
]
`), 0o600)
	if err != nil {
		b.Fatalf("os.WriteFile() failed: %v", err)
	}

	err = checkConfigFile()
	if err != nil {
		b.Fatalf("checkConfigFile() unexpectedly failed: %v", err)
	}

	initFS()

	processToMountList()

	b.Cleanup(drainFS)

	b.ResetTimer()

	for range b.N {
		globalsLock("bptree_test.go:59:3:BenchmarkBPTreePageInsertion")

		ramInodeNumber, ok = globals.virtChildDirEntryMap.getByBasename(FUSERootDirInodeNumber, "ram")
		if !ok {
			b.Fatalf("globals.virtChildDirEntryMap.getByBasename(FUSERootDirInodeNumber, \"ram\") returned !ok")
		}

		for range OBJECTS_PER_PAGE {
			dirEntryInodeNumber = fetchNonce()
			dirEntryBasename = fmt.Sprintf("%016X", dirEntryInodeNumber)

			inode = &inodeStruct{
				inodeNumber:       dirEntryInodeNumber,
				parentInodeNumber: ramInodeNumber,
				isVirt:            false,
				objectPath:        dirEntryBasename,
				basename:          dirEntryBasename,
				eTag:              dirEntryBasename,
				mTime:             time.Now(),
				xTime:             time.Now().Add(1000 * time.Second),
				cacheMap:          make(map[uint64]uint64),
				fhSet:             make(map[uint64]struct{}),
			}

			ok = globals.inodeMap.put(inode)
			if !ok {
				b.Fatalf("globals.inodeMap.put(inode) returned !ok")
			}

			ok = globals.inodeEvictionQueue.insert(inode)
			if !ok {
				b.Fatalf("globals.inodeEvictionQueue.insert(inode) returned !ok")
			}

			ok = globals.physChildDirEntryMap.put(ramInodeNumber, dirEntryBasename, dirEntryInodeNumber)
			if !ok {
				b.Fatalf("globals.physChildDirEntryMap.put(ramInodeNumber, dirEntryBasename, dirEntryInodeNumber) returned !ok")
			}
		}

		globalsUnlock()
	}
}
